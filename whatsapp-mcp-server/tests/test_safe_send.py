import hashlib
import re
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

import main
import whatsapp


class DummyResponse:
    def __init__(self, status_code=200, payload=None, text="OK"):
        self.status_code = status_code
        self._payload = payload or {"success": True, "message": "msg_12345"}
        self.text = text

    def json(self):
        return self._payload


@pytest.fixture
def test_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "messages.db")
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE chats (
            jid TEXT PRIMARY KEY,
            name TEXT,
            last_message_time TIMESTAMP,
            last_read_time TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()
    return db_path


def test_prepare_send_message(test_db):
    chat_jid = "554899999999@s.whatsapp.net"
    text = "Olá! Esta é uma mensagem de teste."

    draft = whatsapp.prepare_send_message(chat_jid, text)

    assert "send_id" in draft
    assert draft["chat_jid"] == chat_jid
    assert draft["text"] == text
    assert draft["text_sha256"] == hashlib.sha256(text.encode("utf-8")).hexdigest()
    assert re.match(r"^ENVIAR [A-Z0-9]{4}$", draft["authorization_code"])
    assert "expires_at" in draft

    # Verify record in database
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT status, text, text_sha256 FROM send_message_audit WHERE send_id = ?", (draft["send_id"],))
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "pending"
    assert row[1] == text
    assert row[2] == draft["text_sha256"]


def test_commit_send_message_disabled_by_default(test_db, monkeypatch):
    # Ensure write is disabled
    monkeypatch.delenv("WHATSAPP_WRITE_ENABLED", raising=False)

    draft = whatsapp.prepare_send_message("554899999999@s.whatsapp.net", "Mensagem")
    result = whatsapp.commit_send_message(draft["send_id"], draft["authorization_code"])

    assert result["success"] is False
    assert result["status"] == "forbidden"
    assert "disabled" in result["error"].lower()


def test_commit_send_message_success_and_replay_prevention(test_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    http_calls = []

    def mock_post(url, json=None, headers=None, timeout=None):
        http_calls.append({"url": url, "json": json, "headers": headers})
        return DummyResponse(status_code=200, payload={"success": True, "message": "wa_msg_999"})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    draft = whatsapp.prepare_send_message("554899999999@s.whatsapp.net", "Texto autorizado")
    send_id = draft["send_id"]
    code = draft["authorization_code"]

    # First commit: should succeed
    result = whatsapp.commit_send_message(send_id, code)
    assert result["success"] is True
    assert result["status"] == "sent"
    assert result["whatsapp_message_id"] == "wa_msg_999"
    assert len(http_calls) == 1
    assert http_calls[0]["json"]["message"] == "Texto autorizado"

    # Verify status in database
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT status, whatsapp_message_id FROM send_message_audit WHERE send_id = ?", (send_id,))
    row = cursor.fetchone()
    conn.close()
    assert row[0] == "sent"
    assert row[1] == "wa_msg_999"

    # Replay attempt: must be rejected
    replay_result = whatsapp.commit_send_message(send_id, code)
    assert replay_result["success"] is False
    assert replay_result["status"] == "failed"
    assert "already" in replay_result["error"].lower()
    # No second HTTP call was made
    assert len(http_calls) == 1


def test_commit_send_message_invalid_code(test_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    draft = whatsapp.prepare_send_message("554899999999@s.whatsapp.net", "Texto seguro")

    result = whatsapp.commit_send_message(draft["send_id"], "ENVIAR WRONG")
    assert result["success"] is False
    assert result["status"] == "unauthorized"


def test_commit_send_message_expired(test_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    draft = whatsapp.prepare_send_message("554899999999@s.whatsapp.net", "Texto expirado")

    # Manually expire the draft in database
    expired_time = (datetime.now(UTC) - timedelta(minutes=15)).isoformat()
    conn = sqlite3.connect(test_db)
    conn.execute("UPDATE send_message_audit SET expires_at = ? WHERE send_id = ?", (expired_time, draft["send_id"]))
    conn.commit()
    conn.close()

    result = whatsapp.commit_send_message(draft["send_id"], draft["authorization_code"])
    assert result["success"] is False
    assert result["status"] == "expired"


def test_commit_send_message_tampered_text(test_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    draft = whatsapp.prepare_send_message("554899999999@s.whatsapp.net", "Texto original")

    # Tamper with text in database while preserving original sha256 hash column
    conn = sqlite3.connect(test_db)
    conn.execute("UPDATE send_message_audit SET text = 'Texto adulterado' WHERE send_id = ?", (draft["send_id"],))
    conn.commit()
    conn.close()

    result = whatsapp.commit_send_message(draft["send_id"], draft["authorization_code"])
    assert result["success"] is False
    assert result["status"] == "failed"
    assert "mismatch" in result["error"].lower() or "integrity" in result["error"].lower()


@pytest.mark.asyncio
async def test_mcp_registered_tools_introspection():
    """Verify that FastMCP registers safe tools and NO legacy single-step write tools."""
    tools = await main.mcp.list_tools()
    tool_names = {t.name for t in tools}

    # Verify required safe tools are present
    assert "prepare_send_message" in tool_names
    assert "commit_send_message" in tool_names
    assert "list_chat_lists" in tool_names
    assert "list_chats_by_list" in tool_names
    assert "get_chat_lists" in tool_names
    assert "create_chat_list" in tool_names
    assert "add_chat_to_list" in tool_names
    assert "remove_chat_from_list" in tool_names

    # Verify dangerous single-step write tools are strictly NOT present
    assert "send_message" not in tool_names
    assert "send_file" not in tool_names
    assert "send_audio_message" not in tool_names
    assert "send_reaction" not in tool_names
    assert "mark_messages_read" not in tool_names
