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

    # Replay attempt: must return existing sent result without resending (idempotence)
    replay_result = whatsapp.commit_send_message(send_id, code)
    assert replay_result["success"] is True
    assert replay_result["status"] == "sent"
    assert replay_result["already_sent"] is True
    assert replay_result["whatsapp_message_id"] == "wa_msg_999"
    # No second HTTP call was made
    assert len(http_calls) == 1


def test_commit_send_message_by_code_only(test_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    http_calls = []

    def mock_post(url, json=None, headers=None, timeout=None):
        http_calls.append(json)
        return DummyResponse(status_code=200, payload={"success": True, "message": "wa_msg_code_only"})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    draft = whatsapp.prepare_send_message("554899999999@s.whatsapp.net", "Mensagem via codigo apenas")
    code = draft["authorization_code"]

    # Commit using ONLY authorization_code (send_id omitted)
    result = whatsapp.commit_send_message(authorization_code=code)
    assert result["success"] is True
    assert result["status"] == "sent"
    assert result["whatsapp_message_id"] == "wa_msg_code_only"
    assert len(http_calls) == 1
    assert http_calls[0]["message"] == "Mensagem via codigo apenas"


def test_get_send_status(test_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")

    draft = whatsapp.prepare_send_message("554899999999@s.whatsapp.net", "Texto de status")
    send_id = draft["send_id"]
    code = draft["authorization_code"]

    # Check status as pending via code
    status_by_code = whatsapp.get_send_status(authorization_code=code)
    assert status_by_code["success"] is True
    assert status_by_code["status"] == "pending"
    assert status_by_code["send_id"] == send_id

    # Check status as pending via send_id
    status_by_id = whatsapp.get_send_status(send_id=send_id)
    assert status_by_id["success"] is True
    assert status_by_id["status"] == "pending"
    assert status_by_id["text"] == "Texto de status"


def test_commit_and_status_reconciliation_on_timeout(test_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")

    draft = whatsapp.prepare_send_message("554899999999@s.whatsapp.net", "Texto com timeout")
    code = draft["authorization_code"]

    # Simulate network timeout during bridge POST
    def mock_post_timeout(url, json=None, headers=None, timeout=None):
        # But simulate that bridge actually persisted the message before dying
        conn = sqlite3.connect(test_db)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                chat_jid TEXT,
                sender TEXT,
                content TEXT,
                timestamp TIMESTAMP,
                is_from_me BOOLEAN
            );
        """)
        conn.execute(
            "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, 1)",
            (
                "wa_timeout_recovered",
                "554899999999@s.whatsapp.net",
                "Me",
                "Texto com timeout",
                datetime.now(UTC).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
        raise TimeoutError("Simulated bridge timeout")

    monkeypatch.setattr(whatsapp.requests, "post", mock_post_timeout)

    # Commit should catch timeout, reconcile with messages table, and succeed
    result = whatsapp.commit_send_message(authorization_code=code)
    assert result["success"] is True
    assert result["status"] == "sent"
    assert result["whatsapp_message_id"] == "wa_timeout_recovered"
    assert result.get("reconciled") is True


def test_prepare_and_commit_persistence_across_restart(test_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")

    draft = whatsapp.prepare_send_message("554899999999@s.whatsapp.net", "Texto persistido")
    code = draft["authorization_code"]

    def mock_post(url, json=None, headers=None, timeout=None):
        return DummyResponse(status_code=200, payload={"success": True, "message": "wa_after_restart"})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    # Simulate restart by pointing to same test_db and committing with code only
    result = whatsapp.commit_send_message(authorization_code=code)
    assert result["success"] is True
    assert result["status"] == "sent"
    assert result["whatsapp_message_id"] == "wa_after_restart"


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
    assert "get_send_status" in tool_names
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
