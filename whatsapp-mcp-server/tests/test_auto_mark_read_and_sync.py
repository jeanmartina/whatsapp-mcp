import concurrent.futures
import json
import os
import sqlite3
import tempfile
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import whatsapp


class DummyResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text or json.dumps(self._payload)

    def json(self):
        return self._payload


@pytest.fixture
def temp_db(monkeypatch):
    """Create an isolated messages.db with necessary schema for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "messages.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE chats (
                jid TEXT PRIMARY KEY,
                name TEXT,
                last_message_time TEXT,
                last_read_time TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE messages (
                id TEXT PRIMARY KEY,
                chat_jid TEXT,
                sender TEXT,
                content TEXT,
                timestamp TEXT,
                is_from_me INTEGER DEFAULT 0,
                media_type TEXT,
                quoted_message_id TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE send_message_audit (
                send_id TEXT PRIMARY KEY,
                chat_jid TEXT,
                recipient_name TEXT,
                text TEXT,
                text_sha256 TEXT,
                reply_to_message_id TEXT,
                authorization_code TEXT,
                authorization_code_hash TEXT,
                status TEXT,
                prepared_at TEXT,
                authorized_at TEXT,
                expires_at TEXT,
                sent_at TEXT,
                whatsapp_message_id TEXT,
                error_message TEXT
            )
        """)
        conn.commit()
        conn.close()

        monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", db_path)
        whatsapp._ensure_pending_read_marks_schema(sqlite3.connect(db_path))

        yield db_path


def seed_chat_and_inbound_message(db_path: str, chat_jid: str, msg_id: str, ts: str, name: str = "Test User"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO chats (jid, name, last_message_time, last_read_time) VALUES (?, ?, ?, NULL)",
        (chat_jid, name, ts),
    )
    cursor.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, 0)",
        (msg_id, chat_jid, name, "Hello inbound", ts),
    )
    conn.commit()
    conn.close()


def test_scenario_1_send_followed_by_mark_read(temp_db, monkeypatch):
    """1. Envio seguido de marcacao de mensagens anteriores como lidas."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899111111@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_01", "2026-11-07 10:00:00")

    draft = whatsapp.prepare_send_message(chat_jid=chat_jid, text="Minha resposta")
    auth_code = draft["authorization_code"]

    calls = []

    def mock_post(url, json=None, headers=None, timeout=None):
        calls.append((url, json))
        if "/send" in url:
            return DummyResponse(200, {"success": True, "message_id": "out_msg_01"})
        elif "/chats/mark-read" in url:
            return DummyResponse(200, {
                "success": True,
                "chat_jid": json.get("chat_jid"),
                "marked_read_through_message_id": json.get("message_id"),
                "unread": False,
            })
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    result = whatsapp.commit_send_message(authorization_code=auth_code)

    assert result["success"] is True
    assert result["status"] == "sent"
    assert result["message_sent"] is True
    assert result["read_marked"] is True
    assert result["read_status"] == "marked"

    # Verify order of calls: send first, then mark-read
    assert len(calls) == 2
    assert "/send" in calls[0][0]
    assert "/chats/mark-read" in calls[1][0]
    assert calls[1][1]["chat_jid"] == chat_jid
    assert calls[1][1]["message_id"] == "msg_in_01"


def test_scenario_2_new_message_arriving_after_send_remains_unread(temp_db, monkeypatch):
    """2. Nova mensagem chegando imediatamente apos o envio e permanecendo nao lida."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899222222@s.whatsapp.net"
    t1 = "2026-11-07 10:00:00"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_pre", t1)

    draft = whatsapp.prepare_send_message(chat_jid=chat_jid, text="Resposta 2")
    auth_code = draft["authorization_code"]

    mark_read_requested_id = None

    def mock_post(url, json=None, headers=None, timeout=None):
        nonlocal mark_read_requested_id
        if "/send" in url:
            return DummyResponse(200, {"success": True, "message_id": "out_msg_02"})
        elif "/chats/mark-read" in url:
            mark_read_requested_id = json.get("message_id")
            # Bridge updates last_read_time to t1
            conn = sqlite3.connect(temp_db)
            c = conn.cursor()
            c.execute("UPDATE chats SET last_read_time = ? WHERE jid = ?", (t1, chat_jid))
            conn.commit()
            conn.close()
            return DummyResponse(200, {
                "success": True,
                "chat_jid": chat_jid,
                "marked_read_through_message_id": mark_read_requested_id,
                "unread": False,
            })
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    result = whatsapp.commit_send_message(authorization_code=auth_code)

    assert result["success"] is True
    assert result["message_sent"] is True
    assert result["read_marked"] is True
    # The mark read targeted msg_in_pre!
    assert mark_read_requested_id == "msg_in_pre"

    # Immediately after send, a brand new inbound message arrives
    conn = sqlite3.connect(temp_db)
    c = conn.cursor()
    t_after = (datetime.now().astimezone() + timedelta(seconds=2)).isoformat(sep=" ")
    c.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, 0)",
        ("msg_in_post", chat_jid, "User", "Outra pergunta rapida!", t_after),
    )
    c.execute("UPDATE chats SET last_message_time = ? WHERE jid = ?", (t_after, chat_jid))
    conn.commit()
    conn.close()

    # Check that chat still has unread status and msg_in_post is unread!
    chat = whatsapp.get_chat(chat_jid, include_last_message=True)
    assert chat["unread"] is True
    assert chat["last_message"] == "Outra pergunta rapida!"


def test_scenario_3_simultaneous_sends_in_six_conversas(temp_db, monkeypatch):
    """3. Envios simultaneos em seis conversas."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jids = [f"55489933333{i}@s.whatsapp.net" for i in range(6)]
    auth_codes = []

    for i, jid in enumerate(chat_jids):
        seed_chat_and_inbound_message(temp_db, jid, f"msg_in_{i}", f"2026-11-07 10:0{i}:00")
        draft = whatsapp.prepare_send_message(chat_jid=jid, text=f"Resposta paralela {i}")
        auth_codes.append(draft["authorization_code"])

    def mock_post(url, json=None, headers=None, timeout=None):
        if "/send" in url:
            return DummyResponse(200, {"success": True, "message_id": f"out_{json.get('recipient')}"})
        elif "/chats/mark-read" in url:
            return DummyResponse(200, {
                "success": True,
                "chat_jid": json.get("chat_jid"),
                "marked_read_through_message_id": json.get("message_id"),
                "unread": False,
            })
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(whatsapp.commit_send_message, code) for code in auth_codes]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == 6
    for r in results:
        assert r["success"] is True
        assert r["status"] == "sent"
        assert r["message_sent"] is True
        assert r["read_marked"] is True


def test_scenario_4_concurrent_operations_on_same_chat(temp_db, monkeypatch):
    """4. Operacoes concorrentes sobre o mesmo chat."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899444444@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_c4", "2026-11-07 10:10:00")

    draft = whatsapp.prepare_send_message(chat_jid=chat_jid, text="Mensagem unica")
    auth_code = draft["authorization_code"]

    def mock_post(url, json=None, headers=None, timeout=None):
        if "/send" in url:
            return DummyResponse(200, {"success": True, "message_id": "out_c4"})
        elif "/chats/mark-read" in url:
            return DummyResponse(200, {"success": True, "chat_jid": chat_jid, "unread": False})
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    # Trigger 2 simultaneous commits for the exact same code
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        f1 = executor.submit(whatsapp.commit_send_message, auth_code)
        f2 = executor.submit(whatsapp.commit_send_message, auth_code)
        r1 = f1.result()
        r2 = f2.result()

    # Both must succeed without crash or duplication (one sent, one already_sent or synchronized)
    assert r1["success"] is True
    assert r2["success"] is True
    assert r1["status"] == "sent"
    assert r2["status"] == "sent"


def test_scenario_5_app_state_conflict_recovery_preserves_integrity(temp_db, monkeypatch):
    """5. Recuperacao de conflito 409/LTHash sem desativar integridade e fila de retentativa."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899555555@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_5", "2026-11-07 10:20:00")

    draft = whatsapp.prepare_send_message(chat_jid=chat_jid, text="Resposta 5")
    auth_code = draft["authorization_code"]

    bridge_attempts = 0

    def mock_post(url, json=None, headers=None, timeout=None):
        nonlocal bridge_attempts
        if "/send" in url:
            return DummyResponse(200, {"success": True, "message_id": "out_5"})
        elif "/chats/mark-read" in url:
            bridge_attempts += 1
            if bridge_attempts == 1:
                # First time simulates 409 conflict
                return DummyResponse(500, {
                    "success": False,
                    "status": "internal_error",
                    "error": "Failed to sync app state with WhatsApp: 409 conflict: mismatching LTHash",
                    "unread": True,
                })
            else:
                # After recovery, bridge succeeds
                return DummyResponse(200, {
                    "success": True,
                    "chat_jid": chat_jid,
                    "unread": False,
                })
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    # Initial commit: send succeeds, but mark-read failed with 409 conflict
    res1 = whatsapp.commit_send_message(authorization_code=auth_code)
    assert res1["success"] is True
    assert res1["status"] == "sent"
    assert res1["message_sent"] is True
    assert res1["read_marked"] is False
    assert "409 conflict" in res1["read_error"]

    # Item must be in pending_read_marks queue
    q_status = whatsapp.get_read_queue_status()
    assert q_status["pending_count"] == 1
    assert q_status["items"][0]["chat_jid"] == chat_jid

    # Now run process_pending_read_marks to test automatic retry and recovery
    proc_res = whatsapp.process_pending_read_marks()
    assert proc_res["processed"] == 1
    assert proc_res["succeeded"] == 1
    assert proc_res["remaining_pending"] == 0

    # Queue status updated
    q_status_after = whatsapp.get_read_queue_status()
    assert q_status_after["pending_count"] == 0
    assert q_status_after["completed_count"] == 1


def test_scenario_6_restart_service_with_pending_marks(temp_db, monkeypatch):
    """6. Reinicializacao do servico com marcacoes pendentes."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899666666@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_old_1", "2026-11-07 10:25:00")
    whatsapp.enqueue_pending_read_mark(chat_jid, message_id="msg_old_1", last_error="Temporary timeout")

    # Simulate server shutdown and restart
    # reconcile_database() is called at startup in main.py
    whatsapp.reconcile_database()

    # Confirm pending item was retained across restart
    status_before = whatsapp.get_read_queue_status()
    assert status_before["pending_count"] == 1
    assert status_before["items"][0]["message_id"] == "msg_old_1"

    # Simulate successful recovery after restart
    def mock_post(url, json=None, headers=None, timeout=None):
        if "/chats/mark-read" in url:
            return DummyResponse(200, {"success": True, "chat_jid": chat_jid, "unread": False})
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    proc = whatsapp.process_pending_read_marks()
    assert proc["succeeded"] == 1

    status_after = whatsapp.get_read_queue_status()
    assert status_after["pending_count"] == 0
    assert status_after["completed_count"] == 1


def test_scenario_7_idempotency_after_timeout_and_retry(temp_db, monkeypatch):
    """7. Idempotencia apos timeout e retry."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899777777@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_7", "2026-11-07 10:30:00")

    draft = whatsapp.prepare_send_message(chat_jid=chat_jid, text="Mensagem com retry")
    auth_code = draft["authorization_code"]

    attempts = 0

    def mock_post(url, json=None, headers=None, timeout=None):
        nonlocal attempts
        if "/send" in url:
            attempts += 1
            if attempts == 1:
                # Simulates timeout: network dropped, but message was committed into messages table
                conn = sqlite3.connect(temp_db)
                c = conn.cursor()
                now_str = datetime.now().astimezone().isoformat(sep=" ")
                c.execute(
                    "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, 'Me', ?, ?, 1)",
                    ("wa_msg_timeout_rec", chat_jid, "Mensagem com retry", now_str),
                )
                conn.commit()
                conn.close()
                raise whatsapp.requests.RequestException("Read timed out")
            return DummyResponse(200, {"success": True, "message_id": "wa_msg_timeout_rec"})
        elif "/chats/mark-read" in url:
            return DummyResponse(200, {"success": True, "chat_jid": chat_jid, "unread": False})
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    # First attempt: raises timeout, but _reconcile_sent_message detects it and returns success!
    res1 = whatsapp.commit_send_message(authorization_code=auth_code)
    assert res1["success"] is True
    assert res1["status"] == "sent"
    assert res1.get("reconciled") is True

    # Second attempt (replay with same authorization_code):
    res2 = whatsapp.commit_send_message(authorization_code=auth_code)
    assert res2["success"] is True
    assert res2["status"] == "sent"
    assert res2.get("already_sent") is True

    # Check only 1 outbound message row exists in DB
    conn = sqlite3.connect(temp_db)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM messages WHERE chat_jid = ? AND is_from_me = 1", (chat_jid,))
    assert c.fetchone()[0] == 1
    conn.close()


def test_scenario_8_permission_or_feature_disabled(temp_db, monkeypatch):
    """8. Falhas de permissao ou funcionalidade desabilitada."""
    # Sub-case A: Write disabled (WHATSAPP_WRITE_ENABLED=false)
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "false")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899888888@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_8", "2026-11-07 10:40:00")

    draft = whatsapp.prepare_send_message(chat_jid=chat_jid, text="Tentativa bloqueada")
    res_w = whatsapp.commit_send_message(authorization_code=draft["authorization_code"])
    assert res_w["success"] is False
    assert res_w["status"] == "forbidden"
    assert "WHATSAPP_WRITE_ENABLED=false" in res_w["error"]

    # Sub-case B: Mark-read disabled (WHATSAPP_MARK_READ_ENABLED=false)
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "false")

    draft2 = whatsapp.prepare_send_message(chat_jid=chat_jid, text="Tentativa envio sem read")

    def mock_post(url, json=None, headers=None, timeout=None):
        if "/send" in url:
            return DummyResponse(200, {"success": True, "message_id": "out_8b"})
        return DummyResponse(403, {"error": "disabled"})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    res_mr = whatsapp.commit_send_message(authorization_code=draft2["authorization_code"])
    # Send must succeed!
    assert res_mr["success"] is True
    assert res_mr["message_sent"] is True
    # Read mark should be gracefully skipped/disabled
    assert res_mr["read_marked"] is False
    assert res_mr["read_status"] == "disabled"
    assert "WHATSAPP_MARK_READ_ENABLED=false" in res_mr["read_error"]


def test_scenario_9_consistency_between_db_and_whatsapp_state(temp_db, monkeypatch):
    """9. Consistencia entre banco local e estado real do WhatsApp."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899999999@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_9", "2026-11-07 10:50:00")

    # Bridge returns 500 error on mark-read
    def mock_post(url, json=None, headers=None, timeout=None):
        if "/send" in url:
            return DummyResponse(200, {"success": True, "message_id": "out_9"})
        elif "/chats/mark-read" in url:
            return DummyResponse(500, {
                "success": False,
                "status": "internal_error",
                "error": "Fatal sync error",
                "unread": True,
            })
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    draft = whatsapp.prepare_send_message(chat_jid=chat_jid, text="Resposta 9")
    res = whatsapp.commit_send_message(authorization_code=draft["authorization_code"])

    assert res["success"] is True
    assert res["message_sent"] is True
    assert res["read_marked"] is False

    # Verify that database was NOT marked as read
    chat = whatsapp.get_chat(chat_jid, include_last_message=True)
    assert chat["last_read_time"] is None
    # Chat must still be unread because outbound message does not clear inbound unread unless read was synced
    # Verify unread state
    conn = sqlite3.connect(temp_db)
    c = conn.cursor()
    c.execute("SELECT last_read_time FROM chats WHERE jid = ?", (chat_jid,))
    assert c.fetchone()[0] is None
    conn.close()


def test_scenario_10_appstate_pending_queued_contract(temp_db, monkeypatch):
    """10. Contrato de bridge retornando pending_queued quando recibos foram enviados mas AppState divergiu."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899000001@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_10", "2026-11-07 11:00:00")

    draft = whatsapp.prepare_send_message(chat_jid=chat_jid, text="Resposta 10")
    auth_code = draft["authorization_code"]

    def mock_post(url, json=None, headers=None, timeout=None):
        if "/send" in url:
            return DummyResponse(200, {"success": True, "message_id": "out_10"})
        elif "/chats/mark-read" in url:
            return DummyResponse(200, {
                "success": True,
                "chat_jid": chat_jid,
                "marked_read_through_message_id": "msg_in_10",
                "status": "pending_queued",
                "remote_synced": False,
                "app_state_error": "409 conflict: mismatching LTHash",
                "unread": False,
            })
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    res = whatsapp.commit_send_message(authorization_code=auth_code)

    assert res["success"] is True
    assert res["status"] == "sent"
    assert res["message_sent"] is True
    assert res["read_marked"] is True
    assert res["read_status"] == "pending_queued"
    assert "409 conflict" in res["read_error"]

    # Verify item was enqueued in pending_read_marks
    q_status = whatsapp.get_read_queue_status()
    assert q_status["pending_count"] == 1
    assert q_status["oldest_pending_created_at"] is not None
    assert q_status["oldest_pending_age_seconds"] is not None
    assert q_status["oldest_pending_age_seconds"] >= 0.0

    # Ensure queue_id was stored properly without AttributeError
    item = q_status["items"][0]
    assert item["chat_jid"] == chat_jid
    assert item["message_id"] == "msg_in_10"
    assert item["status"] == "pending"


def test_scenario_11_process_pending_read_marks_exponential_backoff(temp_db, monkeypatch):
    """11. Backoff exponencial e limites de retentativa na fila de recuperacao."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899000002@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_11", "2026-11-07 11:10:00")

    # Enqueue with max_retries = 2
    item_id = whatsapp.enqueue_pending_read_mark(chat_jid, message_id="msg_in_11", max_retries=2)
    assert isinstance(item_id, int)

    # Mock mark_chat_read to always report remote_synced=False (app state still failing)
    def mock_post(url, json=None, headers=None, timeout=None):
        if "/chats/mark-read" in url:
            return DummyResponse(200, {
                "success": True,
                "status": "pending_queued",
                "remote_synced": False,
                "app_state_error": "409 conflict",
            })
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    # First attempt: processed, fails remote sync, sets next_retry_at in future
    proc1 = whatsapp.process_pending_read_marks()
    assert proc1["processed"] == 1
    assert proc1["succeeded"] == 0
    assert proc1["failed"] == 0

    # Immediate second run: processes 0 because next_retry_at is in the future
    proc2 = whatsapp.process_pending_read_marks()
    assert proc2["processed"] == 0

    # Fast forward next_retry_at in DB
    conn = sqlite3.connect(temp_db)
    past_str = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    conn.execute("UPDATE pending_read_marks SET next_retry_at = ?", (past_str,))
    conn.commit()
    conn.close()

    # Second attempt: reaches max_retries (2) -> transitions to status: 'failed'
    proc3 = whatsapp.process_pending_read_marks()
    assert proc3["processed"] == 1
    assert proc3["failed"] == 1

    q_status = whatsapp.get_read_queue_status()
    assert q_status["failed_count"] == 1
    assert q_status["items"][0]["status"] == "failed"


def test_scenario_12_bridge_unreachable_enqueues_pending_read_mark(temp_db, monkeypatch):
    """12. Enfileiramento automatico quando a bridge esta inacessivel."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899000003@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_12", "2026-11-07 11:20:00")

    def mock_post(url, json=None, headers=None, timeout=None):
        raise whatsapp.requests.RequestException("Bridge down")

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    res = whatsapp.mark_chat_read(chat_jid, message_id="msg_in_12")
    assert res["success"] is False
    assert res["status"] == "unavailable"

    # Must be in queue
    q_status = whatsapp.get_read_queue_status()
    assert q_status["pending_count"] == 1
    assert q_status["items"][0]["chat_jid"] == chat_jid


def test_scenario_13_never_resend_message_on_read_failure(temp_db, monkeypatch):
    """13. Garantia absoluta de nao reenviar mensagens em falhas de leitura."""
    monkeypatch.setenv("WHATSAPP_WRITE_ENABLED", "true")
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")

    chat_jid = "554899000004@s.whatsapp.net"
    seed_chat_and_inbound_message(temp_db, chat_jid, "msg_in_13", "2026-11-07 11:30:00")

    draft = whatsapp.prepare_send_message(chat_jid=chat_jid, text="Mensagem de teste anti-duplicacao")
    auth_code = draft["authorization_code"]

    send_calls = 0

    def mock_post(url, json=None, headers=None, timeout=None):
        nonlocal send_calls
        if "/send" in url:
            send_calls += 1
            return DummyResponse(200, {"success": True, "message_id": "wa_msg_unique_13"})
        elif "/chats/mark-read" in url:
            # Fatal error during read marking
            return DummyResponse(500, {"success": False, "error": "Fatal crash during app state"})
        return DummyResponse(404, {})

    monkeypatch.setattr(whatsapp.requests, "post", mock_post)

    # First commit
    res1 = whatsapp.commit_send_message(authorization_code=auth_code)
    assert res1["success"] is True
    assert res1["status"] == "sent"
    assert res1["message_sent"] is True
    assert res1["read_marked"] is False
    assert send_calls == 1

    # Second commit attempt with same code: must NOT call /send again
    res2 = whatsapp.commit_send_message(authorization_code=auth_code)
    assert res2["success"] is True
    assert res2["status"] == "sent"
    assert res2.get("already_sent") is True
    assert send_calls == 1  # Verified: /send was NOT called a second time!
