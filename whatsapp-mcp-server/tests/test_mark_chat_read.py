"""Unit and integration tests for mark_chat_read and mark_chats_read tools.

Validates the full set of requirements specified in Section 11:
1. Feature flag false blocks operation (status: forbidden)
2. mark_chat_read marks direct chat as read
3. mark_chat_read works in group chats
4. Marks through the latest received message when message_id is omitted
5. Invalid / nonexistent message_id fails with not_found
6. message_id from another chat fails with invalid_argument
7. list_unread_chats excludes the chat once marked read
8. get_chat returns unread=False once marked read
9. New incoming message arriving later resets chat to unread=True
10. Restart (reopening db from disk) preserves the read marker
11. Repeated calls are idempotent
12. Connection / bridge failure does not corrupt state
13. Batch mark_chats_read functions cleanly with limits and validation
"""

import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

import main
import whatsapp


class DummyResponse:
    def __init__(self, status_code: int = 200, payload: dict[str, Any] | None = None, text: str = "OK"):
        self.status_code = status_code
        self._payload = payload or {"success": True}
        self.text = text

    def json(self) -> dict[str, Any]:
        return self._payload


SCHEMA = """
    CREATE TABLE chats (
        jid TEXT PRIMARY KEY,
        name TEXT,
        last_message_time TIMESTAMP,
        last_read_time TIMESTAMP
    );

    CREATE TABLE messages (
        id TEXT,
        chat_jid TEXT,
        sender TEXT,
        content TEXT,
        timestamp TIMESTAMP,
        is_from_me BOOLEAN,
        media_type TEXT,
        filename TEXT,
        url TEXT,
        media_key BLOB,
        file_sha256 BLOB,
        file_enc_sha256 BLOB,
        file_length INTEGER,
        deleted_at TIMESTAMP,
        PRIMARY KEY (id, chat_jid),
        FOREIGN KEY (chat_jid) REFERENCES chats(jid)
    );
"""


@pytest.fixture
def mark_read_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "messages.db")
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", db_path)

    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)

    # Seed initial test data:
    # 1. Direct DM chat
    dm_jid = "554899999999@s.whatsapp.net"
    t1 = "2026-10-07 10:00:00+00:00"
    t2 = "2026-10-07 10:05:00+00:00"
    conn.execute("INSERT INTO chats (jid, name, last_message_time, last_read_time) VALUES (?, ?, ?, NULL)", (dm_jid, "Alice", t2))
    conn.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, 0)",
        ("msg-1", dm_jid, "554899999999", "hello", t1),
    )
    conn.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, 0)",
        ("msg-2", dm_jid, "554899999999", "how are you?", t2),
    )

    # 2. Group chat with multiple participants
    group_jid = "120363012345678901@g.us"
    gt1 = "2026-10-07 11:00:00+00:00"
    gt2 = "2026-10-07 11:10:00+00:00"
    conn.execute("INSERT INTO chats (jid, name, last_message_time, last_read_time) VALUES (?, ?, ?, NULL)", (group_jid, "Project Group", gt2))
    conn.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, 0)",
        ("g-msg-1", group_jid, "554811111111", "meeting at 2", gt1),
    )
    conn.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, 0)",
        ("g-msg-2", group_jid, "554822222222", "acknowledged", gt2),
    )

    # 3. Another unread DM for batch tests
    dm_jid2 = "554888888888@s.whatsapp.net"
    bt1 = "2026-10-07 09:00:00+00:00"
    conn.execute("INSERT INTO chats (jid, name, last_message_time, last_read_time) VALUES (?, ?, ?, NULL)", (dm_jid2, "Bob", bt1))
    conn.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, 0)",
        ("b-msg-1", dm_jid2, "554888888888", "hey", bt1),
    )

    conn.commit()
    conn.close()
    return db_path


# 1. Feature flag false blocks operation
def test_feature_flag_false_blocks_operation(mark_read_db, monkeypatch):
    monkeypatch.delenv("WHATSAPP_MARK_READ_ENABLED", raising=False)
    chat_jid = "554899999999@s.whatsapp.net"

    res = whatsapp.mark_chat_read(chat_jid)
    assert res["success"] is False
    assert res["status"] == "forbidden"
    assert "WHATSAPP_MARK_READ_ENABLED=false" in res["error"]

    res_batch = whatsapp.mark_chats_read([chat_jid])
    assert res_batch["success"] is False
    assert res_batch["status"] == "forbidden"


# 2. mark_chat_read marks direct chat as read
def test_mark_chat_read_direct_chat_success(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899999999@s.whatsapp.net"

    bridge_calls = []

    def fake_post(url, json=None, headers=None, timeout=None):
        bridge_calls.append({"url": url, "json": json, "headers": headers})
        # Simulate bridge updating chats.last_read_time in SQLite
        conn = sqlite3.connect(mark_read_db)
        conn.execute("UPDATE chats SET last_read_time = '2026-10-07 10:05:00+00:00' WHERE jid = ?", (chat_jid,))
        conn.commit()
        conn.close()
        return DummyResponse(
            status_code=200,
            payload={
                "success": True,
                "chat_jid": chat_jid,
                "marked_read_through_message_id": "msg-2",
                "marked_read_through_timestamp": "2026-10-07T10:05:00+00:00",
                "previous_last_read_time": None,
                "new_last_read_time": "2026-10-07T10:05:00+00:00",
                "unread": False,
            },
        )

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)

    res = whatsapp.mark_chat_read(chat_jid)
    assert res["success"] is True
    assert res["chat_jid"] == chat_jid
    assert res["unread"] is False
    assert res["marked_read_through_message_id"] == "msg-2"
    assert len(bridge_calls) == 1
    assert bridge_calls[0]["url"].endswith("/chats/mark-read")


# 3. mark_chat_read works in group chats
def test_mark_chat_read_group_success(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    group_jid = "120363012345678901@g.us"

    def fake_post(url, json=None, headers=None, timeout=None):
        conn = sqlite3.connect(mark_read_db)
        conn.execute("UPDATE chats SET last_read_time = '2026-10-07 11:10:00+00:00' WHERE jid = ?", (group_jid,))
        conn.commit()
        conn.close()
        return DummyResponse(
            status_code=200,
            payload={
                "success": True,
                "chat_jid": group_jid,
                "marked_read_through_message_id": "g-msg-2",
                "marked_read_through_timestamp": "2026-10-07T11:10:00+00:00",
                "previous_last_read_time": None,
                "new_last_read_time": "2026-10-07T11:10:00+00:00",
                "unread": False,
            },
        )

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)

    res = whatsapp.mark_chat_read(group_jid)
    assert res["success"] is True
    assert res["chat_jid"] == group_jid
    assert res["unread"] is False


# 4. Marks through the latest received message when message_id is omitted
def test_marks_up_to_latest_received_message(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899999999@s.whatsapp.net"

    posted_payload = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        nonlocal posted_payload
        posted_payload = json or {}
        return DummyResponse(
            status_code=200,
            payload={
                "success": True,
                "chat_jid": chat_jid,
                "marked_read_through_message_id": "msg-2",
                "marked_read_through_timestamp": "2026-10-07T10:05:00+00:00",
                "previous_last_read_time": None,
                "new_last_read_time": "2026-10-07T10:05:00+00:00",
                "unread": False,
            },
        )

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)

    res = whatsapp.mark_chat_read(chat_jid)
    assert res["success"] is True
    assert res["marked_read_through_message_id"] == "msg-2"
    # When message_id is omitted, payload contains only chat_jid
    assert "message_id" not in posted_payload


# 5. Invalid / nonexistent message_id fails with not_found
def test_invalid_message_id_fails(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899999999@s.whatsapp.net"

    res = whatsapp.mark_chat_read(chat_jid, message_id="nonexistent-id")
    assert res["success"] is False
    assert res["status"] == "not_found"
    assert "Message 'nonexistent-id' not found" in res["error"]


# 6. message_id from another chat fails with invalid_argument
def test_message_id_from_other_chat_fails(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899999999@s.whatsapp.net"
    # g-msg-1 belongs to group_jid, not dm_jid
    res = whatsapp.mark_chat_read(chat_jid, message_id="g-msg-1")
    assert res["success"] is False
    assert res["status"] == "invalid_argument"
    assert "does not belong to chat" in res["error"]


# 7. list_unread_chats excludes the chat once marked read
def test_list_unread_chats_excludes_marked_chat(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899999999@s.whatsapp.net"

    # Before: chat is unread
    unread_chats_before = [c["jid"] for c in whatsapp.list_unread_chats()]
    assert chat_jid in unread_chats_before

    # Mark as read
    def fake_post(url, json=None, headers=None, timeout=None):
        conn = sqlite3.connect(mark_read_db)
        conn.execute("UPDATE chats SET last_read_time = '2026-10-07 10:05:00+00:00' WHERE jid = ?", (chat_jid,))
        conn.commit()
        conn.close()
        return DummyResponse(
            status_code=200,
            payload={
                "success": True,
                "chat_jid": chat_jid,
                "marked_read_through_message_id": "msg-2",
                "marked_read_through_timestamp": "2026-10-07T10:05:00+00:00",
                "previous_last_read_time": None,
                "new_last_read_time": "2026-10-07T10:05:00+00:00",
                "unread": False,
            },
        )

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)
    res = whatsapp.mark_chat_read(chat_jid)
    assert res["success"] is True

    # After: chat is no longer in unread list
    unread_chats_after = [c["jid"] for c in whatsapp.list_unread_chats()]
    assert chat_jid not in unread_chats_after


# 8. get_chat returns unread=False once marked read
def test_get_chat_returns_unread_false(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899999999@s.whatsapp.net"

    chat_before = whatsapp.get_chat(chat_jid)
    assert chat_before is not None
    assert chat_before["unread"] is True

    def fake_post(url, json=None, headers=None, timeout=None):
        conn = sqlite3.connect(mark_read_db)
        conn.execute("UPDATE chats SET last_read_time = '2026-10-07 10:05:00+00:00' WHERE jid = ?", (chat_jid,))
        conn.commit()
        conn.close()
        return DummyResponse(
            status_code=200,
            payload={
                "success": True,
                "chat_jid": chat_jid,
                "marked_read_through_message_id": "msg-2",
                "marked_read_through_timestamp": "2026-10-07T10:05:00+00:00",
                "previous_last_read_time": None,
                "new_last_read_time": "2026-10-07T10:05:00+00:00",
                "unread": False,
            },
        )

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)
    whatsapp.mark_chat_read(chat_jid)

    chat_after = whatsapp.get_chat(chat_jid)
    assert chat_after is not None
    assert chat_after["unread"] is False
    assert chat_after["last_read_time"] is not None


# 9. New incoming message arriving later resets chat to unread=True
def test_new_message_after_mark_read_resets_unread_to_true(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899999999@s.whatsapp.net"

    # Mark as read at 10:05
    conn = sqlite3.connect(mark_read_db)
    conn.execute("UPDATE chats SET last_read_time = '2026-10-07 10:05:00+00:00' WHERE jid = ?", (chat_jid,))
    conn.commit()
    conn.close()

    chat = whatsapp.get_chat(chat_jid)
    assert chat["unread"] is False

    # Simulate a new message arriving at 10:30
    t3 = "2026-10-07 10:30:00+00:00"
    conn = sqlite3.connect(mark_read_db)
    conn.execute("UPDATE chats SET last_message_time = ? WHERE jid = ?", (t3, chat_jid))
    conn.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, 0)",
        ("msg-3", chat_jid, "554899999999", "new message", t3),
    )
    conn.commit()
    conn.close()

    chat_updated = whatsapp.get_chat(chat_jid)
    assert chat_updated["unread"] is True
    assert chat_jid in [c["jid"] for c in whatsapp.list_unread_chats()]


# 10. Restart (reopening db from disk) preserves the read marker
def test_restart_preserves_read_state(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899999999@s.whatsapp.net"

    # Set read marker in SQLite
    read_ts = "2026-10-07 10:05:00+00:00"
    conn = sqlite3.connect(mark_read_db)
    conn.execute("UPDATE chats SET last_read_time = ? WHERE jid = ?", (read_ts, chat_jid))
    conn.commit()
    conn.close()

    # Simulate fresh connection / server restart by reading afresh
    chat = whatsapp.get_chat(chat_jid)
    assert chat["unread"] is False
    assert chat["last_read_time"] == datetime.fromisoformat(read_ts).isoformat()


# 11. Repeated calls are idempotent
def test_repeated_call_is_idempotent(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899999999@s.whatsapp.net"

    call_count = 0

    def fake_post(url, json=None, headers=None, timeout=None):
        nonlocal call_count
        call_count += 1
        conn = sqlite3.connect(mark_read_db)
        conn.execute("UPDATE chats SET last_read_time = '2026-10-07 10:05:00+00:00' WHERE jid = ?", (chat_jid,))
        conn.commit()
        conn.close()
        return DummyResponse(
            status_code=200,
            payload={
                "success": True,
                "chat_jid": chat_jid,
                "marked_read_through_message_id": "msg-2",
                "marked_read_through_timestamp": "2026-10-07T10:05:00+00:00",
                "previous_last_read_time": "2026-10-07T10:05:00+00:00" if call_count > 1 else None,
                "new_last_read_time": "2026-10-07T10:05:00+00:00",
                "unread": False,
            },
        )

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)

    res1 = whatsapp.mark_chat_read(chat_jid)
    res2 = whatsapp.mark_chat_read(chat_jid)

    assert res1["success"] is True
    assert res2["success"] is True
    assert res1["unread"] is False
    assert res2["unread"] is False
    assert call_count == 2


# 12. Connection / bridge failure does not corrupt state
def test_connection_failure_does_not_corrupt_state(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    chat_jid = "554899999999@s.whatsapp.net"

    # Capture initial state
    chat_initial = whatsapp.get_chat(chat_jid)
    initial_last_read = chat_initial["last_read_time"]

    # Simulate bridge returning 503 unavailable
    def fake_post(url, json=None, headers=None, timeout=None):
        return DummyResponse(
            status_code=503,
            payload={
                "success": False,
                "status": "unavailable",
                "error": "WhatsApp client is not connected. Please wait for reconnection.",
            },
        )

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)

    res = whatsapp.mark_chat_read(chat_jid)
    assert res["success"] is False
    assert res["status"] == "unavailable"

    # State in database must be preserved unmodified
    chat_after = whatsapp.get_chat(chat_jid)
    assert chat_after["last_read_time"] == initial_last_read
    assert chat_after["unread"] is True


# 13. Batch mark_chats_read functions cleanly with limits and validation
def test_batch_mark_chats_read_success_and_validation(mark_read_db, monkeypatch):
    monkeypatch.setenv("WHATSAPP_MARK_READ_ENABLED", "true")
    dm1 = "554899999999@s.whatsapp.net"
    dm2 = "554888888888@s.whatsapp.net"

    # 13a. Empty batch validation
    empty_res = whatsapp.mark_chats_read([])
    assert empty_res["success"] is False
    assert empty_res["status"] == "invalid_argument"

    # 13b. Exceeds limit (> 50)
    over_limit_res = whatsapp.mark_chats_read([f"chat{i}@s.whatsapp.net" for i in range(51)])
    assert over_limit_res["success"] is False
    assert over_limit_res["status"] == "invalid_argument"
    assert "Maximum of 50" in over_limit_res["error"]

    # 13c. Successful batch operation
    def fake_post(url, json=None, headers=None, timeout=None):
        jid = json.get("chat_jid")
        return DummyResponse(
            status_code=200,
            payload={
                "success": True,
                "chat_jid": jid,
                "marked_read_through_message_id": "msg",
                "marked_read_through_timestamp": "2026-10-07T10:05:00+00:00",
                "previous_last_read_time": None,
                "new_last_read_time": "2026-10-07T10:05:00+00:00",
                "unread": False,
            },
        )

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)

    batch_res = whatsapp.mark_chats_read([dm1, dm2])
    assert batch_res["success"] is True
    assert batch_res["total"] == 2
    assert len(batch_res["results"]) == 2
    assert batch_res["results"][0]["success"] is True
    assert batch_res["results"][1]["success"] is True
