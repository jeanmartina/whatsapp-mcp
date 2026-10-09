"""Tests for sync status diagnostic tool, datetime query normalization, and database reconciliation."""

import sqlite3
from datetime import datetime, timezone
import pytest

import whatsapp
import main


def _create_test_db(path):
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.executescript("""
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
            quoted_message_id TEXT,
            PRIMARY KEY (id, chat_jid),
            FOREIGN KEY (chat_jid) REFERENCES chats(jid)
        );
        CREATE VIRTUAL TABLE messages_fts USING fts5(
            content,
            content='messages',
            content_rowid='rowid'
        );
        CREATE TRIGGER messages_ai AFTER INSERT ON messages BEGIN
            INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
        END;
    """)
    whatsapp._ensure_chat_list_schema(conn)

    # Insert test chat and messages
    cursor.execute(
        "INSERT INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
        ("test@s.whatsapp.net", "Test User", "2026-10-09 10:00:00-03:00"),
    )
    cursor.execute(
        """INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, media_type)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("msg1", "test@s.whatsapp.net", "test@s.whatsapp.net", "plano de trabalho e VPN", "2026-10-08 14:46:32-03:00", 0, "text"),
    )
    cursor.execute(
        """INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me, media_type)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("msg2", "test@s.whatsapp.net", "Me", "reunião confirmada?", "2026-10-09 10:00:00-03:00", 1, "image"),
    )
    conn.commit()
    conn.close()


@pytest.fixture
def sync_test_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "messages.db")
    _create_test_db(db_path)
    monkeypatch.setattr(whatsapp, "MESSAGES_DB_PATH", db_path)
    return db_path


def test_normalize_query_datetime():
    # String with 'T'
    assert whatsapp._normalize_query_datetime("2026-10-08T00:00:00-03:00") == "2026-10-08 00:00:00-03:00"
    # String with space
    assert whatsapp._normalize_query_datetime("2026-10-08 00:00:00-03:00") == "2026-10-08 00:00:00-03:00"
    # Date only
    assert whatsapp._normalize_query_datetime("2026-10-08") == "2026-10-08 00:00:00"
    # Datetime object
    dt = datetime(2026, 10, 8, 14, 46, 32)
    assert whatsapp._normalize_query_datetime(dt) == "2026-10-08 14:46:32"
    # None
    assert whatsapp._normalize_query_datetime(None) is None


def test_search_messages_with_iso_t_timestamp(sync_test_db):
    # Query using ISO string with 'T' - must find the message stored with space separator
    results = whatsapp.search_messages(
        query="plano de trabalho",
        after="2026-10-08T00:00:00-03:00",
    )
    assert len(results) == 1
    assert results[0]["id"] == "msg1"
    assert "plano de trabalho" in results[0]["content"]


def test_list_messages_with_iso_t_timestamp(sync_test_db):
    results = whatsapp.list_messages(
        after="2026-10-08T00:00:00-03:00",
        sort_by="newest",
    )
    assert len(results) == 2


def test_list_chats_message_available_and_media_type(sync_test_db):
    chats = whatsapp.list_chats(limit=10)
    assert len(chats) == 1
    assert chats[0]["jid"] == "test@s.whatsapp.net"
    assert chats[0]["message_available"] is True
    assert chats[0]["last_media_type"] == "image"
    assert chats[0]["last_message"] == "reunião confirmada?"


def test_get_sync_status(sync_test_db, monkeypatch):
    class DummyHealthResponse:
        status_code = 200
        def json(self):
            return {"status": "ok", "connected": True, "timestamp": 12345678}

    monkeypatch.setattr(whatsapp.requests, "get", lambda *args, **kwargs: DummyHealthResponse())

    status = whatsapp.get_sync_status()
    assert status["bridge_connected"] is True
    assert status["bridge_status"] == "ok"
    assert status["database_accessible"] is True
    assert status["latest_persisted_message_time"] == "2026-10-09 10:00:00-03:00"
    assert status["latest_chat_active_time"] == "2026-10-09 10:00:00-03:00"
    assert status["activity_lag_seconds"] == 0.0


def test_reconcile_database(sync_test_db):
    conn = sqlite3.connect(sync_test_db)
    # Add a newer message without updating chats
    conn.execute(
        """INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("msg3", "test@s.whatsapp.net", "test@s.whatsapp.net", "newer msg", "2026-10-09 12:00:00-03:00", 0),
    )
    # Add an un-persisted sent message to audit
    now = datetime.now(timezone.utc).isoformat(sep=" ")
    conn.execute(
        """INSERT INTO send_message_audit
           (send_id, chat_jid, recipient_name, text, text_sha256, authorization_code_hash, status, prepared_at, expires_at, sent_at, whatsapp_message_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("audit-1", "test@s.whatsapp.net", "Test User", "audit text", "sha", "code", "sent", now, now, now, "wa_audit_1"),
    )
    conn.commit()
    conn.close()

    res = whatsapp.reconcile_database()
    assert res["updated_chats"] >= 1
    assert res["reconciled_audit"] == 1

    # Verify chats was updated and audit message is now in messages
    conn = sqlite3.connect(sync_test_db)
    chat_row = conn.execute("SELECT last_message_time FROM chats WHERE jid = 'test@s.whatsapp.net'").fetchone()
    assert chat_row[0] >= "2026-10-09 12:00:00-03:00"

    msg_row = conn.execute("SELECT content FROM messages WHERE id = 'wa_audit_1'").fetchone()
    assert msg_row is not None
    assert msg_row[0] == "audit text"
    conn.close()
