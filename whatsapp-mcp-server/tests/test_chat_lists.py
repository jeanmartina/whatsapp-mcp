import sqlite3

import pytest

import whatsapp


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
    cursor.execute("""
        CREATE TABLE messages (
            id TEXT PRIMARY KEY,
            chat_jid TEXT,
            sender TEXT,
            content TEXT,
            timestamp TIMESTAMP,
            is_from_me BOOLEAN
        );
    """)
    conn.commit()
    conn.close()
    return db_path


def test_create_and_list_chat_lists(test_db):
    # Initially empty
    assert whatsapp.list_chat_lists() == []

    # Create a new list
    res = whatsapp.create_chat_list("Para responder")
    assert res["created"] is True
    assert res["name"] == "Para responder"
    assert res["source"] == "local"
    assert res["id"] is not None

    # List lists
    lists = whatsapp.list_chat_lists()
    assert len(lists) == 1
    assert lists[0]["name"] == "Para responder"
    assert lists[0]["chat_count"] == 0
    assert lists[0]["source"] == "local"

    # Creating same list name returns existing
    res2 = whatsapp.create_chat_list("Para responder")
    assert res2["created"] is False
    assert res2["id"] == res["id"]

    # Empty name raises ValueError
    with pytest.raises(ValueError):
        whatsapp.create_chat_list("   ")


def test_add_and_remove_chat_from_list(test_db):
    chat_jid = "554899999999@s.whatsapp.net"
    conn = sqlite3.connect(test_db)
    conn.execute("INSERT INTO chats (jid, name) VALUES (?, ?)", (chat_jid, "Contato Teste"))
    conn.commit()
    conn.close()

    # Add chat to list (auto-creates list)
    add_res = whatsapp.add_chat_to_list("Para responder", chat_jid)
    assert add_res["success"] is True
    assert add_res["chat_jid"] == chat_jid
    assert add_res["list_name"] == "Para responder"

    # Verify chat is in list
    chat_lists = whatsapp.get_chat_lists(chat_jid)
    assert len(chat_lists) == 1
    assert chat_lists[0]["name"] == "Para responder"

    # Verify count in list_chat_lists
    lists = whatsapp.list_chat_lists()
    assert len(lists) == 1
    assert lists[0]["chat_count"] == 1

    # Remove chat from list
    rem_res = whatsapp.remove_chat_from_list("Para responder", chat_jid)
    assert rem_res["success"] is True

    # Verify chat is no longer in list
    assert whatsapp.get_chat_lists(chat_jid) == []
    lists = whatsapp.list_chat_lists()
    assert lists[0]["chat_count"] == 0


def test_list_chats_by_list(test_db):
    conn = sqlite3.connect(test_db)
    conn.execute(
        "INSERT INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
        ("chat1@s.whatsapp.net", "Alice", "2026-10-06T10:00:00"),
    )
    conn.execute(
        "INSERT INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
        ("chat2@s.whatsapp.net", "Bob", "2026-10-06T11:00:00"),
    )
    conn.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, ?)",
        ("m1", "chat1@s.whatsapp.net", "chat1@s.whatsapp.net", "Olá Alice", "2026-10-06T10:00:00", 0),
    )
    conn.execute(
        "INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, ?)",
        ("m2", "chat2@s.whatsapp.net", "chat2@s.whatsapp.net", "Olá Bob", "2026-10-06T11:00:00", 0),
    )
    conn.commit()
    conn.close()

    # Add only Alice to 'Para responder'
    whatsapp.add_chat_to_list("Para responder", "chat1@s.whatsapp.net")

    # Query by list_name
    alice_chats = whatsapp.list_chats_by_list(list_name="Para responder")
    assert len(alice_chats) == 1
    assert alice_chats[0]["jid"] == "chat1@s.whatsapp.net"
    assert alice_chats[0]["name"] == "Alice"
    assert alice_chats[0]["last_message"] == "Olá Alice"

    # Query non-existent list returns empty list
    empty_chats = whatsapp.list_chats_by_list(list_name="Lista Inexistente")
    assert empty_chats == []

    # Query without list_name or list_id raises ValueError
    with pytest.raises(ValueError):
        whatsapp.list_chats_by_list()


def test_native_list_precedence_over_local_fallback(test_db):
    conn = sqlite3.connect(test_db)
    whatsapp._ensure_chat_list_schema(conn)
    # Insert chat
    conn.execute(
        "INSERT INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
        ("client@s.whatsapp.net", "Cliente VIP", "2026-10-06T12:00:00"),
    )
    # Insert both local fallback and native list with same name "Para responder"
    conn.execute(
        "INSERT INTO chat_lists (id, name, color, type, source, deleted, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("local-id-1", "Para responder", 0, "CUSTOM", "local", 0, "2026-10-06T10:00:00", "2026-10-06T10:00:00"),
    )
    conn.execute(
        "INSERT INTO chat_lists (id, name, color, type, source, deleted, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("native-id-1", "Para responder", 5, "CUSTOM", "whatsapp", 0, "2026-10-06T10:05:00", "2026-10-06T10:05:00"),
    )
    # Associate client with native list
    conn.execute(
        "INSERT INTO chat_list_items (list_id, chat_jid, created_at) VALUES (?, ?, ?)",
        ("native-id-1", "client@s.whatsapp.net", "2026-10-06T10:05:00"),
    )
    conn.commit()
    conn.close()

    # list_chat_lists should return the native list, deduplicated
    lists = whatsapp.list_chat_lists()
    assert len(lists) == 1
    assert lists[0]["id"] == "native-id-1"
    assert lists[0]["source"] == "whatsapp"
    assert lists[0]["chat_count"] == 1

    # list_chats_by_list should resolve to native list
    chats = whatsapp.list_chats_by_list(list_name="Para responder")
    assert len(chats) == 1
    assert chats[0]["jid"] == "client@s.whatsapp.net"
    assert chats[0]["name"] == "Cliente VIP"
