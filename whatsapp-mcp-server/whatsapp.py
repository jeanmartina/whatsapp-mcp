import hashlib
import json
import os
import os.path
import random
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import requests

import audio

# Configuration via environment variables with sensible defaults
_DEFAULT_BRIDGE_STORE_DIR = os.path.expanduser("~/.local/share/whatsapp-mcp/data/store")
MESSAGES_DB_PATH = os.getenv(
    "WHATSAPP_DB_PATH",
    os.path.join(_DEFAULT_BRIDGE_STORE_DIR, "messages.db"),
)
WHATSMEOW_DB_PATH = os.getenv(
    "WHATSMEOW_DB_PATH",
    os.path.join(_DEFAULT_BRIDGE_STORE_DIR, "whatsapp.db"),
)
WHATSAPP_API_BASE_URL = os.getenv("WHATSAPP_API_URL", "http://localhost:8080/api")

_BRIDGE_TOKEN_PATH = os.path.join(os.path.dirname(WHATSMEOW_DB_PATH), ".bridge-token")


def _read_bridge_token() -> str | None:
    env = os.getenv("WHATSAPP_BRIDGE_TOKEN", "").strip()
    if env:
        return env
    try:
        with open(_BRIDGE_TOKEN_PATH, encoding="utf-8") as fh:
            value = fh.read().strip()
            return value or None
    except FileNotFoundError:
        return None
    except OSError:
        return None


def _bridge_headers() -> dict[str, str]:
    token = _read_bridge_token()
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def is_write_enabled() -> bool:
    """Return whether write operations (such as message sending) are enabled."""
    return os.getenv("WHATSAPP_WRITE_ENABLED", "false").lower() in ("true", "1", "yes")


def is_mark_read_enabled() -> bool:
    """Return whether marking chats as read is enabled."""
    return os.getenv("WHATSAPP_MARK_READ_ENABLED", "false").lower() in ("true", "1", "yes")


def _ensure_chat_list_schema(conn: sqlite3.Connection) -> None:
    """Ensure chat_lists, chat_list_items, and send_message_audit tables exist."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chat_lists (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            color INTEGER DEFAULT 0,
            type TEXT DEFAULT 'CUSTOM',
            source TEXT DEFAULT 'whatsapp',
            deleted BOOLEAN DEFAULT 0,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """)
    cursor.execute("""
        DROP INDEX IF EXISTS idx_chat_lists_name;
    """)
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_lists_name_source ON chat_lists(name, source) WHERE deleted = 0;
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chat_list_items (
            list_id TEXT NOT NULL,
            chat_jid TEXT NOT NULL,
            created_at TIMESTAMP,
            PRIMARY KEY (list_id, chat_jid)
        );
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_chat_list_items_chat_jid ON chat_list_items(chat_jid);
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS send_message_audit (
            send_id TEXT PRIMARY KEY,
            chat_jid TEXT NOT NULL,
            recipient_name TEXT,
            text TEXT NOT NULL,
            text_sha256 TEXT NOT NULL,
            reply_to_message_id TEXT,
            authorization_code_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            prepared_at TIMESTAMP NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            authorized_at TIMESTAMP,
            sent_at TIMESTAMP,
            whatsapp_message_id TEXT,
            error_message TEXT
        );
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_send_audit_status ON send_message_audit(status);
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_send_audit_chat_jid ON send_message_audit(chat_jid);
    """)
    conn.commit()


@dataclass
class Message:
    timestamp: datetime
    sender: str
    content: str
    is_from_me: bool
    chat_jid: str
    id: str
    chat_name: str | None = None
    media_type: str | None = None
    # For media_type == "reaction", the bridge stores the reacted-to message ID
    # in the `filename` column. Exposed to callers as `reaction_to_message_id`.
    filename: str | None = None
    # ID of the message this one is replying to (NULL for non-replies).
    quoted_message_id: str | None = None


@dataclass
class Chat:
    jid: str
    name: str | None
    last_message_time: datetime | None
    last_message: str | None = None
    last_sender: str | None = None
    last_is_from_me: bool | None = None
    # Bridge read marker (chats.last_read_time): how far we have read this
    # chat, from read receipts and history-sync backfill. NULL when the
    # bridge has never seen a read for the chat, or predates the column.
    last_read_time: datetime | None = None

    @property
    def is_group(self) -> bool:
        """Determine if chat is a group based on JID pattern."""
        return self.jid.endswith("@g.us")

    @property
    def unread(self) -> bool:
        """Whether the chat's last message is inbound and unread by us.

        With a read marker this is genuine unread — a chat read on the phone
        or another linked device is not reported. Without one (older bridge,
        or a chat WhatsApp never reported a read for) it degrades to the old
        heuristic: unread if the last message is inbound.

        A missing last-message row (`last_is_from_me is None`) cannot establish
        direction — protocol/unsupported events can advance last_message_time
        without storing a message — so those chats are not reported as unread.
        """
        if self.last_message_time is None or self.last_is_from_me is None:
            return False
        if self.last_is_from_me:
            return False
        if self.last_read_time is None:
            return True
        return self.last_message_time > self.last_read_time


@dataclass
class Contact:
    phone_number: str
    name: str | None
    jid: str


@dataclass
class MessageContext:
    message: Message
    before: list[Message]
    after: list[Message]


def msg_to_dict(message: Message, include_sender_name: bool = True) -> dict[str, Any]:
    """Convert a Message dataclass to a dictionary for JSON serialization."""
    # Extract phone number from JID (e.g., "1234567890@s.whatsapp.net" -> "1234567890")
    sender_phone = message.sender.split("@")[0] if "@" in message.sender else message.sender

    sender_name = None
    sender_display = None
    if include_sender_name:
        if message.is_from_me:
            sender_name = "Me"
            sender_display = "Me"
        else:
            resolved_name = get_sender_name(message.sender)
            # Check if we got an actual name (not just the JID back)
            if resolved_name and resolved_name != message.sender and resolved_name != sender_phone:
                sender_name = resolved_name
                sender_display = f"{resolved_name} ({sender_phone})"
            else:
                sender_name = sender_phone
                sender_display = sender_phone

    return {
        "id": message.id,
        "timestamp": message.timestamp.isoformat(),
        "sender_jid": message.sender,
        "sender_phone": sender_phone,
        "sender_name": sender_name,
        "sender_display": sender_display,  # "Name (phone)" or just phone if no name
        "content": message.content,
        "is_from_me": message.is_from_me,
        "chat_jid": message.chat_jid,
        "chat_name": message.chat_name,
        "media_type": message.media_type,
        "reaction_to_message_id": (message.filename if message.media_type == "reaction" else None),
        "quoted_message_id": message.quoted_message_id,
    }


def chat_to_dict(chat: "Chat") -> dict[str, Any]:
    """Convert a Chat dataclass to a dictionary for JSON serialization."""
    return {
        "jid": chat.jid,
        "name": chat.name,
        "is_group": chat.is_group,
        "last_message_time": chat.last_message_time.isoformat() if chat.last_message_time else None,
        "last_message": chat.last_message,
        "last_sender": chat.last_sender,
        "last_is_from_me": chat.last_is_from_me,
        "last_read_time": chat.last_read_time.isoformat() if chat.last_read_time else None,
        "unread": chat.unread,
    }


def contact_to_dict(contact: "Contact") -> dict[str, Any]:
    """Convert a Contact dataclass to a dictionary for JSON serialization."""
    return {"phone_number": contact.phone_number, "name": contact.name, "jid": contact.jid}


def _last_read_time_select(cursor: sqlite3.Cursor, table_alias: str) -> str:
    """SELECT expression for chats.last_read_time, or a NULL literal.

    The bridge adds the column through its own migration, so a messages.db
    written by an older bridge doesn't have it yet. Reads must keep working
    against such a store — those chats simply report last_read_time = None.
    """
    columns = {row[1] for row in cursor.execute("PRAGMA table_info(chats)").fetchall()}
    return f"{table_alias}.last_read_time" if "last_read_time" in columns else "NULL"


def _last_message_join(chat_alias: str, msg_alias: str) -> str:
    """Deterministic single-row join to the chat's latest message.

    Multiple messages can share last_message_time (history sync is second-
    resolution). Joining solely on timestamp would duplicate chat rows and
    make last_is_from_me / unread non-deterministic; pick one id as tie-break.
    """
    return f"""
            LEFT JOIN messages {msg_alias} ON {chat_alias}.jid = {msg_alias}.chat_jid
                AND {msg_alias}.id = (
                    SELECT m.id FROM messages m
                    WHERE m.chat_jid = {chat_alias}.jid
                      AND m.timestamp = {chat_alias}.last_message_time
                    ORDER BY m.id DESC
                    LIMIT 1
                )
    """


def _sender_aliases(value: str) -> list[str]:
    # messages.sender is written inconsistently: the same contact may appear as
    # bare phone ("13232432100"), full phone JID ("13232432100@s.whatsapp.net"),
    # bare LID ("231241139937355"), or full LID JID ("231241139937355@lid").
    # whatsmeow_lid_map (whatsapp.db) maps pn<->lid; we emit all four forms so
    # an IN-based filter catches every row regardless of which form was stored.
    bare = value.split("@", 1)[0]
    pn: str | None = None
    lid: str | None = None
    if os.path.isfile(WHATSMEOW_DB_PATH):
        try:
            conn = sqlite3.connect(WHATSMEOW_DB_PATH)
            try:
                row = conn.execute("SELECT lid FROM whatsmeow_lid_map WHERE pn = ?", (bare,)).fetchone()
                if row:
                    pn, lid = bare, row[0]
                else:
                    row = conn.execute("SELECT pn FROM whatsmeow_lid_map WHERE lid = ?", (bare,)).fetchone()
                    if row:
                        lid, pn = bare, row[0]
            finally:
                conn.close()
        except sqlite3.Error:
            pass

    aliases: list[str] = []
    if pn:
        aliases += [pn, f"{pn}@s.whatsapp.net"]
    if lid:
        aliases += [lid, f"{lid}@lid"]
    if not aliases:
        # No mapping found; emit the bare form plus both possible suffixes so
        # we still match whichever form the bridge happened to store.
        aliases = [bare, f"{bare}@s.whatsapp.net", f"{bare}@lid"]
    return aliases


def _resolve_lid_to_phone(lid_or_jid: str) -> str | None:
    """Resolve a WhatsApp LID (linked device identifier) to a phone number.

    WhatsApp's newer protocol uses opaque LIDs (e.g. '35047067385985') as sender
    identifiers instead of phone numbers. The whatsmeow_lid_map table maps these
    back to real phone numbers.

    Returns the phone number if found, None otherwise.
    """
    if not os.path.exists(WHATSMEOW_DB_PATH):
        return None
    # Extract the numeric part from JID-style strings (e.g. '35047067385985@lid')
    lid = lid_or_jid.split("@")[0] if "@" in lid_or_jid else lid_or_jid
    try:
        conn = sqlite3.connect(WHATSMEOW_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT pn FROM whatsmeow_lid_map WHERE lid = ? LIMIT 1", (lid,))
        row = cursor.fetchone()
        return row[0] if row else None
    except sqlite3.Error:
        return None
    finally:
        if "conn" in locals():
            conn.close()


def _resolve_name_from_whatsmeow(jid: str) -> str | None:
    """Look up a contact name from whatsmeow's contact store (whatsapp.db).

    Handles both standard JIDs (12345@s.whatsapp.net) and LIDs (opaque numeric
    identifiers used by WhatsApp's linked device protocol). LIDs are first
    resolved to phone numbers via whatsmeow_lid_map, then looked up in contacts.

    Falls back gracefully if the DB or table doesn't exist.
    """
    if not os.path.exists(WHATSMEOW_DB_PATH):
        return None

    lookup_jid = jid
    jid_prefix = jid.split("@")[0] if "@" in jid else jid
    jid_suffix = jid.split("@")[1] if "@" in jid else ""

    # If this is a LID (@lid suffix) or a raw number, try LID map first.
    # LIDs overlap in length with phone numbers (12-15 digits) so we always
    # attempt LID resolution and fall through to direct contact lookup if not found.
    if jid_suffix in ("lid", ""):
        phone = _resolve_lid_to_phone(jid_prefix)
        if phone:
            lookup_jid = phone + "@s.whatsapp.net"
        elif jid_suffix == "lid":
            # Definitely a LID but not in the map — can't resolve
            return None

    try:
        conn = sqlite3.connect(WHATSMEOW_DB_PATH)
        cursor = conn.cursor()
        # whatsmeow_contacts columns: our_jid, their_jid, first_name, full_name, push_name, business_name
        cursor.execute(
            "SELECT full_name, push_name, first_name, business_name FROM whatsmeow_contacts WHERE their_jid = ? LIMIT 1",
            (lookup_jid,),
        )
        row = cursor.fetchone()
        if row:
            # Prefer full_name, then push_name, then first_name, then business_name
            return row[0] or row[1] or row[2] or row[3] or None
        return None
    except sqlite3.Error:
        return None
    finally:
        if "conn" in locals():
            conn.close()


def get_sender_name(sender_jid: str) -> str:
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        # First try matching by exact JID
        cursor.execute(
            """
            SELECT name
            FROM chats
            WHERE jid = ?
            LIMIT 1
        """,
            (sender_jid,),
        )

        result = cursor.fetchone()

        # If no result, try looking for the number within JIDs
        if not result:
            # Extract the phone number part if it's a JID
            if "@" in sender_jid:
                phone_part = sender_jid.split("@")[0]
            else:
                phone_part = sender_jid

            cursor.execute(
                """
                SELECT name
                FROM chats
                WHERE jid LIKE ?
                LIMIT 1
            """,
                (f"%{phone_part}%",),
            )

            result = cursor.fetchone()

        if result and result[0] and not result[0].replace("+", "").isdigit():
            return result[0]

        # Fall back to whatsmeow contact store
        whatsmeow_name = _resolve_name_from_whatsmeow(sender_jid)
        if whatsmeow_name:
            return whatsmeow_name

        # Try with @s.whatsapp.net suffix if bare number
        if "@" not in sender_jid:
            whatsmeow_name = _resolve_name_from_whatsmeow(sender_jid + "@s.whatsapp.net")
            if whatsmeow_name:
                return whatsmeow_name

        return sender_jid

    except sqlite3.Error as e:
        print(f"Database error while getting sender name: {e}")
        return sender_jid
    finally:
        if "conn" in locals():
            conn.close()


def format_message(message: Message, show_chat_info: bool = True) -> None:
    """Print a single message with consistent formatting."""
    output = ""

    if show_chat_info and message.chat_name:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] Chat: {message.chat_name} "
    else:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] "

    content_prefix = ""
    if hasattr(message, "media_type") and message.media_type:
        content_prefix = f"[{message.media_type} - Message ID: {message.id} - Chat JID: {message.chat_jid}] "

    try:
        sender_name = get_sender_name(message.sender) if not message.is_from_me else "Me"
        output += f"From: {sender_name}: {content_prefix}{message.content}\n"
    except Exception as e:
        print(f"Error formatting message: {e}")
    return output


def format_messages_list(messages: list[Message], show_chat_info: bool = True) -> None:
    output = ""
    if not messages:
        output += "No messages to display."
        return output

    for message in messages:
        output += format_message(message, show_chat_info)
    return output


def list_messages(
    after: str | None = None,
    before: str | None = None,
    sender_phone_number: str | None = None,
    chat_jid: str | None = None,
    query: str | None = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1,
    sort_by: str = "newest",
) -> list[dict[str, Any]]:
    """Get messages matching the specified criteria with optional context.

    Args:
        after: Optional ISO-8601 formatted string to only return messages after this date
        before: Optional ISO-8601 formatted string to only return messages before this date
        sender_phone_number: Optional phone number to filter messages by sender
        chat_jid: Optional chat JID to filter messages by chat
        query: Optional search term to filter messages by content
        limit: Maximum number of messages to return (default 20)
        page: Page number for pagination (default 0)
        include_context: Whether to include messages before and after matches (default True)
        context_before: Number of messages to include before each match (default 1)
        context_after: Number of messages to include after each match (default 1)
        sort_by: Sort order - "newest" (default) or "oldest" for chronological ordering

    Returns:
        List of message dictionaries with id, timestamp, sender, content, etc.
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        # Build base query
        query_parts = [
            "SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type, messages.quoted_message_id, messages.filename FROM messages"
        ]
        query_parts.append("JOIN chats ON messages.chat_jid = chats.jid")
        where_clauses = []
        params = []

        # Add filters
        if after:
            try:
                after = datetime.fromisoformat(after)
            except ValueError:
                raise ValueError(f"Invalid date format for 'after': {after}. Please use ISO-8601 format.")

            where_clauses.append("messages.timestamp > ?")
            params.append(after)

        if before:
            try:
                before = datetime.fromisoformat(before)
            except ValueError:
                raise ValueError(f"Invalid date format for 'before': {before}. Please use ISO-8601 format.")

            where_clauses.append("messages.timestamp < ?")
            params.append(before)

        if sender_phone_number:
            aliases = _sender_aliases(sender_phone_number)
            placeholders = ",".join("?" * len(aliases))
            where_clauses.append(f"messages.sender IN ({placeholders})")
            params.extend(aliases)

        if chat_jid:
            where_clauses.append("messages.chat_jid = ?")
            params.append(chat_jid)

        if query:
            # SQLite's LOWER() only handles ASCII, so LIKE LOWER(...) silently
            # excludes Unicode matches. instr() on the raw column preserves them.
            where_clauses.append("(instr(LOWER(messages.content), LOWER(?)) > 0 OR instr(messages.content, ?) > 0)")
            params.extend([query, query])

        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))

        # Add sorting and pagination
        offset = page * limit
        order = "DESC" if sort_by == "newest" else "ASC"
        query_parts.append(f"ORDER BY messages.timestamp {order}")
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])

        cursor.execute(" ".join(query_parts), tuple(params))
        messages = cursor.fetchall()

        result = []
        for msg in messages:
            message = Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7],
                quoted_message_id=msg[8] if len(msg) > 8 else None,
                filename=msg[9] if len(msg) > 9 else None,
            )
            result.append(message)

        if include_context and result:
            # Add context for each message, deduplicated by message ID
            seen_ids = set()
            messages_with_context = []
            for msg in result:
                context = get_message_context(msg.id, context_before, context_after)
                for ctx_msg in context.before:
                    if ctx_msg.id not in seen_ids:
                        seen_ids.add(ctx_msg.id)
                        messages_with_context.append(ctx_msg)
                if context.message.id not in seen_ids:
                    seen_ids.add(context.message.id)
                    messages_with_context.append(context.message)
                for ctx_msg in context.after:
                    if ctx_msg.id not in seen_ids:
                        seen_ids.add(ctx_msg.id)
                        messages_with_context.append(ctx_msg)

            return [msg_to_dict(msg) for msg in messages_with_context]

        # Return messages without context
        return [msg_to_dict(msg) for msg in result]

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def get_message_context(message_id: str, before: int = 5, after: int = 5) -> MessageContext:
    """Get context around a specific message."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        # Get the target message first
        cursor.execute(
            """
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.chat_jid, messages.media_type, messages.quoted_message_id, messages.filename
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.id = ?
        """,
            (message_id,),
        )
        msg_data = cursor.fetchone()

        if not msg_data:
            raise ValueError(f"Message with ID {message_id} not found")

        target_message = Message(
            timestamp=datetime.fromisoformat(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[8],
            quoted_message_id=msg_data[9] if len(msg_data) > 9 else None,
            filename=msg_data[10] if len(msg_data) > 10 else None,
        )

        # Get messages before
        cursor.execute(
            """
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type, messages.quoted_message_id, messages.filename
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ? AND messages.timestamp < ?
            ORDER BY messages.timestamp DESC
            LIMIT ?
        """,
            (msg_data[7], msg_data[0], before),
        )

        before_messages = []
        for msg in cursor.fetchall():
            before_messages.append(
                Message(
                    timestamp=datetime.fromisoformat(msg[0]),
                    sender=msg[1],
                    chat_name=msg[2],
                    content=msg[3],
                    is_from_me=msg[4],
                    chat_jid=msg[5],
                    id=msg[6],
                    media_type=msg[7],
                    quoted_message_id=msg[8] if len(msg) > 8 else None,
                    filename=msg[9] if len(msg) > 9 else None,
                )
            )

        # Get messages after
        cursor.execute(
            """
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type, messages.quoted_message_id, messages.filename
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ? AND messages.timestamp > ?
            ORDER BY messages.timestamp ASC
            LIMIT ?
        """,
            (msg_data[7], msg_data[0], after),
        )

        after_messages = []
        for msg in cursor.fetchall():
            after_messages.append(
                Message(
                    timestamp=datetime.fromisoformat(msg[0]),
                    sender=msg[1],
                    chat_name=msg[2],
                    content=msg[3],
                    is_from_me=msg[4],
                    chat_jid=msg[5],
                    id=msg[6],
                    media_type=msg[7],
                    quoted_message_id=msg[8] if len(msg) > 8 else None,
                    filename=msg[9] if len(msg) > 9 else None,
                )
            )

        return MessageContext(message=target_message, before=before_messages, after=after_messages)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        if "conn" in locals():
            conn.close()


def list_chats(
    query: str | None = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active",
) -> list[dict[str, Any]]:
    """Get chats matching the specified criteria.

    Returns:
        List of chat dictionaries with jid, name, is_group, last_message, etc.
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        # The last message is always joined — is_from_me feeds the unread
        # flag — but its content is only selected when asked for. The columns
        # are referenced by tuple index downstream, so the result shape stays
        # constant across the branch.
        if include_last_message:
            last_message_select = "messages.content as last_message, messages.sender as last_sender"
        else:
            last_message_select = "NULL as last_message, NULL as last_sender"

        query_parts = [
            f"""
            SELECT
                chats.jid,
                chats.name,
                chats.last_message_time,
                {last_message_select},
                messages.is_from_me as last_is_from_me,
                {_last_read_time_select(cursor, "chats")}
            FROM chats
            {_last_message_join("chats", "messages")}
        """
        ]

        where_clauses = []
        params = []

        if query:
            # instr() on the raw column matches Unicode; LOWER()+LIKE only covers ASCII.
            where_clauses.append(
                "(instr(LOWER(chats.name), LOWER(?)) > 0 OR instr(chats.name, ?) > 0 OR chats.jid LIKE ?)"
            )
            params.extend([query, query, f"%{query}%"])

        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))

        # Add sorting
        order_by = "chats.last_message_time DESC" if sort_by == "last_active" else "chats.name"
        query_parts.append(f"ORDER BY {order_by}")

        # Add pagination
        offset = (page) * limit
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])

        cursor.execute(" ".join(query_parts), tuple(params))
        chats = cursor.fetchall()

        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5],
                last_read_time=datetime.fromisoformat(chat_data[6]) if chat_data[6] else None,
            )
            result.append(chat_to_dict(chat))

        return result

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def search_contacts(query: str) -> list[dict[str, Any]]:
    """Search contacts by name or phone number.

    Searches both the messages.db chats table and whatsmeow's contact store
    (whatsapp.db) to find contacts. Results are deduplicated by JID.
    """
    seen_jids: set[str] = set()
    result: list[dict[str, Any]] = []
    # JIDs are all ASCII so LIKE is safe; names use instr() because SQLite's
    # LOWER() only folds case for ASCII and would drop Unicode matches.
    jid_pattern = "%" + query + "%"

    # 1) Search messages.db chats table (existing behavior)
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT jid, name
            FROM chats
            WHERE
                (instr(LOWER(name), LOWER(?)) > 0 OR instr(name, ?) > 0 OR jid LIKE ?)
                AND jid NOT LIKE '%@g.us'
            ORDER BY name, jid
            LIMIT 50
        """,
            (query, query, jid_pattern),
        )
        for jid, name in cursor.fetchall():
            if jid not in seen_jids:
                seen_jids.add(jid)
                contact = Contact(phone_number=jid.split("@")[0], name=name, jid=jid)
                result.append(contact_to_dict(contact))
    except sqlite3.Error as e:
        print(f"Database error (messages.db): {e}")
    finally:
        if "conn" in locals():
            conn.close()

    # 2) Search whatsmeow contact store (whatsapp.db)
    if os.path.exists(WHATSMEOW_DB_PATH):
        try:
            conn2 = sqlite3.connect(WHATSMEOW_DB_PATH)
            cursor2 = conn2.cursor()
            cursor2.execute(
                """
                SELECT their_jid, full_name, push_name, first_name, business_name
                FROM whatsmeow_contacts
                WHERE
                    instr(LOWER(full_name), LOWER(?)) > 0 OR instr(full_name, ?) > 0
                    OR instr(LOWER(push_name), LOWER(?)) > 0 OR instr(push_name, ?) > 0
                    OR instr(LOWER(first_name), LOWER(?)) > 0 OR instr(first_name, ?) > 0
                    OR instr(LOWER(business_name), LOWER(?)) > 0 OR instr(business_name, ?) > 0
                    OR their_jid LIKE ?
                LIMIT 50
            """,
                (query, query, query, query, query, query, query, query, jid_pattern),
            )
            for their_jid, full_name, push_name, first_name, business_name in cursor2.fetchall():
                if their_jid not in seen_jids:
                    seen_jids.add(their_jid)
                    name = full_name or push_name or first_name or business_name or ""
                    contact = Contact(phone_number=their_jid.split("@")[0], name=name, jid=their_jid)
                    result.append(contact_to_dict(contact))
        except sqlite3.Error as e:
            print(f"Database error (whatsapp.db): {e}")
        finally:
            if "conn2" in locals():
                conn2.close()

    return result


def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> list[dict[str, Any]]:
    """Get all chats involving the contact.

    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        aliases = _sender_aliases(jid)
        placeholders = ",".join("?" * len(aliases))
        cursor.execute(
            f"""
            SELECT DISTINCT
                c.jid,
                c.name,
                c.last_message_time,
                last_msg.content as last_message,
                last_msg.sender as last_sender,
                last_msg.is_from_me as last_is_from_me,
                {_last_read_time_select(cursor, "c")}
            FROM chats c
            {_last_message_join("c", "last_msg")}
            WHERE EXISTS (
                SELECT 1
                FROM messages contact_msg
                WHERE contact_msg.chat_jid = c.jid
                    AND contact_msg.sender IN ({placeholders})
            ) OR c.jid = ?
            ORDER BY c.last_message_time DESC
            LIMIT ? OFFSET ?
        """,
            (*aliases, jid, limit, page * limit),
        )

        chats = cursor.fetchall()

        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5],
                last_read_time=datetime.fromisoformat(chat_data[6]) if chat_data[6] else None,
            )
            result.append(chat_to_dict(chat))

        return result

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def get_last_interaction(jid: str) -> dict[str, Any] | None:
    """Get most recent message involving the contact.

    Args:
        jid: The JID of the contact to search for

    Returns:
        Message dictionary or None if no messages found
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        aliases = _sender_aliases(jid)
        placeholders = ",".join("?" * len(aliases))
        cursor.execute(
            f"""
            SELECT
                m.timestamp,
                m.sender,
                c.name,
                m.content,
                m.is_from_me,
                c.jid,
                m.id,
                m.media_type
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.sender IN ({placeholders}) OR c.jid = ?
            ORDER BY m.timestamp DESC
            LIMIT 1
        """,
            (*aliases, jid),
        )

        msg_data = cursor.fetchone()

        if not msg_data:
            return None

        message = Message(
            timestamp=datetime.fromisoformat(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[7],
        )

        return msg_to_dict(message)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if "conn" in locals():
            conn.close()


def get_chat(chat_jid: str, include_last_message: bool = True) -> dict[str, Any] | None:
    """Get chat metadata by JID.

    Returns:
        Chat dictionary or None if not found
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        # See list_chats: the last message is always joined for is_from_me,
        # and the result tuple shape stays stable across the branch.
        if include_last_message:
            last_message_select = "m.content as last_message, m.sender as last_sender"
        else:
            last_message_select = "NULL as last_message, NULL as last_sender"

        query = f"""
            SELECT
                c.jid,
                c.name,
                c.last_message_time,
                {last_message_select},
                m.is_from_me as last_is_from_me,
                {_last_read_time_select(cursor, "c")}
            FROM chats c
            {_last_message_join("c", "m")}
            WHERE c.jid = ?
        """

        cursor.execute(query, (chat_jid,))
        chat_data = cursor.fetchone()

        if not chat_data:
            return None

        chat = Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5],
            last_read_time=datetime.fromisoformat(chat_data[6]) if chat_data[6] else None,
        )
        return chat_to_dict(chat)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if "conn" in locals():
            conn.close()


def get_direct_chat_by_contact(sender_phone_number: str) -> dict[str, Any] | None:
    """Get chat metadata by sender phone number."""
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            f"""
            SELECT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me,
                {_last_read_time_select(cursor, "c")}
            FROM chats c
            {_last_message_join("c", "m")}
            WHERE c.jid LIKE ? AND c.jid NOT LIKE '%@g.us'
            LIMIT 1
        """,
            (f"%{sender_phone_number}%",),
        )

        chat_data = cursor.fetchone()

        if not chat_data:
            return None

        chat = Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5],
            last_read_time=datetime.fromisoformat(chat_data[6]) if chat_data[6] else None,
        )
        return chat_to_dict(chat)

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if "conn" in locals():
            conn.close()


def send_message(
    recipient: str,
    message: str,
    quoted_message_id: str = "",
    quoted_sender_jid: str = "",
    quoted_content: str = "",
    mentions: list[str] | None = None,
) -> tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"

        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload: dict[str, Any] = {
            "recipient": recipient,
            "message": message,
        }
        if quoted_message_id:
            payload["quoted_message_id"] = quoted_message_id
            payload["quoted_sender_jid"] = quoted_sender_jid
            payload["quoted_content"] = quoted_content
        if mentions:
            payload["mentions"] = mentions

        response = requests.post(url, json=payload, headers=_bridge_headers())

        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"

    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def send_file(recipient: str, media_path: str, caption: str = "") -> tuple[bool, str]:
    """Send a media file (image, video, document) with an optional caption.

    The bridge populates the WA media-message Caption field from `message`, so
    passing both in one /api/send call produces a single attachment-with-caption
    message instead of two separate messages.
    """
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"

        if not media_path:
            return False, "Media path must be provided"

        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"

        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {"recipient": recipient, "media_path": media_path}
        if caption:
            payload["message"] = caption

        response = requests.post(url, json=payload, headers=_bridge_headers())

        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"

    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def send_audio_message(recipient: str, media_path: str) -> tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"

        if not media_path:
            return False, "Media path must be provided"

        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"

        converted_path: str | None = None
        if not media_path.endswith(".ogg"):
            try:
                media_path = audio.convert_to_opus_ogg_temp(media_path)
                converted_path = media_path
            except Exception as e:
                return False, f"Error converting file to opus ogg. You likely need to install ffmpeg: {str(e)}"

        try:
            url = f"{WHATSAPP_API_BASE_URL}/send"
            payload = {"recipient": recipient, "media_path": media_path}

            response = requests.post(url, json=payload, headers=_bridge_headers())

            # Check if the request was successful
            if response.status_code == 200:
                result = response.json()
                return result.get("success", False), result.get("message", "Unknown response")
            else:
                return False, f"Error: HTTP {response.status_code} - {response.text}"
        finally:
            # The converted file is a NamedTemporaryFile(delete=False); remove it
            # once the bridge has read it so every audio send doesn't leak an .ogg.
            if converted_path:
                try:
                    os.unlink(converted_path)
                except OSError:
                    pass

    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def send_reaction(
    recipient: str,
    message_id: str,
    emoji: str,
    from_me: bool = False,
    sender_jid: str = "",
) -> tuple[bool, str]:
    """Send (or remove) a reaction to a WhatsApp message.

    Args:
        recipient: The chat JID the message belongs to (phone JID or group JID).
        message_id: The ID of the message to react to.
        emoji: The reaction emoji. Pass an empty string to remove an existing reaction.
        from_me: Whether the original message was sent by the current user.
        sender_jid: JID of the original message sender (required for group messages
                    when from_me is False so the bridge can build the correct key).

    Returns:
        Tuple of (success, status_message).
    """
    try:
        if not recipient:
            return False, "Recipient must be provided"
        if not message_id:
            return False, "Message ID must be provided"

        url = f"{WHATSAPP_API_BASE_URL}/react"
        payload: dict[str, Any] = {
            "recipient": recipient,
            "message_id": message_id,
            "emoji": emoji,
            "from_me": from_me,
            "sender_jid": sender_jid,
        }

        response = requests.post(url, json=payload, headers=_bridge_headers())

        if response.status_code == 200:
            result = response.json()
            if result.get("ok"):
                return True, "Reaction sent"
            return False, result.get("error", "Unknown error")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"

    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def mark_messages_read(
    message_ids: list[str],
    chat_jid: str,
    sender_jid: str = "",
    timestamp: str | None = None,
) -> tuple[bool, str]:
    """Mark selected messages as read through the WhatsApp bridge."""
    try:
        normalized_ids = [message_id.strip() for message_id in message_ids]
        if not normalized_ids or any(not message_id for message_id in normalized_ids):
            return False, "At least one non-empty message ID must be provided"
        if not chat_jid:
            return False, "Chat JID must be provided"
        if chat_jid.endswith("@g.us") and not sender_jid:
            return False, "Sender JID must be provided for group read receipts"

        payload: dict[str, Any] = {
            "message_ids": normalized_ids,
            "chat_jid": chat_jid,
        }
        if sender_jid:
            payload["sender_jid"] = sender_jid
        if timestamp:
            payload["timestamp"] = timestamp

        response = requests.post(
            f"{WHATSAPP_API_BASE_URL}/mark-read",
            json=payload,
            headers=_bridge_headers(),
        )

        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        return False, f"Error: HTTP {response.status_code} - {response.text}"

    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def mark_chat_read(
    chat_jid: str,
    message_id: str | None = None,
) -> dict[str, Any]:
    """Mark a WhatsApp chat as read natively.

    Validates feature flag WHATSAPP_MARK_READ_ENABLED=true, checks local existence,
    and calls the WhatsApp bridge to dispatch read receipts and multi-device app state sync.

    Args:
        chat_jid: The target chat JID (e.g. '554899999999@s.whatsapp.net' or '120363...@g.us')
        message_id: Optional ID of a specific message to mark read through.

    Returns:
        Dictionary with execution result, timestamps, and unread state.
    """
    if not is_mark_read_enabled():
        return {
            "success": False,
            "status": "forbidden",
            "error": "WHATSAPP_MARK_READ_ENABLED=false",
        }

    chat_jid_clean = (chat_jid or "").strip()
    if not chat_jid_clean:
        return {
            "success": False,
            "status": "invalid_argument",
            "error": "chat_jid is required",
        }

    if "@" not in chat_jid_clean:
        chat_jid_clean = f"{chat_jid_clean}@s.whatsapp.net"

    chat = get_chat(chat_jid_clean, include_last_message=False)
    if not chat:
        return {
            "success": False,
            "status": "not_found",
            "error": f"Chat '{chat_jid_clean}' not found",
        }

    message_id_clean = (message_id or "").strip() or None
    if message_id_clean:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_jid FROM messages WHERE id = ?", (message_id_clean,))
            row = cursor.fetchone()
            if not row:
                return {
                    "success": False,
                    "status": "not_found",
                    "error": f"Message '{message_id_clean}' not found",
                }
            if row[0] != chat_jid_clean:
                return {
                    "success": False,
                    "status": "invalid_argument",
                    "error": f"Message '{message_id_clean}' does not belong to chat '{chat_jid_clean}'",
                }
        finally:
            conn.close()

    payload: dict[str, Any] = {"chat_jid": chat_jid_clean}
    if message_id_clean:
        payload["message_id"] = message_id_clean

    try:
        response = requests.post(
            f"{WHATSAPP_API_BASE_URL}/chats/mark-read",
            json=payload,
            headers=_bridge_headers(),
            timeout=15,
        )
        if response.status_code == 200:
            return response.json()

        try:
            err_data = response.json()
            if isinstance(err_data, dict) and "error" in err_data:
                return err_data
        except Exception:
            pass

        status_label = "forbidden" if response.status_code == 403 else "unavailable" if response.status_code == 503 else "failed"
        return {
            "success": False,
            "status": status_label,
            "error": f"Bridge error HTTP {response.status_code}: {response.text}",
        }
    except requests.RequestException as e:
        return {
            "success": False,
            "status": "unavailable",
            "error": f"WhatsApp bridge unreachable: {e}",
        }


def mark_chats_read(
    chat_jids: list[str],
) -> dict[str, Any]:
    """Mark multiple WhatsApp chats as read in batch (up to 50 chats).

    Args:
        chat_jids: List of chat JIDs to mark as read.

    Returns:
        Summary dictionary with overall success, total count, and individual results.
    """
    if not is_mark_read_enabled():
        return {
            "success": False,
            "status": "forbidden",
            "error": "WHATSAPP_MARK_READ_ENABLED=false",
        }

    if not chat_jids:
        return {
            "success": False,
            "status": "invalid_argument",
            "error": "chat_jids must not be empty",
        }

    if len(chat_jids) > 50:
        return {
            "success": False,
            "status": "invalid_argument",
            "error": "Maximum of 50 chats allowed per batch",
        }

    results = [mark_chat_read(jid) for jid in chat_jids]
    all_success = all(r.get("success", False) for r in results)
    return {
        "success": all_success,
        "total": len(chat_jids),
        "results": results,
    }


def download_media(message_id: str, chat_jid: str) -> str | None:
    """Download media from a message and return the local file path.

    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message

    Returns:
        The local file path if download was successful, None otherwise
    """
    try:
        url = f"{WHATSAPP_API_BASE_URL}/download"
        payload = {"message_id": message_id, "chat_jid": chat_jid}

        response = requests.post(url, json=payload, headers=_bridge_headers())

        if response.status_code == 200:
            result = response.json()
            if result.get("success", False):
                path = result.get("path")
                print(f"Media downloaded successfully: {path}")
                return path
            else:
                print(f"Download failed: {result.get('message', 'Unknown error')}")
                return None
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
            return None

    except requests.RequestException as e:
        print(f"Request error: {str(e)}")
        return None
    except json.JSONDecodeError:
        print(f"Error parsing response: {response.text}")
        return None
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return None


def parse_timeframe(timeframe: str | None) -> tuple[str | None, str | None]:
    """Parse natural or ISO timeframes into (after, before) ISO strings."""
    if not timeframe:
        return None, None
    tf = timeframe.strip().lower()
    now = datetime.now().astimezone()
    if tf == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start.isoformat(), None
    elif tf == "yesterday":
        start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return start.isoformat(), end.isoformat()
    elif tf in ("last_24_hours", "24h"):
        start = now - timedelta(hours=24)
        return start.isoformat(), None
    elif tf in ("last_3_days", "3d"):
        start = now - timedelta(days=3)
        return start.isoformat(), None
    elif tf == "this_week":
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        return start.isoformat(), None
    elif tf in ("last_7_days", "7d", "last_week"):
        start = now - timedelta(days=7)
        return start.isoformat(), None
    elif tf in ("last_30_days", "30d", "this_month"):
        start = now - timedelta(days=30)
        return start.isoformat(), None
    elif tf in ("last_6_months", "6m"):
        start = now - timedelta(days=180)
        return start.isoformat(), None
    elif tf in ("last_year", "1y"):
        start = now - timedelta(days=365)
        return start.isoformat(), None
    try:
        dt = datetime.fromisoformat(timeframe)
        return dt.isoformat(), None
    except ValueError:
        return None, None


def search_messages(
    query: str,
    chat_jid: str | None = None,
    sender_phone_number: str | None = None,
    after: str | None = None,
    before: str | None = None,
    timeframe: str | None = None,
    limit: int = 20,
    page: int = 0,
) -> list[dict[str, Any]]:
    """Search messages using SQLite FTS5 full-text index across conversations.

    Supports phrase search (\"termo exato\"), boolean operators (AND, OR, NOT), wildcards (prefix*),
    and falls back cleanly to substring search if special FTS syntax is malformed.
    """
    if timeframe:
        tf_after, tf_before = parse_timeframe(timeframe)
        after = after or tf_after
        before = before or tf_before

    conn = sqlite3.connect(MESSAGES_DB_PATH)
    cursor = conn.cursor()
    offset = page * limit

    fts_sql = """
        SELECT
            m.timestamp,
            m.sender,
            chats.name,
            m.content,
            m.is_from_me,
            chats.jid,
            m.id,
            m.media_type,
            m.quoted_message_id,
            m.filename,
            snippet(messages_fts, 0, '«', '»', '...', 15) AS match_snippet
        FROM messages_fts f
        JOIN messages m ON m.rowid = f.rowid
        JOIN chats ON m.chat_jid = chats.jid
        WHERE messages_fts MATCH ?
    """
    where_extra = []
    params: list[Any] = [query]

    if chat_jid:
        where_extra.append("m.chat_jid = ?")
        params.append(chat_jid)

    if sender_phone_number:
        aliases = _sender_aliases(sender_phone_number)
        placeholders = ",".join("?" * len(aliases))
        where_extra.append(f"m.sender IN ({placeholders})")
        params.extend(aliases)

    if after:
        where_extra.append("m.timestamp >= ?")
        params.append(after)

    if before:
        where_extra.append("m.timestamp <= ?")
        params.append(before)

    if where_extra:
        fts_sql += " AND " + " AND ".join(where_extra)

    fts_sql += " ORDER BY m.timestamp DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    try:
        cursor.execute(fts_sql, tuple(params))
        rows = cursor.fetchall()
    except sqlite3.OperationalError:
        # Fallback to substring matching if FTS syntax is invalid
        fallback_sql = """
            SELECT
                m.timestamp,
                m.sender,
                chats.name,
                m.content,
                m.is_from_me,
                chats.jid,
                m.id,
                m.media_type,
                m.quoted_message_id,
                m.filename,
                m.content AS match_snippet
            FROM messages m
            JOIN chats ON m.chat_jid = chats.jid
            WHERE (instr(LOWER(m.content), LOWER(?)) > 0 OR instr(m.content, ?) > 0)
        """
        fallback_params: list[Any] = [query, query]
        fb_where = []
        if chat_jid:
            fb_where.append("m.chat_jid = ?")
            fallback_params.append(chat_jid)
        if sender_phone_number:
            aliases = _sender_aliases(sender_phone_number)
            placeholders = ",".join("?" * len(aliases))
            fb_where.append(f"m.sender IN ({placeholders})")
            fallback_params.extend(aliases)
        if after:
            fb_where.append("m.timestamp >= ?")
            fallback_params.append(after)
        if before:
            fb_where.append("m.timestamp <= ?")
            fallback_params.append(before)
        if fb_where:
            fallback_sql += " AND " + " AND ".join(fb_where)
        fallback_sql += " ORDER BY m.timestamp DESC LIMIT ? OFFSET ?"
        fallback_params.extend([limit, offset])

        cursor.execute(fallback_sql, tuple(fallback_params))
        rows = cursor.fetchall()

    results = []
    for r in rows:
        results.append(
            {
                "id": r[6],
                "timestamp": r[0],
                "chat_jid": r[5],
                "chat_name": r[2] or r[5],
                "sender": r[1],
                "sender_name": get_sender_name(r[1]) if r[1] else None,
                "is_from_me": bool(r[4]),
                "content": r[3],
                "match_snippet": r[10],
                "media_type": r[7] if r[7] else None,
                "quoted_message_id": r[8] if r[8] else None,
            }
        )
    return results


def catch_up(
    timeframe: str = "today",
    only_groups: bool = False,
    limit_chats: int = 10,
) -> dict[str, Any]:
    """Generate an activity digest and catch-up summary of recent WhatsApp activity.

    Includes message counts, most active chats with previews, questions directed at the user,
    and media summaries.
    """
    after, before = parse_timeframe(timeframe)
    if not after:
        after, _ = parse_timeframe("today")

    conn = sqlite3.connect(MESSAGES_DB_PATH)
    cursor = conn.cursor()

    where_clauses = ["m.timestamp >= ?"]
    params: list[Any] = [after]
    if before:
        where_clauses.append("m.timestamp <= ?")
        params.append(before)
    if only_groups:
        where_clauses.append("c.jid LIKE '%@g.us'")

    where_str = " AND ".join(where_clauses)

    cursor.execute(
        f"SELECT COUNT(*) FROM messages m JOIN chats c ON m.chat_jid = c.jid WHERE {where_str}", tuple(params)
    )
    total_messages = cursor.fetchone()[0]

    active_chats_sql = f"""
        SELECT
            c.jid,
            c.name,
            COUNT(m.id) AS msg_count,
            MAX(m.timestamp) AS last_activity
        FROM messages m
        JOIN chats c ON m.chat_jid = c.jid
        WHERE {where_str}
        GROUP BY c.jid
        ORDER BY msg_count DESC
        LIMIT ?
    """
    cursor.execute(active_chats_sql, tuple(params + [limit_chats]))
    active_chats_rows = cursor.fetchall()

    active_chats = []
    for row in active_chats_rows:
        chat_jid, chat_name, count, last_act = row
        cursor.execute(
            """
            SELECT sender, content, timestamp, is_from_me, media_type
            FROM messages
            WHERE chat_jid = ? AND timestamp >= ?
            ORDER BY timestamp DESC
            LIMIT 3
            """,
            (chat_jid, after),
        )
        recent = [
            {
                "sender": r[0],
                "sender_name": get_sender_name(r[0]) if r[0] else None,
                "content": r[1],
                "timestamp": r[2],
                "is_from_me": bool(r[3]),
                "media_type": r[4] or None,
            }
            for r in cursor.fetchall()
        ]
        active_chats.append(
            {
                "jid": chat_jid,
                "name": chat_name or chat_jid,
                "is_group": chat_jid.endswith("@g.us"),
                "message_count": count,
                "last_activity": last_act,
                "recent_messages": recent,
            }
        )

    q_params = list(params)
    cursor.execute(
        f"""
        SELECT m.timestamp, m.sender, c.name, m.content, c.jid, m.id
        FROM messages m
        JOIN chats c ON m.chat_jid = c.jid
        WHERE {where_str}
          AND m.is_from_me = 0
          AND (m.content LIKE '%?' OR m.content LIKE '%? %')
        ORDER BY m.timestamp DESC
        LIMIT 10
        """,
        tuple(q_params),
    )
    questions = [
        {
            "id": r[5],
            "timestamp": r[0],
            "sender": r[1],
            "sender_name": get_sender_name(r[1]) if r[1] else None,
            "chat_name": r[2] or r[4],
            "chat_jid": r[4],
            "question": r[3],
        }
        for r in cursor.fetchall()
    ]

    cursor.execute(
        f"""
        SELECT media_type, COUNT(*)
        FROM messages m
        JOIN chats c ON m.chat_jid = c.jid
        WHERE {where_str} AND m.media_type != ''
        GROUP BY media_type
        """,
        tuple(params),
    )
    media_summary = {r[0]: r[1] for r in cursor.fetchall()}

    summary_text = f"Activity in '{timeframe}': {total_messages} messages across {len(active_chats)} top chats."
    if questions:
        summary_text += f" Found {len(questions)} questions directed at you."
    if media_summary:
        media_parts = [f"{count} {mtype}(s)" for mtype, count in media_summary.items()]
        summary_text += f" Media received: {', '.join(media_parts)}."

    return {
        "timeframe": timeframe,
        "after": after,
        "before": before,
        "total_messages": total_messages,
        "summary": summary_text,
        "active_chats": active_chats,
        "questions_for_you": questions,
        "media_summary": media_summary,
    }


def list_unread_chats(only_groups: bool = False, limit: int = 30) -> list[dict[str, Any]]:
    """List chats that have unread incoming messages."""
    conn = sqlite3.connect(MESSAGES_DB_PATH)
    cursor = conn.cursor()

    group_clause = "AND c.jid LIKE '%@g.us'" if only_groups else ""
    query = f"""
        SELECT
            c.jid,
            c.name,
            COUNT(m.id) AS unread_count,
            MAX(m.timestamp) AS last_message_time,
            c.last_read_time
        FROM messages m
        JOIN chats c ON m.chat_jid = c.jid
        WHERE m.is_from_me = 0
          AND (c.last_read_time IS NULL OR m.timestamp > c.last_read_time)
          {group_clause}
        GROUP BY c.jid
        ORDER BY unread_count DESC
        LIMIT ?
    """
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()

    results = []
    for r in rows:
        chat_jid, chat_name, unread_count, last_time, last_read = r
        cursor.execute(
            """
            SELECT sender, content, timestamp, media_type
            FROM messages
            WHERE chat_jid = ? AND is_from_me = 0
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (chat_jid,),
        )
        last_msg = cursor.fetchone()
        last_preview = None
        if last_msg:
            last_preview = {
                "sender": last_msg[0],
                "sender_name": get_sender_name(last_msg[0]) if last_msg[0] else None,
                "content": last_msg[1],
                "timestamp": last_msg[2],
                "media_type": last_msg[3] or None,
            }

        results.append(
            {
                "jid": chat_jid,
                "name": chat_name or chat_jid,
                "is_group": chat_jid.endswith("@g.us"),
                "unread_count": unread_count,
                "last_message_time": last_time,
                "last_read_time": last_read,
                "latest_incoming_message": last_preview,
            }
        )
    return results


def extract_action_items(
    chat_jid: str | None = None,
    timeframe: str = "last_7_days",
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Extract action items, asks, commitments, and deadlines from recent conversations."""
    after, before = parse_timeframe(timeframe)
    if not after:
        after, _ = parse_timeframe("last_7_days")

    conn = sqlite3.connect(MESSAGES_DB_PATH)
    cursor = conn.cursor()

    where_clauses = ["m.timestamp >= ?"]
    params: list[Any] = [after]
    if before:
        where_clauses.append("m.timestamp <= ?")
        params.append(before)
    if chat_jid:
        where_clauses.append("m.chat_jid = ?")
        params.append(chat_jid)

    keywords = [
        "preciso",
        "favor",
        "pode enviar",
        "pode fazer",
        "pode me",
        "me envia",
        "me passa",
        "combinado",
        "reunião",
        "reuniao",
        "até amanhã",
        "ate amanha",
        "segunda",
        "sexta",
        "assinar",
        "enviar",
        "aprovar",
        "template",
        "link",
        "aguardo",
        "urgente",
        "deadline",
        "please",
        "can you",
        "let's schedule",
        "todo",
        "action item",
    ]
    like_clauses = " OR ".join(["LOWER(m.content) LIKE ?" for _ in keywords])
    where_clauses.append(f"({like_clauses} OR m.content LIKE '%?')")
    params.extend([f"%{kw}%" for kw in keywords])

    where_str = " AND ".join(where_clauses)
    sql = f"""
        SELECT
            m.id,
            m.timestamp,
            m.sender,
            c.name,
            c.jid,
            m.content,
            m.is_from_me
        FROM messages m
        JOIN chats c ON m.chat_jid = c.jid
        WHERE {where_str}
        ORDER BY m.timestamp DESC
        LIMIT ?
    """
    params.append(limit)
    cursor.execute(sql, tuple(params))
    rows = cursor.fetchall()

    action_items = []
    for r in rows:
        msg_id, ts, sender, c_name, c_jid, content, is_from_me = r
        item_type = "question" if content.strip().endswith("?") else "action_or_commitment"
        action_items.append(
            {
                "message_id": msg_id,
                "timestamp": ts,
                "chat_jid": c_jid,
                "chat_name": c_name or c_jid,
                "sender": sender,
                "sender_name": get_sender_name(sender) if sender else None,
                "is_from_me": bool(is_from_me),
                "type": item_type,
                "content": content,
            }
        )
    return action_items


def list_chat_lists() -> list[dict[str, Any]]:
    """List all WhatsApp labels and custom chat lists with their chat counts.

    Returns:
        List of chat lists, each with id, name, type, source, and chat_count.
    """
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        _ensure_chat_list_schema(conn)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                l.id,
                l.name,
                l.type,
                l.source,
                COUNT(i.chat_jid) as chat_count
            FROM chat_lists l
            LEFT JOIN chat_list_items i ON l.id = i.list_id
            WHERE l.deleted = 0
            GROUP BY l.id, l.name, l.type, l.source
            ORDER BY CASE WHEN l.source = 'whatsapp' THEN 0 ELSE 1 END, l.name COLLATE NOCASE ASC
            """
        )
        rows = cursor.fetchall()
        seen_names = set()
        result = []
        for r in rows:
            name_lower = (r[1] or "").lower()
            if name_lower in seen_names:
                continue
            seen_names.add(name_lower)
            result.append(
                {
                    "id": r[0],
                    "name": r[1],
                    "type": r[2] or "CUSTOM",
                    "source": r[3] or "whatsapp",
                    "chat_count": r[4],
                }
            )
        result.sort(key=lambda x: x["name"].lower())
        return result
    except sqlite3.Error as e:
        print(f"Database error in list_chat_lists: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def list_chats_by_list(
    list_name: str | None = None,
    list_id: str | None = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active",
) -> list[dict[str, Any]]:
    """Get chats belonging to a specific WhatsApp label or custom list.

    Args:
        list_name: Name of the chat list/label (e.g. "Para responder")
        list_id: ID of the chat list/label
        limit: Maximum number of chats to return (default: 20)
        page: Page number for pagination (0-based)
        include_last_message: Whether to fetch content of the last message
        sort_by: "last_active" (default) or "name"

    Returns:
        List of chat dictionaries with the same structure as list_chats.
    """
    if not list_name and not list_id:
        raise ValueError("Either list_name or list_id must be provided")

    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        _ensure_chat_list_schema(conn)
        cursor = conn.cursor()

        # Resolve list ID
        target_list_id = list_id
        if not target_list_id and list_name:
            cursor.execute(
                """
                SELECT id FROM chat_lists 
                WHERE LOWER(name) = LOWER(?) AND deleted = 0 
                ORDER BY CASE WHEN source = 'whatsapp' THEN 0 ELSE 1 END
                LIMIT 1
                """,
                (list_name.strip(),),
            )
            row = cursor.fetchone()
            if not row:
                # Try partial match if exact match fails
                cursor.execute(
                    """
                    SELECT id FROM chat_lists 
                    WHERE instr(LOWER(name), LOWER(?)) > 0 AND deleted = 0 
                    ORDER BY CASE WHEN source = 'whatsapp' THEN 0 ELSE 1 END
                    LIMIT 1
                    """,
                    (list_name.strip(),),
                )
                row = cursor.fetchone()
            if row:
                target_list_id = row[0]
            else:
                return []

        if include_last_message:
            last_message_select = "messages.content as last_message, messages.sender as last_sender"
        else:
            last_message_select = "NULL as last_message, NULL as last_sender"

        query_parts = [
            f"""
            SELECT
                chats.jid,
                chats.name,
                chats.last_message_time,
                {last_message_select},
                messages.is_from_me as last_is_from_me,
                {_last_read_time_select(cursor, "chats")}
            FROM chats
            JOIN chat_list_items cli ON chats.jid = cli.chat_jid
            {_last_message_join("chats", "messages")}
            WHERE cli.list_id = ?
            """
        ]

        order_by = "chats.last_message_time DESC" if sort_by == "last_active" else "chats.name"
        query_parts.append(f"ORDER BY {order_by}")

        offset = page * limit
        query_parts.append("LIMIT ? OFFSET ?")
        params = [target_list_id, limit, offset]

        cursor.execute(" ".join(query_parts), tuple(params))
        chats = cursor.fetchall()

        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5],
                last_read_time=datetime.fromisoformat(chat_data[6]) if chat_data[6] else None,
            )
            result.append(chat_to_dict(chat))

        return result
    except sqlite3.Error as e:
        print(f"Database error in list_chats_by_list: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def get_chat_lists(chat_jid: str) -> list[dict[str, Any]]:
    """Get all lists/labels to which a chat belongs.

    Args:
        chat_jid: WhatsApp JID of the chat

    Returns:
        List of lists/labels the chat is part of.
    """
    if not chat_jid:
        return []

    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        _ensure_chat_list_schema(conn)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                l.id,
                l.name,
                l.type,
                l.source
            FROM chat_lists l
            JOIN chat_list_items cli ON l.id = cli.list_id
            WHERE cli.chat_jid = ? AND l.deleted = 0
            ORDER BY CASE WHEN l.source = 'whatsapp' THEN 0 ELSE 1 END, l.name COLLATE NOCASE ASC
            """,
            (chat_jid.strip(),),
        )
        rows = cursor.fetchall()
        result = []
        for r in rows:
            result.append(
                {
                    "id": r[0],
                    "name": r[1],
                    "type": r[2] or "CUSTOM",
                    "source": r[3] or "whatsapp",
                }
            )
        return result
    except sqlite3.Error as e:
        print(f"Database error in get_chat_lists: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def create_chat_list(name: str) -> dict[str, Any]:
    """Create a local chat list / tag.

    Args:
        name: Name of the list (e.g., "Para responder")

    Returns:
        Dictionary with list id, name, and creation status.
    """
    trimmed = name.strip()
    if not trimmed:
        raise ValueError("List name cannot be empty")

    now = datetime.now(UTC).isoformat()
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        _ensure_chat_list_schema(conn)
        cursor = conn.cursor()

        # Check if list with same name already exists
        cursor.execute(
            "SELECT id, name, deleted FROM chat_lists WHERE LOWER(name) = LOWER(?) LIMIT 1",
            (trimmed,),
        )
        existing = cursor.fetchone()
        if existing:
            list_id, ex_name, deleted = existing
            if deleted:
                cursor.execute(
                    "UPDATE chat_lists SET deleted = 0, updated_at = ? WHERE id = ?",
                    (now, list_id),
                )
                conn.commit()
                return {
                    "id": list_id,
                    "name": ex_name,
                    "source": "local",
                    "created": True,
                    "message": "Reactivated deleted list",
                }
            return {
                "id": list_id,
                "name": ex_name,
                "source": "local",
                "created": False,
                "message": "List already exists",
            }

        list_id = str(uuid.uuid4())
        cursor.execute(
            """
            INSERT INTO chat_lists (id, name, color, type, source, deleted, created_at, updated_at)
            VALUES (?, ?, 0, 'CUSTOM', 'local', 0, ?, ?)
            """,
            (list_id, trimmed, now, now),
        )
        conn.commit()
        return {"id": list_id, "name": trimmed, "source": "local", "created": True}
    except sqlite3.Error as e:
        print(f"Database error in create_chat_list: {e}")
        raise
    finally:
        if "conn" in locals():
            conn.close()


def add_chat_to_list(list_name: str, chat_jid: str) -> dict[str, Any]:
    """Add a chat to a list/label.

    Args:
        list_name: Name or ID of the list
        chat_jid: WhatsApp JID of the chat

    Returns:
        Status dictionary
    """
    if not list_name or not list_name.strip():
        raise ValueError("list_name must be provided")
    if not chat_jid or not chat_jid.strip():
        raise ValueError("chat_jid must be provided")

    now = datetime.now(UTC).isoformat()
    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        _ensure_chat_list_schema(conn)
        cursor = conn.cursor()

        # Resolve list by name or ID
        cursor.execute(
            "SELECT id, name FROM chat_lists WHERE (LOWER(name) = LOWER(?) OR id = ?) AND deleted = 0 LIMIT 1",
            (list_name.strip(), list_name.strip()),
        )
        row = cursor.fetchone()
        if not row:
            created = create_chat_list(list_name.strip())
            list_id = created["id"]
            resolved_name = list_name.strip()
        else:
            list_id, resolved_name = row

        cursor.execute(
            """
            INSERT OR IGNORE INTO chat_list_items (list_id, chat_jid, created_at)
            VALUES (?, ?, ?)
            """,
            (list_id, chat_jid.strip(), now),
        )
        conn.commit()
        return {
            "success": True,
            "list_id": list_id,
            "list_name": resolved_name,
            "chat_jid": chat_jid.strip(),
            "message": f"Added chat {chat_jid.strip()} to list '{resolved_name}'",
        }
    except sqlite3.Error as e:
        print(f"Database error in add_chat_to_list: {e}")
        raise
    finally:
        if "conn" in locals():
            conn.close()


def remove_chat_from_list(list_name: str, chat_jid: str) -> dict[str, Any]:
    """Remove a chat from a list/label.

    Args:
        list_name: Name or ID of the list
        chat_jid: WhatsApp JID of the chat

    Returns:
        Status dictionary
    """
    if not list_name or not list_name.strip():
        raise ValueError("list_name must be provided")
    if not chat_jid or not chat_jid.strip():
        raise ValueError("chat_jid must be provided")

    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        _ensure_chat_list_schema(conn)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id, name FROM chat_lists WHERE (LOWER(name) = LOWER(?) OR id = ?) AND deleted = 0 LIMIT 1",
            (list_name.strip(), list_name.strip()),
        )
        row = cursor.fetchone()
        if not row:
            return {"success": False, "message": f"List '{list_name}' not found"}

        list_id, resolved_name = row
        cursor.execute(
            "DELETE FROM chat_list_items WHERE list_id = ? AND chat_jid = ?",
            (list_id, chat_jid.strip()),
        )
        conn.commit()
        return {
            "success": True,
            "list_id": list_id,
            "list_name": resolved_name,
            "chat_jid": chat_jid.strip(),
            "message": f"Removed chat {chat_jid.strip()} from list '{resolved_name}'",
        }
    except sqlite3.Error as e:
        print(f"Database error in remove_chat_from_list: {e}")
        raise
    finally:
        if "conn" in locals():
            conn.close()


def prepare_send_message(
    chat_jid: str,
    text: str,
    reply_to_message_id: str | None = None,
) -> dict[str, Any]:
    """Prepare a message draft for two-phase explicit confirmation sending.

    This function NEVER sends anything to WhatsApp. It registers a pending send draft
    and generates a single-use authorization code that must be explicitly confirmed
    via `commit_send_message`.

    Args:
        chat_jid: Target chat JID or phone number (e.g. "12025551234@s.whatsapp.net" or group JID)
        text: Exact message text to send
        reply_to_message_id: Optional ID of the message to reply to

    Returns:
        Dictionary containing send_id, chat_jid, recipient_name, text, text_sha256,
        expires_at, and authorization_code.
    """
    if not chat_jid or not chat_jid.strip():
        raise ValueError("chat_jid is required")
    if not text or not text.strip():
        raise ValueError("text is required")

    chat_jid_clean = chat_jid.strip()
    if "@" not in chat_jid_clean:
        chat_jid_clean = f"{chat_jid_clean}@s.whatsapp.net"

    text_clean = text
    text_hash = hashlib.sha256(text_clean.encode("utf-8")).hexdigest()

    # Generate 4-character random code suffix (excluding ambiguous chars 0, O, 1, I)
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    code_suffix = "".join(random.choices(alphabet, k=4))
    authorization_code = f"ENVIAR {code_suffix}"
    code_hash = hashlib.sha256(authorization_code.encode("utf-8")).hexdigest()

    send_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    expires_at = now + timedelta(minutes=10)

    # Resolve recipient name
    recipient_name = get_sender_name(chat_jid_clean)

    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        _ensure_chat_list_schema(conn)
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO send_message_audit (
                send_id, chat_jid, recipient_name, text, text_sha256,
                reply_to_message_id, authorization_code_hash, status,
                prepared_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                send_id,
                chat_jid_clean,
                recipient_name,
                text_clean,
                text_hash,
                reply_to_message_id,
                code_hash,
                now.isoformat(),
                expires_at.isoformat(),
            ),
        )
        conn.commit()

        result = {
            "send_id": send_id,
            "chat_jid": chat_jid_clean,
            "recipient_name": recipient_name,
            "text": text_clean,
            "text_sha256": text_hash,
            "expires_at": expires_at.isoformat(),
            "authorization_code": authorization_code,
        }
        if not is_write_enabled():
            result["warning"] = (
                "Server write operations are currently disabled (WHATSAPP_WRITE_ENABLED=false). "
                "commit_send_message will fail until write is enabled in configuration."
            )
        return result
    finally:
        if "conn" in locals():
            conn.close()


def commit_send_message(
    send_id: str,
    authorization_code: str,
) -> dict[str, Any]:
    """Execute message delivery after explicit human authorization code verification.

    This is the ONLY function authorized to send messages to WhatsApp.
    Validates that:
    1. Server write operations are enabled (WHATSAPP_WRITE_ENABLED=true).
    2. The draft send_id exists and is in 'pending' status.
    3. The draft has not expired (10-minute window).
    4. The authorization code matches exactly.
    5. The draft text content and SHA-256 hash are intact.

    Args:
        send_id: Identifier generated by prepare_send_message
        authorization_code: Confirmation code (e.g. "ENVIAR K7M4")

    Returns:
        Delivery result dictionary with status and details.
    """
    if not is_write_enabled():
        return {
            "success": False,
            "status": "forbidden",
            "error": (
                "Write operations are disabled on this WhatsApp MCP server (WHATSAPP_WRITE_ENABLED=false). "
                "To enable message sending, set WHATSAPP_WRITE_ENABLED=true in the server configuration."
            ),
        }

    if not send_id or not send_id.strip():
        raise ValueError("send_id is required")
    if not authorization_code or not authorization_code.strip():
        raise ValueError("authorization_code is required")

    send_id_clean = send_id.strip()
    code_input = authorization_code.strip().upper()
    code_hash = hashlib.sha256(code_input.encode("utf-8")).hexdigest()
    now = datetime.now(UTC)

    try:
        conn = sqlite3.connect(MESSAGES_DB_PATH)
        _ensure_chat_list_schema(conn)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                chat_jid, recipient_name, text, text_sha256,
                reply_to_message_id, authorization_code_hash,
                status, expires_at
            FROM send_message_audit
            WHERE send_id = ?
            """,
            (send_id_clean,),
        )
        row = cursor.fetchone()
        if not row:
            return {
                "success": False,
                "status": "failed",
                "error": f"Draft send_id '{send_id_clean}' not found.",
            }

        (
            chat_jid,
            recipient_name,
            text,
            stored_text_hash,
            reply_to_id,
            stored_code_hash,
            status,
            expires_at_str,
        ) = row

        if status != "pending":
            return {
                "success": False,
                "status": "failed",
                "error": f"Draft '{send_id_clean}' cannot be used because it is already '{status}'.",
            }

        expires_at = datetime.fromisoformat(expires_at_str)
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)

        if now > expires_at:
            cursor.execute(
                "UPDATE send_message_audit SET status = 'expired' WHERE send_id = ?",
                (send_id_clean,),
            )
            conn.commit()
            return {
                "success": False,
                "status": "expired",
                "error": f"Authorization code for send_id '{send_id_clean}' expired at {expires_at_str}.",
            }

        if code_hash != stored_code_hash:
            return {
                "success": False,
                "status": "unauthorized",
                "error": "Invalid authorization code. Authorization codes are case-insensitive and formatted as 'ENVIAR XXXX'.",
            }

        # Check content integrity
        current_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if current_hash != stored_text_hash:
            cursor.execute(
                "UPDATE send_message_audit SET status = 'tampered' WHERE send_id = ?",
                (send_id_clean,),
            )
            conn.commit()
            return {
                "success": False,
                "status": "failed",
                "error": "Message integrity check failed (SHA-256 mismatch). Send cancelled.",
            }

        # Dispatch through WhatsApp bridge REST API
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": chat_jid,
            "message": text,
        }
        if reply_to_id:
            payload["quoted_message_id"] = reply_to_id

        try:
            resp = requests.post(
                url,
                json=payload,
                headers=_bridge_headers(),
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                wa_msg_id = data.get("message", "sent")
                cursor.execute(
                    """
                    UPDATE send_message_audit
                    SET status = 'sent', authorized_at = ?, sent_at = ?, whatsapp_message_id = ?
                    WHERE send_id = ?
                    """,
                    (now.isoformat(), now.isoformat(), wa_msg_id, send_id_clean),
                )
                conn.commit()
                return {
                    "success": True,
                    "status": "sent",
                    "send_id": send_id_clean,
                    "chat_jid": chat_jid,
                    "recipient_name": recipient_name,
                    "whatsapp_message_id": wa_msg_id,
                    "sent_at": now.isoformat(),
                }
            else:
                err_text = resp.text
                cursor.execute(
                    """
                    UPDATE send_message_audit
                    SET status = 'failed', authorized_at = ?, error_message = ?
                    WHERE send_id = ?
                    """,
                    (now.isoformat(), f"HTTP {resp.status_code}: {err_text}", send_id_clean),
                )
                conn.commit()
                return {
                    "success": False,
                    "status": "failed",
                    "send_id": send_id_clean,
                    "error": f"Bridge error HTTP {resp.status_code}: {err_text}",
                }
        except Exception as exc:
            cursor.execute(
                """
                UPDATE send_message_audit
                SET status = 'failed', authorized_at = ?, error_message = ?
                WHERE send_id = ?
                """,
                (now.isoformat(), str(exc), send_id_clean),
            )
            conn.commit()
            return {
                "success": False,
                "status": "failed",
                "send_id": send_id_clean,
                "error": f"Bridge communication error: {exc}",
            }
    finally:
        if "conn" in locals():
            conn.close()
