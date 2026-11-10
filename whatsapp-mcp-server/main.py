import asyncio
import os
import signal
import sys
import tempfile
import threading
from typing import Annotated, Any

from mcp.server.fastmcp import FastMCP, Image
from mcp.server.lowlevel.server import NotificationOptions
from pydantic import Field

import media_preview
import transcription
from mcp_config import resolve_host, resolve_port, resolve_transport
from parent_watchdog import install_stdio_parent_watchdog
from whatsapp import (
    MESSAGES_DB_PATH,
    msg_to_dict,
    reconcile_database,
)
from whatsapp import (
    add_chat_to_list as whatsapp_add_chat_to_list,
)
from whatsapp import (
    catch_up as whatsapp_catch_up,
)
from whatsapp import (
    commit_send_message as whatsapp_commit_send_message,
)
from whatsapp import (
    create_chat_list as whatsapp_create_chat_list,
)
from whatsapp import (
    download_media as whatsapp_download_media,
)
from whatsapp import (
    extract_action_items as whatsapp_extract_action_items,
)
from whatsapp import (
    get_chat as whatsapp_get_chat,
)
from whatsapp import (
    get_chat_lists as whatsapp_get_chat_lists,
)
from whatsapp import (
    get_contact_chats as whatsapp_get_contact_chats,
)
from whatsapp import (
    get_direct_chat_by_contact as whatsapp_get_direct_chat_by_contact,
)
from whatsapp import (
    get_last_interaction as whatsapp_get_last_interaction,
)
from whatsapp import (
    get_message_context as whatsapp_get_message_context,
)
from whatsapp import (
    get_read_queue_status as whatsapp_get_read_queue_status,
)
from whatsapp import (
    get_send_status as whatsapp_get_send_status,
)
from whatsapp import (
    get_sender_name as whatsapp_get_sender_name,
)
from whatsapp import (
    get_sync_status as whatsapp_get_sync_status,
)
from whatsapp import (
    process_pending_read_marks as whatsapp_process_pending_read_marks,
)
from whatsapp import (
    list_chat_lists as whatsapp_list_chat_lists,
)
from whatsapp import (
    list_chats as whatsapp_list_chats,
)
from whatsapp import (
    list_chats_by_list as whatsapp_list_chats_by_list,
)
from whatsapp import (
    list_messages as whatsapp_list_messages,
)
from whatsapp import (
    list_unread_chats as whatsapp_list_unread_chats,
)
from whatsapp import (
    mark_chat_read as whatsapp_mark_chat_read,
)
from whatsapp import (
    mark_chats_read as whatsapp_mark_chats_read,
)
from whatsapp import (
    prepare_send_message as whatsapp_prepare_send_message,
)
from whatsapp import (
    remove_chat_from_list as whatsapp_remove_chat_from_list,
)
from whatsapp import (
    search_contacts as whatsapp_search_contacts,
)
from whatsapp import (
    search_messages as whatsapp_search_messages,
)

SERVER_INSTRUCTIONS = (
    "WhatsApp MCP Server Guidelines:\n\n"
    "SAFE TWO-PHASE MESSAGE SENDING:\n"
    "To send a WhatsApp message, you MUST strictly follow this two-phase workflow:\n"
    "1. Call prepare_send_message(chat_jid, text) to create a draft.\n"
    "   - It returns a draft containing send_id and an authorization_code (e.g. 'ENVIAR K7M4').\n"
    "2. Present the recipient, message text, and authorization code to the user, asking for confirmation.\n"
    "3. When the user responds with the authorization code:\n"
    "   - Call commit_send_message(authorization_code=code) immediately.\n"
    "   - CRITICAL: NEVER call prepare_send_message again just because you lost or forgot send_id!\n"
    "   - The server looks up and confirms the draft directly by authorization_code.\n"
    "   - Calling prepare_send_message again creates a new draft with a new code and breaks the confirmation flow.\n"
    "4. You can use get_send_status(authorization_code=code) at any time to inspect draft delivery status without sending."
)

# Initialize FastMCP server. Env-var handling is deferred to the __main__ block
# so importing this module never parses env vars or exits the process.
mcp = FastMCP("whatsapp", instructions=SERVER_INSTRUCTIONS)


# Advertise tools listChanged capability so clients (ChatGPT, Claude, etc.) know tools can be updated
_orig_create_initialization_options = mcp._mcp_server.create_initialization_options


def _custom_create_initialization_options(
    notification_options: NotificationOptions | None = None,
    experimental_capabilities: dict[str, dict[str, Any]] | None = None,
) -> Any:
    opts = notification_options or NotificationOptions()
    opts.tools_changed = True
    return _orig_create_initialization_options(
        notification_options=opts,
        experimental_capabilities=experimental_capabilities,
    )


mcp._mcp_server.create_initialization_options = _custom_create_initialization_options

# Enforce strict read-only mode by default: no write tools registered
READ_ONLY = os.getenv("WHATSAPP_READ_ONLY", "true").lower() in ("true", "1", "yes")


def read_only_tool():
    """Decorator to only register a tool if not in read-only mode."""

    def decorator(fn):
        if not READ_ONLY:
            return mcp.tool()(fn)
        return fn

    return decorator


PreviewDimension = Annotated[
    int,
    Field(
        strict=True,
        ge=media_preview.MIN_MAX_DIMENSION,
        le=media_preview.MAX_MAX_DIMENSION,
        description="Longest preview edge in pixels; must be an integer from 1 to 2048.",
    ),
]


@mcp.tool()
def search_contacts(query: str) -> list[dict[str, Any]]:
    """Search WhatsApp contacts by name or phone number.

    Args:
        query: Search term to match against contact names or phone numbers
    """
    contacts = whatsapp_search_contacts(query)
    return contacts


@mcp.tool()
def get_contact(
    identifier: str | None = None,
    phone_number: str | None = None,
    phone: str | None = None,
) -> dict[str, Any]:
    """Look up a WhatsApp contact by phone number, LID, or full JID.

    Automatically detects the identifier type and queries appropriately.

    Args:
        identifier: Phone number, LID, or full JID. Examples:
                    - "12025551234" (phone number)
                    - "35047067385985" (LID - numeric)
                    - "12025551234@s.whatsapp.net" (phone JID)
                    - "184125298348272@lid" (LID JID)
        phone_number: Backward-compatible alias for `identifier`.
        phone: Backward-compatible alias for `identifier` (matches README parameter name).

    Returns:
        Dictionary with jid, name, display_name, is_lid, and resolved status
    """
    if identifier is None:
        identifier = phone_number
    if identifier is None:
        identifier = phone
    if identifier is None:
        raise ValueError("Missing required argument: identifier (or phone_number / phone)")

    identifier = identifier.strip()
    if not identifier:
        raise ValueError("identifier must be non-empty")

    # Detect identifier type and normalize to JID.
    bare_numeric_digits: str | None = None
    if "@" in identifier:
        # Already a JID - use as-is
        jid = identifier
        is_lid = jid.endswith("@lid") or jid.split("@", 1)[-1] == "lid"
    else:
        digits = "".join(c for c in identifier if c.isdigit())
        if digits:
            # LIDs can overlap phone-number lengths, so bare numeric inputs try phone first.
            jid = f"{digits}@s.whatsapp.net"
            is_lid = False
            if identifier.isdigit():
                bare_numeric_digits = digits
        else:
            # Non-numeric and not a JID; try as-is.
            jid = identifier
            is_lid = False

    jid_user = jid.split("@", 1)[0]

    display_name: str | None = None
    resolved = False

    # Prefer chats table lookup via get_chat (works for both phone and LID contacts).
    candidates: list[tuple[str, bool]] = [(jid, is_lid)]
    if bare_numeric_digits:
        candidates.append((f"{bare_numeric_digits}@lid", True))

    chat = None
    for candidate_jid, candidate_is_lid in candidates:
        chat = whatsapp_get_chat(candidate_jid, include_last_message=False)
        if chat:
            jid = candidate_jid
            is_lid = candidate_is_lid
            jid_user = jid.split("@", 1)[0]
            break

    if chat and chat.get("name"):
        display_name = chat["name"]
        resolved = display_name not in (jid, jid_user)
    else:
        # Fallback: best-effort sender-name resolution (may use fuzzy LIKE lookup).
        display_name = whatsapp_get_sender_name(jid)
        resolved = display_name not in (jid, jid_user, identifier)

    return {
        "identifier": identifier,
        "jid": jid,
        "phone_number": jid_user if not is_lid else None,
        "lid": jid_user if is_lid else None,
        "name": display_name if resolved else jid_user,
        "display_name": display_name,
        "is_lid": is_lid,
        "resolved": resolved,
    }


@mcp.tool()
def list_messages(
    after: str | None = None,
    before: str | None = None,
    sender_phone_number: str | None = None,
    chat_jid: str | None = None,
    query: str | None = None,
    limit: int = 50,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1,
    sort_by: str = "newest",
) -> list[dict[str, Any]]:
    """Get WhatsApp messages matching specified criteria with optional context.

    Each message includes sender_display showing "Name (phone)" for easy identification.

    Args:
        after: ISO-8601 date string (e.g., "2026-01-01" or "2026-01-01T09:00:00")
        before: ISO-8601 date string (e.g., "2026-01-09" or "2026-01-09T18:00:00")
        sender_phone_number: Phone number to filter by sender (e.g., "12025551234")
        chat_jid: Chat JID to filter by (e.g., "12025551234@s.whatsapp.net" or group JID)
        query: Search term to filter messages by content
        limit: Max messages to return (default 50, max 500)
        page: Page number for pagination (default 0)
        include_context: Include surrounding messages for context (default True)
        context_before: Messages to include before each match (default 1)
        context_after: Messages to include after each match (default 1)
        sort_by: "newest" (default, most recent first) or "oldest" (chronological)
    """
    # Cap limit at 500 to prevent excessive queries
    limit = min(limit, 500)
    messages = whatsapp_list_messages(
        after=after,
        before=before,
        sender_phone_number=sender_phone_number,
        chat_jid=chat_jid,
        query=query,
        limit=limit,
        page=page,
        include_context=include_context,
        context_before=context_before,
        context_after=context_after,
        sort_by=sort_by,
    )
    return messages


@mcp.tool()
def list_chats(
    query: str | None = None,
    limit: int = 50,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active",
) -> list[dict[str, Any]]:
    """Get WhatsApp chats matching specified criteria.

    Args:
        query: Search term to filter chats by name or JID
        limit: Max chats to return (default 50, max 200)
        page: Page number for pagination (default 0)
        include_last_message: Include the last message in each chat (default True)
        sort_by: "last_active" (default, most recent first) or "name" (alphabetical)

    Returns:
        Chat dictionaries with jid, name, is_group, last_message_time, last_message,
        last_sender, last_is_from_me, last_read_time and unread. `last_read_time` is
        how far the chat has been read on any device (null if never reported); `unread`
        is true when the last message is inbound and newer than that marker, so chats
        already read on the phone are not reported as unread.
    """
    # Cap limit at 200 to prevent excessive queries
    limit = min(limit, 200)
    chats = whatsapp_list_chats(
        query=query, limit=limit, page=page, include_last_message=include_last_message, sort_by=sort_by
    )
    return chats


@mcp.tool()
def get_chat(chat_jid: str, include_last_message: bool = True) -> dict[str, Any]:
    """Get WhatsApp chat metadata by JID.

    Args:
        chat_jid: The JID of the chat to retrieve
        include_last_message: Whether to include the last message (default True)

    Returns:
        Chat dictionary — same shape as list_chats, including last_read_time and unread.
    """
    chat = whatsapp_get_chat(chat_jid, include_last_message)
    return chat


@mcp.tool()
def get_direct_chat_by_contact(sender_phone_number: str) -> dict[str, Any]:
    """Get WhatsApp chat metadata by sender phone number.

    Args:
        sender_phone_number: The phone number to search for
    """
    chat = whatsapp_get_direct_chat_by_contact(sender_phone_number)
    return chat


@mcp.tool()
def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> list[dict[str, Any]]:
    """Get all WhatsApp chats involving the contact.

    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    chats = whatsapp_get_contact_chats(jid, limit, page)
    return chats


@mcp.tool()
def get_last_interaction(jid: str) -> dict[str, Any]:
    """Get most recent WhatsApp message involving the contact.

    Args:
        jid: The JID of the contact to search for

    Returns:
        Message dictionary with id, timestamp, sender, content, etc. or empty dict if not found.
    """
    message = whatsapp_get_last_interaction(jid)
    return message if message else {}


@mcp.tool()
def get_message_context(message_id: str, before: int = 5, after: int = 5) -> dict[str, Any]:
    """Get context around a specific WhatsApp message.

    Args:
        message_id: The ID of the message to get context for
        before: Number of messages to include before the target message (default 5)
        after: Number of messages to include after the target message (default 5)
    """
    context = whatsapp_get_message_context(message_id, before, after)
    return {
        "message": msg_to_dict(context.message),
        "before": [msg_to_dict(message) for message in context.before],
        "after": [msg_to_dict(message) for message in context.after],
    }


@mcp.tool()
def search_messages(
    query: Annotated[
        str,
        Field(
            description="Search query across all messages. Supports exact phrases in quotes, boolean AND/OR/NOT, and prefix wildcards (e.g. 'termo*')"
        ),
    ],
    chat_jid: Annotated[
        str | None, Field(description="Optional chat JID to filter messages to a specific conversation")
    ] = None,
    sender_phone_number: Annotated[
        str | None, Field(description="Optional phone number to filter messages by sender")
    ] = None,
    after: Annotated[str | None, Field(description="Optional ISO timestamp to filter messages after this date")] = None,
    before: Annotated[
        str | None, Field(description="Optional ISO timestamp to filter messages before this date")
    ] = None,
    timeframe: Annotated[
        str | None,
        Field(
            description="Optional natural timeframe preset: 'today', 'yesterday', 'last_24_hours', 'last_3_days', 'this_week', 'last_7_days', 'last_30_days', 'last_6_months'"
        ),
    ] = None,
    limit: Annotated[int, Field(description="Maximum number of messages to return (default 20)")] = 20,
    page: Annotated[int, Field(description="Page number for pagination (default 0)")] = 0,
) -> list[dict[str, Any]]:
    """Search messages across all conversations using high-performance SQLite FTS5 full-text indexing."""
    return whatsapp_search_messages(
        query=query,
        chat_jid=chat_jid,
        sender_phone_number=sender_phone_number,
        after=after,
        before=before,
        timeframe=timeframe,
        limit=limit,
        page=page,
    )


@mcp.tool()
def catch_up(
    timeframe: Annotated[
        str,
        Field(
            description="Timeframe for the activity digest: 'today', 'yesterday', 'last_24_hours', 'last_3_days', 'this_week', 'last_7_days', 'last_30_days'"
        ),
    ] = "today",
    only_groups: Annotated[bool, Field(description="Whether to only include group chats in the digest")] = False,
    limit_chats: Annotated[int, Field(description="Maximum number of active chats to highlight (default 10)")] = 10,
) -> dict[str, Any]:
    """Provide an intelligent activity digest and catch-up summary of recent WhatsApp activity.

    Includes total message counts, most active chats with recent message previews,
    unanswered questions directed at the user, and a breakdown of media received.
    """
    return whatsapp_catch_up(
        timeframe=timeframe,
        only_groups=only_groups,
        limit_chats=limit_chats,
    )


@mcp.tool()
def list_unread_chats(
    only_groups: Annotated[bool, Field(description="Whether to only list unread group chats")] = False,
    limit: Annotated[int, Field(description="Maximum number of unread chats to return (default 30)")] = 30,
) -> list[dict[str, Any]]:
    """List conversations that have unread incoming messages, sorted by unread count."""
    return whatsapp_list_unread_chats(
        only_groups=only_groups,
        limit=limit,
    )


@mcp.tool()
def mark_chat_read(
    chat_jid: Annotated[
        str, Field(description="The chat JID to mark as read (e.g. '554899999999@s.whatsapp.net' or group JID)")
    ],
    message_id: Annotated[
        str | None, Field(description="Optional ID of a specific message to mark read through")
    ] = None,
) -> dict[str, Any]:
    """Mark a WhatsApp conversation as read natively.

    Synchronizes read state with WhatsApp (multi-device app state sync) and sends
    read receipts for unread messages up to the specified message (or the latest received message).
    Respects account privacy settings for read receipts.

    Requires WHATSAPP_MARK_READ_ENABLED=true.

    Args:
        chat_jid: Target chat JID or phone number.
        message_id: Optional ID of a specific message to mark read through. If omitted, marks through the latest known received message.

    Returns:
        Status dictionary with chat_jid, read markers, timestamps, and unread flag.
    """
    return whatsapp_mark_chat_read(
        chat_jid=chat_jid,
        message_id=message_id,
    )


@mcp.tool()
def mark_chats_read(
    chat_jids: Annotated[list[str], Field(description="List of chat JIDs to mark as read (up to 50)")],
) -> dict[str, Any]:
    """Mark multiple WhatsApp conversations as read in batch (up to 50 chats).

    Requires WHATSAPP_MARK_READ_ENABLED=true.

    Args:
        chat_jids: List of chat JIDs to mark as read.

    Returns:
        Summary dictionary with overall success, total count, and individual chat results.
    """
    return whatsapp_mark_chats_read(
        chat_jids=chat_jids,
    )


@mcp.tool()
def extract_action_items(
    chat_jid: Annotated[
        str | None, Field(description="Optional chat JID to filter messages to a specific conversation")
    ] = None,
    timeframe: Annotated[
        str, Field(description="Timeframe to scan: 'today', 'last_3_days', 'this_week', 'last_7_days', 'last_30_days'")
    ] = "last_7_days",
    limit: Annotated[int, Field(description="Maximum number of action items to extract (default 20)")] = 20,
) -> list[dict[str, Any]]:
    """Extract action items, asks, commitments, and deadlines from recent conversations."""
    return whatsapp_extract_action_items(
        chat_jid=chat_jid,
        timeframe=timeframe,
        limit=limit,
    )


@mcp.tool()
def prepare_send_message(
    chat_jid: str,
    text: str,
    reply_to_message_id: str | None = None,
) -> dict[str, Any]:
    """Phase 1 of safe message sending: prepare a message draft and generate an authorization code.

    SECURITY GUARANTEE: This function NEVER sends anything to WhatsApp.
    It prepares a draft, computes a SHA-256 fingerprint, and generates a single-use
    confirmation code (e.g. 'ENVIAR K7M4') valid for 10 minutes.
    The message will ONLY be dispatched if commit_send_message is subsequently called
    with the exact authorization code and server write permissions are enabled.

    CRITICAL AGENT INSTRUCTIONS:
    - Present the message text, recipient, and the confirmation code (e.g. 'ENVIAR K7M4') to the user.
    - When the user sends back the confirmation code, immediately call commit_send_message(authorization_code=code).
    - NEVER call prepare_send_message again upon receiving an authorization code, even if you lost send_id.
    - The server resolves drafts directly by authorization_code. Calling prepare_send_message again resets the code!

    Args:
        chat_jid: Target chat JID or phone number (e.g. '554899999999@s.whatsapp.net' or group JID)
        text: Exact message text to send
        reply_to_message_id: Optional ID of the message to reply to

    Returns:
        Draft details including send_id, text_sha256, expires_at, and authorization_code.
    """
    return whatsapp_prepare_send_message(
        chat_jid=chat_jid,
        text=text,
        reply_to_message_id=reply_to_message_id,
    )


@mcp.tool()
def commit_send_message(
    authorization_code: str,
    send_id: str | None = None,
) -> dict[str, Any]:
    """Phase 2 of safe message sending: authorize and dispatch a prepared message draft.

    SECURITY GUARANTEE: This is the ONLY function that can send messages to WhatsApp.
    Requires server write permissions (WHATSAPP_WRITE_ENABLED=true) and verifies
    the single-use authorization code (e.g. 'ENVIAR K7M4') generated during Phase 1.

    CRITICAL AGENT INSTRUCTIONS:
    - When the user provides the confirmation code (e.g. 'ENVIAR K7M4'), call this tool immediately!
    - DO NOT call prepare_send_message again just because you forgot or lost send_id!
    - Only authorization_code is required. send_id is optional.
    - If the message was already sent, this tool safely returns the existing confirmation without duplicating.

    Args:
        authorization_code: Confirmation code received from user (e.g. 'ENVIAR K7M4').
        send_id: Optional draft ID from prepare_send_message (can be omitted; code is sufficient).

    Returns:
        Send status dictionary.
    """
    return whatsapp_commit_send_message(
        authorization_code=authorization_code,
        send_id=send_id,
    )


@mcp.tool()
def get_send_status(
    authorization_code: str | None = None,
    send_id: str | None = None,
) -> dict[str, Any]:
    """Check the status of a WhatsApp message draft by confirmation code or send_id (read-only).

    Use this tool to verify whether a draft is pending, sent, expired, or failed.
    Does NOT send any messages.

    Args:
        authorization_code: Confirmation code (e.g. 'ENVIAR K7M4')
        send_id: Optional draft ID from prepare_send_message

    Returns:
        Draft status dictionary.
    """
    return whatsapp_get_send_status(
        authorization_code=authorization_code,
        send_id=send_id,
    )


@mcp.tool()
def list_chat_lists() -> list[dict[str, Any]]:
    """List all WhatsApp labels and custom chat lists (e.g. 'Para responder') with chat counts.

    Returns:
        List of chat lists with id, name, type, source, and chat_count.
    """
    return whatsapp_list_chat_lists()


@mcp.tool()
def list_chats_by_list(
    list_name: str | None = None,
    list_id: str | None = None,
    limit: int = 50,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active",
) -> list[dict[str, Any]]:
    """Get chats belonging to a WhatsApp label or custom list (e.g. 'Para responder').

    Args:
        list_name: Name of the chat list/label
        list_id: ID of the chat list/label
        limit: Max chats to return (default 50, max 200)
        page: Page number for pagination (default 0)
        include_last_message: Include the last message in each chat (default True)
        sort_by: 'last_active' (default, most recent first) or 'name' (alphabetical)

    Returns:
        List of chat dictionaries with the same structure as list_chats.
    """
    limit = min(limit, 200)
    return whatsapp_list_chats_by_list(
        list_name=list_name,
        list_id=list_id,
        limit=limit,
        page=page,
        include_last_message=include_last_message,
        sort_by=sort_by,
    )


@mcp.tool()
def get_chat_lists(chat_jid: str) -> list[dict[str, Any]]:
    """Get all lists/labels to which a chat belongs.

    Args:
        chat_jid: WhatsApp JID of the chat

    Returns:
        List of lists/labels the chat is part of.
    """
    return whatsapp_get_chat_lists(chat_jid)


@mcp.tool()
def create_chat_list(name: str) -> dict[str, Any]:
    """Create a new local chat list / filter tag (e.g. 'Para responder').

    Args:
        name: Name for the chat list

    Returns:
        Dictionary with list ID, name, and status.
    """
    return whatsapp_create_chat_list(name)


@mcp.tool()
def add_chat_to_list(list_name: str, chat_jid: str) -> dict[str, Any]:
    """Add a chat to a list/label.

    Args:
        list_name: Name or ID of the list
        chat_jid: WhatsApp JID of the chat to add

    Returns:
        Status dictionary.
    """
    return whatsapp_add_chat_to_list(list_name, chat_jid)


@mcp.tool()
def remove_chat_from_list(list_name: str, chat_jid: str) -> dict[str, Any]:
    """Remove a chat from a list/label.

    Args:
        list_name: Name or ID of the list
        chat_jid: WhatsApp JID of the chat to remove

    Returns:
        Status dictionary.
    """
    return whatsapp_remove_chat_from_list(list_name, chat_jid)


@mcp.tool()
def download_media(message_id: str, chat_jid: str) -> dict[str, Any]:
    """Download media from a WhatsApp message and get the local file path.

    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message

    Returns:
        A dictionary containing success status, a status message, and the file path if successful
    """
    file_path = whatsapp_download_media(message_id, chat_jid)

    if file_path:
        return {"success": True, "message": "Media downloaded successfully", "file_path": file_path}
    else:
        return {"success": False, "message": "Failed to download media"}


@mcp.tool()
def view_media(
    message_id: str,
    chat_jid: str,
    max_dimension: PreviewDimension = media_preview.DEFAULT_MAX_DIMENSION,
) -> Any:
    """View the media of a WhatsApp message as an image.

    download_media only returns a local file path, which a client without
    filesystem access cannot open. This returns the picture itself instead.
    Videos return their first frame, which is enough to tell what was sent.
    Both are downscaled so a single photo cannot flood the context. Voice notes
    are not images — use transcribe_audio or read the transcript from the
    message content with list_messages.

    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message
        max_dimension: Longest edge of the returned image in pixels (default 1024)

    Returns:
        Image content on success, otherwise a dictionary explaining why not
    """
    try:
        media_preview.validate_max_dimension(max_dimension)
    except media_preview.PreviewError as exc:
        return {"success": False, "message": str(exc)}

    file_path = whatsapp_download_media(message_id, chat_jid)
    if not file_path:
        return {"success": False, "message": "Failed to download media"}

    with tempfile.TemporaryDirectory() as work_dir:
        try:
            data, image_format = media_preview.render_preview(file_path, max_dimension=max_dimension, work_dir=work_dir)
        except media_preview.PreviewError as exc:
            return {"success": False, "message": str(exc), "file_path": file_path}

    return Image(data=data, format=image_format)


@mcp.tool()
def transcribe_audio(
    message_id: str,
    chat_jid: str,
    force: bool = False,
    model: str | None = None,
    language: str | None = None,
) -> dict[str, Any]:
    """Transcribe a WhatsApp voice note and return its text.

    Runs whisper.cpp locally by default, or sends audio to the operator's
    configured OpenAI-compatible endpoint. The transcript is also written into
    the message's empty content field, so afterwards it is readable through list_messages by any
    client — including one with no filesystem access — without transcribing
    again.

    Call this for a voice note whose content field is still empty. Requires
    whisper.cpp, FFmpeg, and WHISPER_MODEL for the default provider; alternatively
    configure WHATSAPP_TRANSCRIPTION_PROVIDER=openai_compatible, URL and MODEL.

    Args:
        message_id: The ID of the message containing the voice note
        chat_jid: The JID of the chat containing the message
        force: Transcribe again even when a transcript is already stored
        model: Optional alias from WHATSAPP_TRANSCRIPTION_MODEL_MAP, e.g. large-v3-turbo.
            Aliases must be preconfigured by the server operator; never pass a file path.
        language: Optional language code, e.g. auto, pt, es, en, it. Omit to use
            the server default. Auto detects the original spoken language and does
            not translate it. Use force=true to replace an existing transcript.

    Returns:
        A dictionary with success status and the transcript
    """
    if not force:
        existing = transcription.stored_transcript(MESSAGES_DB_PATH, message_id, chat_jid)
        if existing:
            if model is not None or language is not None:
                return {
                    "success": False,
                    "message": "Transcript already stored. Set force=true to use a requested model or language.",
                    "transcript": existing,
                }
            return {"success": True, "message": "Transcript already stored", "transcript": existing}

    # WhatsApp expires media server-side after a few weeks, so the file has to
    # be on disk. This is a no-op when the bridge already downloaded it.
    file_path = whatsapp_download_media(message_id, chat_jid)
    if not file_path:
        return {
            "success": False,
            "message": (
                "Could not obtain the audio file. WhatsApp expires media after a while, "
                "so an old voice note may no longer be downloadable."
            ),
        }
    if not transcription.is_audio(file_path):
        return {
            "success": False,
            "message": "This message is not audio. Use download_media instead.",
            "file_path": file_path,
        }

    try:
        provider = transcription.provider_name()
        selected_model = transcription.select_model(provider, model)
        selected_language = transcription.select_language(provider, language)
        with tempfile.TemporaryDirectory() as work_dir:
            text = transcription.transcribe_file(
                file_path,
                work_dir,
                model=selected_model,
                provider=provider,
                language_override=selected_language,
            )
    except transcription.TranscriptionError as exc:
        return {"success": False, "message": str(exc)}

    if not transcription.store_transcript(
        MESSAGES_DB_PATH, message_id, chat_jid, text, selected_model, provider=provider
    ):
        # The words are worth returning even when the row could not be updated,
        # but say so: without the row, list_messages will not show them.
        return {
            "success": True,
            "message": "Transcribed, but the transcript could not be stored on the message row",
            "transcript": text,
            "model": model or transcription.display_model(selected_model, provider),
            "language": selected_language,
        }

    return {
        "success": True,
        "message": "Transcribed",
        "transcript": f"{transcription.label(selected_model, provider)}{text}",
        "model": model or transcription.display_model(selected_model, provider),
        "language": selected_language,
    }


@mcp.tool()
def get_sync_status() -> dict[str, Any]:
    """Diagnose WhatsApp bridge, SQLite persistence, and synchronization health.

    Returns connectivity state, latest message and chat timestamps, today's
    message count, calculated activity lag, and detected sync issues.
    """
    return whatsapp_get_sync_status()


@mcp.tool()
def get_read_queue_status(
    limit: Annotated[int, Field(description="Max recent queue items to return (default 20)")] = 20,
) -> dict[str, Any]:
    """Inspect the status of the recoverable pending read-mark queue.

    Returns the count of pending, completed, and failed operations along with
    a sanitized list of recent items without exposing sensitive message content.
    """
    return whatsapp_get_read_queue_status(limit=limit)


@mcp.tool()
def process_pending_read_marks(
    max_items: Annotated[int, Field(description="Max pending items to process in this run (default 10)")] = 10,
) -> dict[str, Any]:
    """Process pending read markings in the retry queue with exponential backoff.

    Attempts to mark pending chats as read and updates their retry counters and timestamps.
    """
    return whatsapp_process_pending_read_marks(max_items=max_items)


def create_asgi_app(mount_path: str = "", auth_token: str | None = None) -> Any:
    """Create a unified ASGI application supporting both SSE and Streamable HTTP transports.

    Serves:
    - SSE: /sse, /messages (and mounted at {mount_path}/sse, {mount_path}/messages when configured)
    - Streamable HTTP: /mcp, / (and mounted at {mount_path}/mcp, {mount_path} when configured)
    - Health checks: GET /health, GET / (and mounted at {mount_path}/health, {mount_path})
    - Authentication: Bearer token in Authorization header, ?token=<token> in query,
      or /<token> prefix in path when auth_token is configured.
    """
    from urllib.parse import parse_qs
    from starlette.applications import Starlette
    from starlette.middleware.cors import CORSMiddleware
    from starlette.requests import Request
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Mount, Route

    # Disable DNS rebinding protection since the server runs behind reverse proxy (Traefik)
    mcp.settings.transport_security.enable_dns_rebinding_protection = False

    clean_mount = "/" + mount_path.strip("/") if mount_path and mount_path.strip("/") else ""

    sse_app = mcp.sse_app(mount_path=clean_mount or "/")
    mcp.streamable_http_app()

    sse_route = [r for r in sse_app.routes if getattr(r, "path", "") == "/sse"][0]
    messages_mount = [r for r in sse_app.routes if getattr(r, "path", "") == "/messages"][0]

    class DynamicStreamApp:
        async def __call__(self, scope, receive, send):
            await mcp.session_manager.handle_request(scope, receive, send)

    stream_endpoint = DynamicStreamApp()

    class HealthOrStreamHandler:
        async def __call__(self, scope, receive, send):
            if scope["type"] == "http":
                method = scope.get("method")
                if method == "POST":
                    await stream_endpoint(scope, receive, send)
                    return
                headers = dict(scope.get("headers", []))
                accept = headers.get(b"accept", b"").decode("latin1")
                if "text/event-stream" in accept:
                    req = Request(scope, receive, send)
                    res = await sse_route.endpoint(req)
                    await res(scope, receive, send)
                    return
                sse_ep = f"{clean_mount}/sse" if clean_mount else "/sse"
                res = JSONResponse({"status": "healthy", "service": "whatsapp-mcp", "sse_endpoint": sse_ep})
                await res(scope, receive, send)
                return
            await stream_endpoint(scope, receive, send)

    routes = [
        # SSE endpoints
        Route("/sse", endpoint=sse_route.endpoint, methods=["GET"]),
        messages_mount,
        # Streamable HTTP endpoints
        Route("/mcp", endpoint=stream_endpoint, methods=["POST", "GET"]),
        Route("/health", endpoint=HealthOrStreamHandler(), methods=["GET"]),
        Route("/", endpoint=HealthOrStreamHandler()),
    ]

    if clean_mount:
        routes.extend([
            Route(f"{clean_mount}/sse", endpoint=sse_route.endpoint, methods=["GET"]),
            Mount(f"{clean_mount}/messages", app=messages_mount.app),
            Route(f"{clean_mount}/mcp", endpoint=stream_endpoint, methods=["POST", "GET"]),
            Route(f"{clean_mount}/health", endpoint=HealthOrStreamHandler(), methods=["GET"]),
            Route(clean_mount, endpoint=HealthOrStreamHandler()),
        ])

    import contextlib

    @contextlib.asynccontextmanager
    async def lifespan_wrapper(application):
        if getattr(mcp.session_manager, "_has_started", False):
            mcp._session_manager = None
            mcp.streamable_http_app()

        worker_stop = asyncio.Event()

        async def _background_queue_worker():
            # Initial brief delay before starting periodic pending read mark drain
            await asyncio.sleep(5)
            while not worker_stop.is_set():
                try:
                    await asyncio.to_thread(whatsapp_process_pending_read_marks, max_items=10)
                except Exception as exc:
                    print(f"[QueueWorker] Error processing pending read marks: {exc}", file=sys.stderr)
                try:
                    await asyncio.wait_for(worker_stop.wait(), timeout=60.0)
                except asyncio.TimeoutError:
                    pass

        worker_task = asyncio.create_task(_background_queue_worker())
        try:
            async with mcp.session_manager.run():
                yield
        finally:
            worker_stop.set()
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task

    app = Starlette(
        routes=routes,
        lifespan=lifespan_wrapper,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if auth_token:
        class BearerAuthMiddleware:
            def __init__(self, inner, token: str):
                self.inner = inner
                self.token = token

            async def __call__(self, scope, receive, send):
                if scope["type"] == "http":
                    headers = dict(scope.get("headers", []))
                    auth = headers.get(b"authorization", b"").decode("latin1").strip()
                    path = scope.get("path", "")
                    qs = parse_qs(scope.get("query_string", b"").decode("latin1"))
                    query_token = qs.get("token", [None])[0]

                    token_prefix = f"/{self.token}"
                    path_has_token = path == token_prefix or path.startswith(f"{token_prefix}/")

                    health_paths = {"/", "", "/health"}
                    if clean_mount:
                        health_paths.update({clean_mount, f"{clean_mount}/", f"{clean_mount}/health"})

                    is_health_check = (path in health_paths) and (
                        scope.get("method") in ("GET", "HEAD") and b"text/event-stream" not in headers.get(b"accept", b"")
                    )

                    is_messages_session = path in ("/messages", f"{clean_mount}/messages") or path.startswith("/messages/") or (
                        bool(clean_mount) and path.startswith(f"{clean_mount}/messages/")
                    )

                    if not is_health_check and not is_messages_session:
                        if auth != f"Bearer {self.token}" and query_token != self.token and not path_has_token:
                            res = Response("Unauthorized\n", status_code=401, headers={"WWW-Authenticate": "Bearer", "Content-Type": "text/plain; charset=utf-8"})
                            await res(scope, receive, send)
                            return

                    if path_has_token:
                        scope = dict(scope)
                        stripped = path[len(token_prefix):]
                        scope["path"] = stripped if stripped else "/"

                await self.inner(scope, receive, send)

        return BearerAuthMiddleware(app, auth_token)

    return app


def shutdown_handler(signum, frame):
    """Handle shutdown signals gracefully to prevent zombie processes."""
    sys.exit(0)


if __name__ == "__main__":
    # Capture before any await — os.getppid() is dynamic.
    parent_pid = os.getppid()
    # Register signal handlers for clean shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Reconcile database on startup
    reconcile_database()

    # Resolve the transport first: host/port are only used (and validated) for the
    # network transports, so a bad WHATSAPP_MCP_PORT can't break a stdio launch.
    # The localhost default keeps a remote server unreachable until explicitly opened up.
    try:
        transport = resolve_transport(os.getenv("WHATSAPP_MCP_TRANSPORT"))
        if transport != "stdio":
            mcp.settings.host = resolve_host(os.getenv("WHATSAPP_MCP_HOST"))
            mcp.settings.port = resolve_port(os.getenv("WHATSAPP_MCP_PORT"))
            # stdout is reserved for the protocol on stdio; log startup to stderr.
            print(
                f"WhatsApp MCP server listening on {mcp.settings.host}:{mcp.settings.port} via {transport}",
                file=sys.stderr,
            )
    except ValueError as exc:
        raise SystemExit(str(exc)) from None

    if transport == "stdio":
        install_stdio_parent_watchdog("WHATSAPP_PARENT_WATCHDOG_S", parent_pid=parent_pid)
        stop_worker = threading.Event()

        def _stdio_queue_worker():
            while not stop_worker.wait(60.0):
                try:
                    whatsapp_process_pending_read_marks(max_items=10)
                except Exception as exc:
                    print(f"[QueueWorker] Error processing pending read marks: {exc}", file=sys.stderr)

        t = threading.Thread(target=_stdio_queue_worker, daemon=True, name="PendingReadMarksWorker")
        t.start()
        try:
            mcp.run(transport=transport)
        finally:
            stop_worker.set()
    else:
        import uvicorn
        from urllib.parse import urlparse

        public_base_url = os.getenv("PUBLIC_BASE_URL", "")
        public_mount_path = urlparse(public_base_url).path.rstrip("/") if public_base_url else os.getenv("MCP_MOUNT_PATH", "")
        auth_token = os.getenv("MCP_AUTH_TOKEN")

        asgi_app = create_asgi_app(mount_path=public_mount_path, auth_token=auth_token)

        uvicorn.run(
            asgi_app,
            host=mcp.settings.host,
            port=mcp.settings.port,
            log_level=mcp.settings.log_level.lower(),
        )
