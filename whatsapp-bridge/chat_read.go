package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

var (
	errChatNotFound        = errors.New("chat not found")
	errMessageNotFound     = errors.New("message not found")
	errMessageChatMismatch = errors.New("message does not belong to chat")
)

// MarkChatReadRequest represents the incoming payload to mark a chat as read.
type MarkChatReadRequest struct {
	ChatJID   string `json:"chat_jid"`
	MessageID string `json:"message_id,omitempty"`
}

// MarkChatReadResponse represents the result of a mark chat read operation.
type MarkChatReadResponse struct {
	Success                     bool    `json:"success"`
	ChatJID                     string  `json:"chat_jid,omitempty"`
	MarkedReadThroughMessageID  *string `json:"marked_read_through_message_id,omitempty"`
	MarkedReadThroughTimestamp *string `json:"marked_read_through_timestamp,omitempty"`
	PreviousLastReadTime        *string `json:"previous_last_read_time,omitempty"`
	NewLastReadTime             *string `json:"new_last_read_time,omitempty"`
	Unread                      bool    `json:"unread"`
	Status                      string  `json:"status,omitempty"`
	Error                       string  `json:"error,omitempty"`
}

// StoredMessageMeta holds message metadata needed for read position calculations.
type StoredMessageMeta struct {
	ID        string
	ChatJID   string
	Sender    string
	Timestamp time.Time
	IsFromMe  bool
}

// whatsAppReadClient abstracts whatsmeow operations for mark-read and testing.
type whatsAppReadClient interface {
	IsConnected() bool
	MarkRead(ctx context.Context, ids []types.MessageID, timestamp time.Time, chat, sender types.JID, receiptTypeExtra ...types.ReceiptType) error
	SendAppState(ctx context.Context, patch appstate.PatchInfo) error
}

// isMarkReadAllowed checks if marking chats as read is permitted.
// Requires WHATSAPP_MARK_READ_ENABLED=true and WHATSAPP_READ_ONLY != true.
func isMarkReadAllowed() bool {
	if getEnvBool("WHATSAPP_READ_ONLY", false) {
		return false
	}
	return getEnvBool("WHATSAPP_MARK_READ_ENABLED", false)
}

// GetChatReadInfo retrieves previous last_read_time and last_message_time for a chat.
func (store *MessageStore) GetChatReadInfo(chatJID string) (*time.Time, *time.Time, bool, error) {
	var (
		lr sql.NullTime
		lm sql.NullTime
	)
	err := store.db.QueryRow(
		"SELECT last_read_time, last_message_time FROM chats WHERE jid = ?",
		chatJID,
	).Scan(&lr, &lm)
	if errors.Is(err, sql.ErrNoRows) {
		// Check if any messages exist for this chat even if no row in chats table
		var count int
		if countErr := store.db.QueryRow("SELECT COUNT(1) FROM messages WHERE chat_jid = ?", chatJID).Scan(&count); countErr == nil && count > 0 {
			return nil, nil, true, nil
		}
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	var lastRead, lastMsg *time.Time
	if lr.Valid {
		t := lr.Time.UTC()
		lastRead = &t
	}
	if lm.Valid {
		t := lm.Time.UTC()
		lastMsg = &t
	}
	return lastRead, lastMsg, true, nil
}

// GetTargetMessage finds a specific message in the chat.
func (store *MessageStore) GetTargetMessage(chatJID, messageID string) (*StoredMessageMeta, error) {
	var msg StoredMessageMeta
	err := store.db.QueryRow(
		"SELECT id, chat_jid, sender, timestamp, is_from_me FROM messages WHERE id = ?",
		messageID,
	).Scan(&msg.ID, &msg.ChatJID, &msg.Sender, &msg.Timestamp, &msg.IsFromMe)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errMessageNotFound
	}
	if err != nil {
		return nil, err
	}
	if msg.ChatJID != chatJID {
		return nil, errMessageChatMismatch
	}
	msg.Timestamp = msg.Timestamp.UTC()
	return &msg, nil
}

// GetLatestInboundMessage retrieves the most recent inbound message for a chat.
func (store *MessageStore) GetLatestInboundMessage(chatJID string) (*StoredMessageMeta, error) {
	var msg StoredMessageMeta
	err := store.db.QueryRow(
		`SELECT id, chat_jid, sender, timestamp, is_from_me 
		 FROM messages 
		 WHERE chat_jid = ? AND is_from_me = 0 
		 ORDER BY timestamp DESC, id DESC 
		 LIMIT 1`,
		chatJID,
	).Scan(&msg.ID, &msg.ChatJID, &msg.Sender, &msg.Timestamp, &msg.IsFromMe)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	msg.Timestamp = msg.Timestamp.UTC()
	return &msg, nil
}

// GetLatestMessage retrieves the most recent message (inbound or outbound) for a chat.
func (store *MessageStore) GetLatestMessage(chatJID string) (*StoredMessageMeta, error) {
	var msg StoredMessageMeta
	err := store.db.QueryRow(
		`SELECT id, chat_jid, sender, timestamp, is_from_me 
		 FROM messages 
		 WHERE chat_jid = ? 
		 ORDER BY timestamp DESC, id DESC 
		 LIMIT 1`,
		chatJID,
	).Scan(&msg.ID, &msg.ChatJID, &msg.Sender, &msg.Timestamp, &msg.IsFromMe)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	msg.Timestamp = msg.Timestamp.UTC()
	return &msg, nil
}

// GetUnreadInboundMessagesUpTo queries unread inbound messages up to upTo timestamp.
func (store *MessageStore) GetUnreadInboundMessagesUpTo(chatJID string, upTo time.Time, since *time.Time) ([]StoredMessageMeta, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if since != nil {
		rows, err = store.db.Query(
			`SELECT id, chat_jid, sender, timestamp, is_from_me 
			 FROM messages 
			 WHERE chat_jid = ? AND is_from_me = 0 AND timestamp <= ? AND timestamp > ?
			 ORDER BY timestamp ASC`,
			chatJID, upTo, *since,
		)
	} else {
		rows, err = store.db.Query(
			`SELECT id, chat_jid, sender, timestamp, is_from_me 
			 FROM messages 
			 WHERE chat_jid = ? AND is_from_me = 0 AND timestamp <= ?
			 ORDER BY timestamp ASC`,
			chatJID, upTo,
		)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []StoredMessageMeta
	for rows.Next() {
		var m StoredMessageMeta
		if err := rows.Scan(&m.ID, &m.ChatJID, &m.Sender, &m.Timestamp, &m.IsFromMe); err != nil {
			return nil, err
		}
		m.Timestamp = m.Timestamp.UTC()
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// IsChatUnread determines whether a chat currently has unread inbound messages.
func (store *MessageStore) IsChatUnread(chatJID string) (bool, error) {
	var (
		lastMsgTime  sql.NullTime
		lastReadTime sql.NullTime
		lastIsFromMe sql.NullBool
	)
	err := store.db.QueryRow(`
		SELECT c.last_message_time, c.last_read_time, m.is_from_me
		FROM chats c
		LEFT JOIN messages m ON m.chat_jid = c.jid AND m.timestamp = c.last_message_time
		WHERE c.jid = ?
		ORDER BY m.id DESC LIMIT 1
	`, chatJID).Scan(&lastMsgTime, &lastReadTime, &lastIsFromMe)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !lastMsgTime.Valid || !lastIsFromMe.Valid {
		return false, nil
	}
	if lastIsFromMe.Bool {
		return false, nil
	}
	if !lastReadTime.Valid {
		return true, nil
	}
	return lastMsgTime.Time.After(lastReadTime.Time), nil
}

// registerChatReadEndpoint mounts the POST /api/chats/mark-read endpoint.
func registerChatReadEndpoint(
	mux *http.ServeMux,
	auth func(http.HandlerFunc) http.HandlerFunc,
	client *whatsmeow.Client,
	messageStore *MessageStore,
) {
	mux.HandleFunc("/api/chats/mark-read", auth(func(w http.ResponseWriter, r *http.Request) {
		var readCli whatsAppReadClient
		if client != nil {
			readCli = client
		}
		handleMarkChatRead(w, r, readCli, client, messageStore)
	}))
}

// handleMarkChatRead handles the core mark chat read logic.
func handleMarkChatRead(
	w http.ResponseWriter,
	r *http.Request,
	cli whatsAppReadClient,
	clientForJID *whatsmeow.Client,
	messageStore *MessageStore,
) {
	if !isMarkReadAllowed() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
			Success: false,
			Status:  "forbidden",
			Error:   "WHATSAPP_MARK_READ_ENABLED=false",
		})
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req MarkChatReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
			Success: false,
			Status:  "invalid_argument",
			Error:   "Invalid request format",
		})
		return
	}

	req.ChatJID = strings.TrimSpace(req.ChatJID)
	req.MessageID = strings.TrimSpace(req.MessageID)

	if req.ChatJID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
			Success: false,
			Status:  "invalid_argument",
			Error:   "chat_jid is required",
		})
		return
	}

	if !strings.Contains(req.ChatJID, "@") {
		req.ChatJID = req.ChatJID + "@s.whatsapp.net"
	}

	parsedChatJID, err := types.ParseJID(req.ChatJID)
	if err != nil || parsedChatJID.User == "" || parsedChatJID.Server == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
			Success: false,
			Status:  "invalid_argument",
			Error:   fmt.Sprintf("Invalid chat_jid: %s", req.ChatJID),
		})
		return
	}

	if cli == nil || !cli.IsConnected() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
			Success: false,
			Status:  "unavailable",
			Error:   "WhatsApp client is not connected. Please wait for reconnection.",
		})
		return
	}

	prevLastRead, lastMsgTime, found, err := messageStore.GetChatReadInfo(req.ChatJID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
			Success: false,
			Status:  "internal_error",
			Error:   fmt.Sprintf("Database error: %v", err),
		})
		return
	}
	if !found {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
			Success: false,
			Status:  "not_found",
			Error:   fmt.Sprintf("Chat %s not found", req.ChatJID),
		})
		return
	}

	var (
		targetMsg            *StoredMessageMeta
		readThroughMsgID     *string
		readThroughTimestamp time.Time
	)

	if req.MessageID != "" {
		targetMsg, err = messageStore.GetTargetMessage(req.ChatJID, req.MessageID)
		if errors.Is(err, errMessageNotFound) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
				Success: false,
				Status:  "not_found",
				Error:   fmt.Sprintf("Message %s not found", req.MessageID),
			})
			return
		}
		if errors.Is(err, errMessageChatMismatch) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
				Success: false,
				Status:  "invalid_argument",
				Error:   fmt.Sprintf("Message %s does not belong to chat %s", req.MessageID, req.ChatJID),
			})
			return
		}
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
				Success: false,
				Status:  "internal_error",
				Error:   fmt.Sprintf("Failed to retrieve message: %v", err),
			})
			return
		}
		readThroughMsgID = &targetMsg.ID
		readThroughTimestamp = targetMsg.Timestamp
	} else {
		// Default: mark read through the latest received (inbound) message
		targetMsg, err = messageStore.GetLatestInboundMessage(req.ChatJID)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
				Success: false,
				Status:  "internal_error",
				Error:   fmt.Sprintf("Failed to query inbound messages: %v", err),
			})
			return
		}
		if targetMsg != nil {
			readThroughMsgID = &targetMsg.ID
			readThroughTimestamp = targetMsg.Timestamp
		} else {
			// No inbound message found, fall back to latest message overall
			targetMsg, err = messageStore.GetLatestMessage(req.ChatJID)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
					Success: false,
					Status:  "internal_error",
					Error:   fmt.Sprintf("Failed to query messages: %v", err),
				})
				return
			}
			if targetMsg != nil {
				readThroughMsgID = &targetMsg.ID
				readThroughTimestamp = targetMsg.Timestamp
			} else if lastMsgTime != nil {
				readThroughTimestamp = *lastMsgTime
			} else {
				readThroughTimestamp = time.Now().UTC()
			}
		}

		// Ensure read marker is at least lastMsgTime so the chat doesn't linger as unread
		if lastMsgTime != nil && lastMsgTime.After(readThroughTimestamp) {
			readThroughTimestamp = *lastMsgTime
		}
	}

	// 1. Send read receipts for unread inbound messages up to readThroughTimestamp
	unreadMsgs, err := messageStore.GetUnreadInboundMessagesUpTo(req.ChatJID, readThroughTimestamp, prevLastRead)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
			Success: false,
			Status:  "internal_error",
			Error:   fmt.Sprintf("Failed to query unread messages: %v", err),
		})
		return
	}

	resolvedChatJID := parsedChatJID
	if clientForJID != nil {
		if rJID, rErr := resolveRecipientJID(clientForJID, req.ChatJID); rErr == nil && rJID.User != "" {
			resolvedChatJID = rJID
		}
	}

	if len(unreadMsgs) > 0 {
		bySender := make(map[string][]types.MessageID)
		for _, m := range unreadMsgs {
			bySender[m.Sender] = append(bySender[m.Sender], types.MessageID(m.ID))
		}
		for sender, ids := range bySender {
			var senderJID types.JID
			if resolvedChatJID.Server == types.GroupServer && clientForJID != nil {
				senderJID, _ = resolveRecipientJID(clientForJID, sender)
			}
			if markErr := cli.MarkRead(context.Background(), ids, readThroughTimestamp, resolvedChatJID, senderJID); markErr != nil {
				fmt.Printf("Warning: failed to send read receipt for %s in chat %s: %v\n", sender, req.ChatJID, markErr)
			}
		}
	}

	// 2. Multi-device sync: send AppState patch to WAPatchRegularLow
	var lastMessageKey *waCommon.MessageKey
	if targetMsg != nil && targetMsg.ID != "" {
		key := &waCommon.MessageKey{
			RemoteJID: proto.String(resolvedChatJID.String()),
			FromMe:    proto.Bool(targetMsg.IsFromMe),
			ID:        proto.String(targetMsg.ID),
		}
		if resolvedChatJID.Server == types.GroupServer && !targetMsg.IsFromMe && targetMsg.Sender != "" && clientForJID != nil {
			senderJID, rErr := resolveRecipientJID(clientForJID, targetMsg.Sender)
			if rErr == nil && senderJID.User != "" {
				key.Participant = proto.String(senderJID.ToNonAD().String())
			}
		}
		lastMessageKey = key
	}

	patch := appstate.BuildMarkChatAsRead(resolvedChatJID, true, readThroughTimestamp, lastMessageKey)
	if sendErr := cli.SendAppState(context.Background(), patch); sendErr != nil {
		fmt.Printf("Warning: failed to send app state mark-read patch for %s: %v\n", req.ChatJID, sendErr)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
			Success: false,
			Status:  "internal_error",
			Error:   fmt.Sprintf("Failed to sync app state with WhatsApp: %v", sendErr),
		})
		return
	}

	// 3. Persist local read marker in database
	if markErr := messageStore.MarkChatRead(req.ChatJID, readThroughTimestamp); markErr != nil {
		fmt.Printf("Warning: failed to update local read marker for %s: %v\n", req.ChatJID, markErr)
	}

	// 4. Derive updated unread state
	isUnread, _ := messageStore.IsChatUnread(req.ChatJID)

	newLastRead, _, _, _ := messageStore.GetChatReadInfo(req.ChatJID)
	var prevLastReadStr, newLastReadStr *string
	if prevLastRead != nil {
		s := prevLastRead.Format(time.RFC3339)
		prevLastReadStr = &s
	}
	if newLastRead != nil {
		s := newLastRead.Format(time.RFC3339)
		newLastReadStr = &s
	}
	tsStr := readThroughTimestamp.Format(time.RFC3339)
	readThroughTimestampStr := &tsStr

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(MarkChatReadResponse{
		Success:                     true,
		ChatJID:                     req.ChatJID,
		MarkedReadThroughMessageID:  readThroughMsgID,
		MarkedReadThroughTimestamp: readThroughTimestampStr,
		PreviousLastReadTime:        prevLastReadStr,
		NewLastReadTime:             newLastReadStr,
		Unread:                      isUnread,
	})
}
