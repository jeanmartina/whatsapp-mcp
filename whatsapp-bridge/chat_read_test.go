package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type mockReadClient struct {
	connected       bool
	markReadCalls   []markReadCall
	appStatePatches []appstate.PatchInfo
	markReadErr     error
	appStateErr     error
}

type markReadCall struct {
	ids       []types.MessageID
	timestamp time.Time
	chat      types.JID
	sender    types.JID
}

func (m *mockReadClient) IsConnected() bool {
	return m.connected
}

func (m *mockReadClient) MarkRead(ctx context.Context, ids []types.MessageID, timestamp time.Time, chat, sender types.JID, receiptTypeExtra ...types.ReceiptType) error {
	m.markReadCalls = append(m.markReadCalls, markReadCall{
		ids:       ids,
		timestamp: timestamp,
		chat:      chat,
		sender:    sender,
	})
	return m.markReadErr
}

func (m *mockReadClient) SendAppState(ctx context.Context, patch appstate.PatchInfo) error {
	m.appStatePatches = append(m.appStatePatches, patch)
	return m.appStateErr
}

func TestIsMarkReadAllowed(t *testing.T) {
	t.Run("default is disallowed", func(t *testing.T) {
		t.Setenv("WHATSAPP_MARK_READ_ENABLED", "")
		t.Setenv("WHATSAPP_READ_ONLY", "")
		if isMarkReadAllowed() {
			t.Fatal("expected isMarkReadAllowed to be false by default")
		}
	})

	t.Run("explicit true is allowed", func(t *testing.T) {
		t.Setenv("WHATSAPP_MARK_READ_ENABLED", "true")
		t.Setenv("WHATSAPP_READ_ONLY", "false")
		if !isMarkReadAllowed() {
			t.Fatal("expected isMarkReadAllowed to be true when WHATSAPP_MARK_READ_ENABLED=true")
		}
	})

	t.Run("read only mode overrides mark read enabled", func(t *testing.T) {
		t.Setenv("WHATSAPP_MARK_READ_ENABLED", "true")
		t.Setenv("WHATSAPP_READ_ONLY", "true")
		if isMarkReadAllowed() {
			t.Fatal("expected isMarkReadAllowed to be false when WHATSAPP_READ_ONLY=true")
		}
	})
}

func TestMarkChatReadEndpointValidation(t *testing.T) {
	t.Setenv("WHATSAPP_MARK_READ_ENABLED", "true")
	ms := newTestMessageStore(t)
	mockCli := &mockReadClient{connected: false}

	cases := []struct {
		name       string
		method     string
		body       string
		enabled    bool
		wantStatus int
		wantError  string
	}{
		{
			name:       "feature flag disabled returns 403",
			method:     http.MethodPost,
			body:       `{"chat_jid":"15551234567@s.whatsapp.net"}`,
			enabled:    false,
			wantStatus: http.StatusForbidden,
			wantError:  "WHATSAPP_MARK_READ_ENABLED=false",
		},
		{
			name:       "method not allowed",
			method:     http.MethodGet,
			body:       "",
			enabled:    true,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "invalid json",
			method:     http.MethodPost,
			body:       "{invalid",
			enabled:    true,
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_argument",
		},
		{
			name:       "missing chat_jid",
			method:     http.MethodPost,
			body:       `{"message_id":"123"}`,
			enabled:    true,
			wantStatus: http.StatusBadRequest,
			wantError:  "chat_jid is required",
		},
		{
			name:       "invalid chat_jid format",
			method:     http.MethodPost,
			body:       `{"chat_jid":"invalid@@@"}`,
			enabled:    true,
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_argument",
		},
		{
			name:       "client disconnected returns 503",
			method:     http.MethodPost,
			body:       `{"chat_jid":"15551234567@s.whatsapp.net"}`,
			enabled:    true,
			wantStatus: http.StatusServiceUnavailable,
			wantError:  "unavailable",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.enabled {
				t.Setenv("WHATSAPP_MARK_READ_ENABLED", "true")
			} else {
				t.Setenv("WHATSAPP_MARK_READ_ENABLED", "false")
			}

			req := httptest.NewRequest(tc.method, "/api/chats/mark-read", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()

			handleMarkChatRead(rec, req, mockCli, nil, ms)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantError != "" && !strings.Contains(rec.Body.String(), tc.wantError) {
				t.Fatalf("expected body to contain %q, got %s", tc.wantError, rec.Body.String())
			}
		})
	}
}

func TestMarkChatRead_NotFoundAndMismatchErrors(t *testing.T) {
	t.Setenv("WHATSAPP_MARK_READ_ENABLED", "true")
	ms := newTestMessageStore(t)
	mockCli := &mockReadClient{connected: true}

	chat1 := "15551234567@s.whatsapp.net"
	chat2 := "15559876543@s.whatsapp.net"
	t1 := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

	if err := ms.StoreChat(chat1, "Chat 1", t1); err != nil {
		t.Fatalf("StoreChat: %v", err)
	}
	if err := ms.StoreMessage("msg-chat1", chat1, "15551234567", "hi", t1, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage: %v", err)
	}

	t.Run("chat not found returns 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/chats/mark-read", strings.NewReader(`{"chat_jid":"`+chat2+`"}`))
		rec := httptest.NewRecorder()
		handleMarkChatRead(rec, req, mockCli, nil, ms)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d (body: %s)", rec.Code, rec.Body.String())
		}
	})

	t.Run("message not found returns 404", func(t *testing.T) {
		body := `{"chat_jid":"` + chat1 + `","message_id":"nonexistent"}`
		req := httptest.NewRequest(http.MethodPost, "/api/chats/mark-read", strings.NewReader(body))
		rec := httptest.NewRecorder()
		handleMarkChatRead(rec, req, mockCli, nil, ms)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d (body: %s)", rec.Code, rec.Body.String())
		}
	})

	t.Run("message belonging to other chat returns 400", func(t *testing.T) {
		if err := ms.StoreChat(chat2, "Chat 2", t1); err != nil {
			t.Fatalf("StoreChat 2: %v", err)
		}
		body := `{"chat_jid":"` + chat2 + `","message_id":"msg-chat1"}`
		req := httptest.NewRequest(http.MethodPost, "/api/chats/mark-read", strings.NewReader(body))
		rec := httptest.NewRecorder()
		handleMarkChatRead(rec, req, mockCli, nil, ms)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d (body: %s)", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "does not belong to chat") {
			t.Fatalf("expected chat mismatch error message, got %s", rec.Body.String())
		}
	})
}

func TestMarkChatRead_DirectChatSuccess(t *testing.T) {
	t.Setenv("WHATSAPP_MARK_READ_ENABLED", "true")
	ms := newTestMessageStore(t)
	mockCli := &mockReadClient{connected: true}

	chatJID := "15551234567@s.whatsapp.net"
	t1 := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 7, 10, 5, 0, 0, time.UTC)

	if err := ms.StoreChat(chatJID, "Alice", t2); err != nil {
		t.Fatalf("StoreChat: %v", err)
	}
	if err := ms.StoreMessage("msg1", chatJID, "15551234567", "first", t1, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage 1: %v", err)
	}
	if err := ms.StoreMessage("msg2", chatJID, "15551234567", "second", t2, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage 2: %v", err)
	}

	body := `{"chat_jid":"` + chatJID + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/chats/mark-read", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handleMarkChatRead(rec, req, mockCli, nil, ms)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	var resp MarkChatReadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected success = true")
	}
	if resp.Unread {
		t.Fatalf("expected unread = false after marking latest read")
	}
	if resp.MarkedReadThroughMessageID == nil || *resp.MarkedReadThroughMessageID != "msg2" {
		t.Fatalf("expected marked_read_through_message_id = 'msg2', got %v", resp.MarkedReadThroughMessageID)
	}

	// Verify receipts were sent
	if len(mockCli.markReadCalls) != 1 {
		t.Fatalf("expected 1 MarkRead call, got %d", len(mockCli.markReadCalls))
	}
	if len(mockCli.markReadCalls[0].ids) != 2 {
		t.Fatalf("expected 2 message IDs marked, got %d", len(mockCli.markReadCalls[0].ids))
	}

	// Verify AppState patch was sent
	if len(mockCli.appStatePatches) != 1 {
		t.Fatalf("expected 1 AppState patch, got %d", len(mockCli.appStatePatches))
	}
	patch := mockCli.appStatePatches[0]
	if patch.Type != appstate.WAPatchRegularLow {
		t.Fatalf("expected patch type WAPatchRegularLow, got %s", patch.Type)
	}

	// Verify database persistence
	lastRead, _, found, err := ms.GetChatReadInfo(chatJID)
	if err != nil || !found || lastRead == nil {
		t.Fatalf("expected lastRead persisted, got %v found=%v err=%v", lastRead, found, err)
	}
	if !lastRead.Equal(t2) {
		t.Fatalf("expected lastRead = %v, got %v", t2, *lastRead)
	}

	// Idempotency: calling a second time
	req2 := httptest.NewRequest(http.MethodPost, "/api/chats/mark-read", strings.NewReader(body))
	rec2 := httptest.NewRecorder()
	handleMarkChatRead(rec2, req2, mockCli, nil, ms)
	if rec2.Code != http.StatusOK {
		t.Fatalf("idempotent second call failed: %d", rec2.Code)
	}
}

func TestMarkChatRead_GroupChatMultiSender(t *testing.T) {
	t.Setenv("WHATSAPP_MARK_READ_ENABLED", "true")
	ms := newTestMessageStore(t)
	mockCli := &mockReadClient{connected: true}

	groupJID := "120363012345678901@g.us"
	alice := "15551111111"
	bob := "15552222222"
	t1 := time.Date(2026, 10, 7, 11, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 7, 11, 5, 0, 0, time.UTC)

	if err := ms.StoreChat(groupJID, "Project Group", t2); err != nil {
		t.Fatalf("StoreChat: %v", err)
	}
	if err := ms.StoreMessage("g-msg1", groupJID, alice, "from alice", t1, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage 1: %v", err)
	}
	if err := ms.StoreMessage("g-msg2", groupJID, bob, "from bob", t2, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage 2: %v", err)
	}

	body := `{"chat_jid":"` + groupJID + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/chats/mark-read", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handleMarkChatRead(rec, req, mockCli, nil, ms)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	// In a group with 2 senders, MarkRead must be called once per sender
	if len(mockCli.markReadCalls) != 2 {
		t.Fatalf("expected 2 MarkRead calls (1 per sender), got %d", len(mockCli.markReadCalls))
	}

	// AppState patch must still be sent exactly once for the group
	if len(mockCli.appStatePatches) != 1 {
		t.Fatalf("expected 1 AppState patch, got %d", len(mockCli.appStatePatches))
	}
}

func TestMarkChatRead_SpecificMessageID(t *testing.T) {
	t.Setenv("WHATSAPP_MARK_READ_ENABLED", "true")
	ms := newTestMessageStore(t)
	mockCli := &mockReadClient{connected: true}

	chatJID := "15551234567@s.whatsapp.net"
	t1 := time.Date(2026, 10, 7, 9, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 7, 9, 30, 0, 0, time.UTC)

	if err := ms.StoreChat(chatJID, "Alice", t2); err != nil {
		t.Fatalf("StoreChat: %v", err)
	}
	if err := ms.StoreMessage("m-old", chatJID, "15551234567", "old message", t1, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage 1: %v", err)
	}
	if err := ms.StoreMessage("m-new", chatJID, "15551234567", "newer message", t2, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage 2: %v", err)
	}

	// Mark read only through m-old
	body := `{"chat_jid":"` + chatJID + `","message_id":"m-old"}`
	req := httptest.NewRequest(http.MethodPost, "/api/chats/mark-read", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handleMarkChatRead(rec, req, mockCli, nil, ms)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp MarkChatReadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.MarkedReadThroughMessageID == nil || *resp.MarkedReadThroughMessageID != "m-old" {
		t.Fatalf("expected marked_read_through_message_id = 'm-old', got %v", resp.MarkedReadThroughMessageID)
	}
	// Because m-new is newer than m-old, the chat remains unread!
	if !resp.Unread {
		t.Fatalf("expected unread = true because m-new is still unread")
	}
}

func TestIncomingMarkChatAsReadAppStateEvent(t *testing.T) {
	ms := newTestMessageStore(t)
	chatJID := "15551234567@s.whatsapp.net"
	t2 := time.Date(2026, 10, 7, 10, 30, 0, 0, time.UTC)

	if err := ms.StoreChat(chatJID, "Alice", t2); err != nil {
		t.Fatalf("StoreChat: %v", err)
	}
	if err := ms.StoreMessage("m1", chatJID, "15551234567", "hello", t2, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage: %v", err)
	}

	// Initially unread
	unread, err := ms.IsChatUnread(chatJID)
	if err != nil || !unread {
		t.Fatalf("expected chat to be unread initially, got unread=%v err=%v", unread, err)
	}

	// Simulate incoming *events.MarkChatAsRead event
	evt := &events.MarkChatAsRead{
		JID:       types.NewJID("15551234567", types.DefaultUserServer),
		Timestamp: t2,
		Action: &waSyncAction.MarkChatAsReadAction{
			Read: proto.Bool(true),
			MessageRange: &waSyncAction.SyncActionMessageRange{
				LastMessageTimestamp: proto.Int64(t2.Unix()),
			},
		},
	}

	if evt.Action != nil && evt.Action.GetRead() {
		readAt := evt.Timestamp
		if mr := evt.Action.GetMessageRange(); mr != nil && mr.LastMessageTimestamp != nil && *mr.LastMessageTimestamp > 0 {
			readAt = time.Unix(*mr.LastMessageTimestamp, 0)
		}
		if err := ms.MarkChatRead(chatJID, readAt); err != nil {
			t.Fatalf("MarkChatRead failed: %v", err)
		}
	}

	unreadAfter, err := ms.IsChatUnread(chatJID)
	if err != nil || unreadAfter {
		t.Fatalf("expected chat to be read after app state event, got unread=%v", unreadAfter)
	}

	lastRead, _, found, _ := ms.GetChatReadInfo(chatJID)
	if !found || lastRead == nil || !lastRead.Equal(t2) {
		t.Fatalf("expected lastRead = %v, got %v", t2, lastRead)
	}

	// New message arrives after mark read -> chat becomes unread again
	t3 := time.Date(2026, 10, 7, 11, 0, 0, 0, time.UTC)
	if err := ms.StoreChat(chatJID, "Alice", t3); err != nil {
		t.Fatalf("StoreChat 3: %v", err)
	}
	if err := ms.StoreMessage("m2", chatJID, "15551234567", "new message", t3, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage 2: %v", err)
	}

	unreadNew, err := ms.IsChatUnread(chatJID)
	if err != nil || !unreadNew {
		t.Fatalf("expected chat to become unread again on new message, got unread=%v", unreadNew)
	}
}
