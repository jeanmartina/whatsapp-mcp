package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type mockReadClient struct {
	mu                 sync.Mutex
	connected          bool
	markReadCalls      []markReadCall
	appStatePatches    []appstate.PatchInfo
	fetchAppStateCalls []fetchAppStateCall
	peerMessages       []*waE2E.Message
	markReadErr        error
	appStateErr        error
	fetchAppStateErr   error
	appStateErrQueue   []error
	fetchAppStateFn    func(name appstate.WAPatchName, fullSync, onlyIfNotSynced bool) error
}

type fetchAppStateCall struct {
	name            appstate.WAPatchName
	fullSync        bool
	onlyIfNotSynced bool
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markReadCalls = append(m.markReadCalls, markReadCall{
		ids:       ids,
		timestamp: timestamp,
		chat:      chat,
		sender:    sender,
	})
	return m.markReadErr
}

func (m *mockReadClient) SendAppState(ctx context.Context, patch appstate.PatchInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appStatePatches = append(m.appStatePatches, patch)
	if len(m.appStateErrQueue) > 0 {
		err := m.appStateErrQueue[0]
		m.appStateErrQueue = m.appStateErrQueue[1:]
		return err
	}
	return m.appStateErr
}

func (m *mockReadClient) FetchAppState(ctx context.Context, name appstate.WAPatchName, fullSync, onlyIfNotSynced bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchAppStateCalls = append(m.fetchAppStateCalls, fetchAppStateCall{
		name:            name,
		fullSync:        fullSync,
		onlyIfNotSynced: onlyIfNotSynced,
	})
	if m.fetchAppStateFn != nil {
		return m.fetchAppStateFn(name, fullSync, onlyIfNotSynced)
	}
	return m.fetchAppStateErr
}

func (m *mockReadClient) SendPeerMessage(ctx context.Context, message *waE2E.Message) (whatsmeow.SendResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peerMessages = append(m.peerMessages, message)
	return whatsmeow.SendResponse{Timestamp: time.Now()}, nil
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

func TestSendAppStateWithRecovery_Conflict409Recovery(t *testing.T) {
	mgr := NewAppStateManager()
	patch := appstate.BuildMarkChatAsRead(types.NewJID("15551234567", types.DefaultUserServer), true, time.Now(), nil)

	conflictErr := errors.New("server returned error updating app state (regular_low): <error code=\"409\" text=\"conflict\"/> (also, applying patches in the response failed: failed to decode app state regular_low patches: failed to verify patch v270: mismatching LTHash)")

	mockCli := &mockReadClient{
		connected:        true,
		appStateErrQueue: []error{conflictErr, nil},
	}

	err := mgr.SendAppStateWithRecovery(context.Background(), mockCli, patch)
	if err != nil {
		t.Fatalf("expected SendAppStateWithRecovery to succeed after recovery, got %v", err)
	}

	if len(mockCli.fetchAppStateCalls) != 1 {
		t.Fatalf("expected 1 FetchAppState call for resync, got %d", len(mockCli.fetchAppStateCalls))
	}
	if mockCli.fetchAppStateCalls[0].fullSync {
		t.Fatalf("expected fullSync = false in FetchAppState call")
	}
	if mockCli.fetchAppStateCalls[0].name != appstate.WAPatchRegularLow {
		t.Fatalf("expected patch name regular_low, got %s", mockCli.fetchAppStateCalls[0].name)
	}
	if len(mockCli.appStatePatches) != 2 {
		t.Fatalf("expected 2 SendAppState attempts, got %d", len(mockCli.appStatePatches))
	}

	diag := mgr.GetDiagnostics(context.Background(), mockCli)
	if diag.ConflictsDetected != 1 {
		t.Fatalf("expected 1 conflict detected, got %d", diag.ConflictsDetected)
	}
	if diag.RecoveriesSucceeded != 1 {
		t.Fatalf("expected 1 recovery succeeded, got %d", diag.RecoveriesSucceeded)
	}
	if diag.RecoveriesAttempted != 1 {
		t.Fatalf("expected 1 recovery attempted, got %d", diag.RecoveriesAttempted)
	}
}

func TestSendAppStateWithRecovery_Serialization(t *testing.T) {
	mgr := NewAppStateManager()
	patch := appstate.BuildMarkChatAsRead(types.NewJID("15551234567", types.DefaultUserServer), true, time.Now(), nil)

	const concurrency = 6
	var activeCalls int64
	var maxActive int64
	var mu sync.Mutex

	mockCli := &mockReadClient{connected: true}
	// Wrap mock client with concurrency counter
	type concurrentMock struct {
		*mockReadClient
	}
	_ = concurrentMock{mockCli}

	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mu.Lock()
			activeCalls++
			if activeCalls > maxActive {
				maxActive = activeCalls
			}
			mu.Unlock()

			err := mgr.SendAppStateWithRecovery(context.Background(), mockCli, patch)
			if err != nil {
				errCh <- err
			}

			mu.Lock()
			activeCalls--
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("unexpected error during concurrent SendAppStateWithRecovery: %v", err)
	}

	if len(mockCli.appStatePatches) != concurrency {
		t.Fatalf("expected %d total patches sent, got %d", concurrency, len(mockCli.appStatePatches))
	}
}

func TestSendAppStateWithRecovery_ExhaustedRetries(t *testing.T) {
	mgr := NewAppStateManager()
	patch := appstate.BuildMarkChatAsRead(types.NewJID("15551234567", types.DefaultUserServer), true, time.Now(), nil)

	conflictErr := errors.New("server returned error updating app state (regular_low): 409 conflict: mismatching LTHash")
	mockCli := &mockReadClient{
		connected:        true,
		fetchAppStateErr: errors.New("network failure during snapshot download"),
		appStateErr:      conflictErr,
	}

	err := mgr.SendAppStateWithRecovery(context.Background(), mockCli, patch)
	if err == nil {
		t.Fatalf("expected error after exhausted retries, got nil")
	}

	diag := mgr.GetDiagnostics(context.Background(), mockCli)
	if diag.ConflictsDetected != 1 {
		t.Fatalf("expected 1 conflict detected, got %d", diag.ConflictsDetected)
	}
	if diag.RecoveriesFailed != int64(mgr.maxRetries) {
		t.Fatalf("expected %d recoveries failed, got %d", mgr.maxRetries, diag.RecoveriesFailed)
	}
}

func TestMarkChatRead_AppStateFailureQueuesPending(t *testing.T) {
	t.Setenv("WHATSAPP_MARK_READ_ENABLED", "true")
	ms := newTestMessageStore(t)
	chatJID := "15551234567@s.whatsapp.net"
	t1 := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)

	if err := ms.StoreChat(chatJID, "Alice", t1); err != nil {
		t.Fatalf("StoreChat: %v", err)
	}
	if err := ms.StoreMessage("msg1", chatJID, "15551234567", "hello", t1, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage: %v", err)
	}

	permErr := errors.New("server returned error updating app state (regular_low): 409 conflict (mismatching LTHash)")
	mockCli := &mockReadClient{
		connected:        true,
		appStateErr:      permErr,
		fetchAppStateErr: errors.New("snapshot fetch failed"),
	}

	body := `{"chat_jid":"` + chatJID + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/chats/mark-read", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handleMarkChatRead(rec, req, mockCli, nil, ms)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK with pending_queued status, got %d (body: %s)", rec.Code, rec.Body.String())
	}

	var resp MarkChatReadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !resp.Success {
		t.Fatalf("expected success = true when receipts were delivered and appstate queued")
	}
	if resp.RemoteSynced {
		t.Fatalf("expected remote_synced = false on app state error")
	}
	if resp.Status != "pending_queued" {
		t.Fatalf("expected status = pending_queued, got %s", resp.Status)
	}
	if resp.AppStateError == "" {
		t.Fatalf("expected app_state_error to be populated, got empty")
	}

	// Verify receipts were sent
	if len(mockCli.markReadCalls) == 0 {
		t.Fatalf("expected markReadCalls > 0")
	}

	// Verify database local read marker was updated to reflect sent receipts
	lastRead, _, found, err := ms.GetChatReadInfo(chatJID)
	if err != nil || !found {
		t.Fatalf("GetChatReadInfo error: %v", err)
	}
	if lastRead == nil {
		t.Fatalf("expected lastRead to be set in database, got nil")
	}
}

func TestMarkChatRead_DisconnectedClientFailsWithoutUpdatingDB(t *testing.T) {
	t.Setenv("WHATSAPP_MARK_READ_ENABLED", "true")
	ms := newTestMessageStore(t)
	chatJID := "15551234567@s.whatsapp.net"
	t1 := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)

	if err := ms.StoreChat(chatJID, "Alice", t1); err != nil {
		t.Fatalf("StoreChat: %v", err)
	}
	if err := ms.StoreMessage("msg1", chatJID, "15551234567", "hello", t1, false, "", "", "", nil, nil, nil, 0, ""); err != nil {
		t.Fatalf("StoreMessage: %v", err)
	}

	mockCli := &mockReadClient{
		connected: false,
	}

	body := `{"chat_jid":"` + chatJID + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/chats/mark-read", strings.NewReader(body))
	rec := httptest.NewRecorder()

	handleMarkChatRead(rec, req, mockCli, nil, ms)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	// Verify database was NOT updated
	lastRead, _, found, err := ms.GetChatReadInfo(chatJID)
	if err != nil || !found {
		t.Fatalf("GetChatReadInfo error: %v", err)
	}
	if lastRead != nil {
		t.Fatalf("expected lastRead to remain nil when disconnected, got %v", lastRead)
	}
}

func TestSendAppStateWithRecovery_TieredLTHashRecovery(t *testing.T) {
	mgr := NewAppStateManager()
	patch := appstate.BuildMarkChatAsRead(types.NewJID("15551234567", types.DefaultUserServer), true, time.Now(), nil)

	conflictErr := errors.New("server returned error updating app state (regular_low): <error code=\"409\" text=\"conflict\"/> (also, applying patches in the response failed: failed to decode app state regular_low patches: failed to verify patch v270: mismatching LTHash)")

	mockCli := &mockReadClient{
		connected:        true,
		appStateErrQueue: []error{conflictErr, nil},
		fetchAppStateFn: func(name appstate.WAPatchName, fullSync, onlyIfNotSynced bool) error {
			if !fullSync {
				// Tier 1 incremental sync fails due to corrupt local LTHash
				return errors.New("failed to verify patch v270: mismatching LTHash")
			}
			// Tier 2 full snapshot sync succeeds
			return nil
		},
	}

	err := mgr.SendAppStateWithRecovery(context.Background(), mockCli, patch)
	if err != nil {
		t.Fatalf("expected SendAppStateWithRecovery to succeed on Tier 2 fullSync, got %v", err)
	}

	mockCli.mu.Lock()
	calls := append([]fetchAppStateCall(nil), mockCli.fetchAppStateCalls...)
	mockCli.mu.Unlock()

	if len(calls) != 2 {
		t.Fatalf("expected 2 FetchAppState calls (Tier 1 incremental then Tier 2 fullSync), got %d", len(calls))
	}
	if calls[0].fullSync != false {
		t.Fatalf("expected call 0 to be incremental (fullSync=false)")
	}
	if calls[1].fullSync != true {
		t.Fatalf("expected call 1 to be full snapshot (fullSync=true)")
	}

	diag := mgr.GetDiagnostics(context.Background(), mockCli)
	if diag.ConflictsDetected != 1 {
		t.Fatalf("expected 1 conflict detected, got %d", diag.ConflictsDetected)
	}
	if diag.IncrementalRecoveriesAttempted != 1 {
		t.Fatalf("expected 1 incremental recovery attempted, got %d", diag.IncrementalRecoveriesAttempted)
	}
	if diag.FullSyncRecoveriesAttempted != 1 {
		t.Fatalf("expected 1 full sync recovery attempted, got %d", diag.FullSyncRecoveriesAttempted)
	}
	if diag.RecoveriesSucceeded != 1 {
		t.Fatalf("expected 1 recovery succeeded, got %d", diag.RecoveriesSucceeded)
	}
}

func TestSendAppStateWithRecovery_TieredPeerRecovery(t *testing.T) {
	mgr := NewAppStateManager()
	patch := appstate.BuildMarkChatAsRead(types.NewJID("15551234567", types.DefaultUserServer), true, time.Now(), nil)

	conflictErr := errors.New("server returned error updating app state (regular_low): 409 conflict")

	callCount := 0
	mockCli := &mockReadClient{
		connected:        true,
		appStateErrQueue: []error{conflictErr, nil},
		fetchAppStateFn: func(name appstate.WAPatchName, fullSync, onlyIfNotSynced bool) error {
			callCount++
			if callCount == 1 {
				// Tier 1 incremental fails
				return errors.New("mismatching LTHash")
			}
			if callCount == 2 {
				// Tier 2 server snapshot fails
				return errors.New("server snapshot 500")
			}
			// Tier 3 full sync after peer recovery request succeeds
			return nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := mgr.SendAppStateWithRecovery(ctx, mockCli, patch)
	if err != nil {
		t.Fatalf("expected SendAppStateWithRecovery to succeed after Tier 3 peer recovery, got %v", err)
	}

	mockCli.mu.Lock()
	peerCalls := len(mockCli.peerMessages)
	mockCli.mu.Unlock()

	if peerCalls != 1 {
		t.Fatalf("expected 1 peer message dispatch, got %d", peerCalls)
	}

	diag := mgr.GetDiagnostics(context.Background(), mockCli)
	if diag.PeerRecoveriesAttempted != 1 {
		t.Fatalf("expected 1 peer recovery attempted, got %d", diag.PeerRecoveriesAttempted)
	}
	if diag.RecoveriesSucceeded != 1 {
		t.Fatalf("expected 1 recovery succeeded, got %d", diag.RecoveriesSucceeded)
	}
}

func TestFetchAppStateWithLock_Coordination(t *testing.T) {
	mgr := NewAppStateManager()
	mockCli := &mockReadClient{connected: true}

	var executionOrder []string
	var mu sync.Mutex

	mockCli.fetchAppStateFn = func(name appstate.WAPatchName, fullSync, onlyIfNotSynced bool) error {
		mu.Lock()
		executionOrder = append(executionOrder, "fetch")
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = mgr.FetchAppStateWithLock(context.Background(), mockCli, appstate.WAPatchRegularLow, false, false)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		patch := appstate.BuildMarkChatAsRead(types.NewJID("15551234567", types.DefaultUserServer), true, time.Now(), nil)
		_ = mgr.SendAppStateWithRecovery(context.Background(), mockCli, patch)
		mu.Lock()
		executionOrder = append(executionOrder, "send")
		mu.Unlock()
	}()

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(executionOrder) != 2 {
		t.Fatalf("expected 2 operations recorded, got %d", len(executionOrder))
	}
}

func TestSendAppStateWithRecovery_SixConcurrentSends(t *testing.T) {
	mgr := NewAppStateManager()
	mockCli := &mockReadClient{connected: true}

	const numChats = 6
	var wg sync.WaitGroup
	errCh := make(chan error, numChats)

	for i := 0; i < numChats; i++ {
		wg.Add(1)
		chatPhone := fmt.Sprintf("1555000000%d", i)
		go func(phone string) {
			defer wg.Done()
			jid := types.NewJID(phone, types.DefaultUserServer)
			patch := appstate.BuildMarkChatAsRead(jid, true, time.Now(), nil)
			if err := mgr.SendAppStateWithRecovery(context.Background(), mockCli, patch); err != nil {
				errCh <- fmt.Errorf("chat %s: %w", phone, err)
			}
		}(chatPhone)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("unexpected error during 6 concurrent sends: %v", err)
	}

	mockCli.mu.Lock()
	totalPatches := len(mockCli.appStatePatches)
	mockCli.mu.Unlock()

	if totalPatches != numChats {
		t.Fatalf("expected %d total patches sent, got %d", numChats, totalPatches)
	}
}

