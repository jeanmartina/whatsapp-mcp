package main

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func setupTestStore(t *testing.T) (*MessageStore, func()) {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open memory db: %v", err)
	}

	// Create chats table which chat_list_items references
	_, err = db.Exec(`
		CREATE TABLE chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP,
			last_read_time TIMESTAMP
		);
		CREATE TABLE messages (
			id TEXT PRIMARY KEY,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN
		);
	`)
	if err != nil {
		t.Fatalf("failed to create base tables: %v", err)
	}

	if err := ensureMessageStoreSchema(db); err != nil {
		t.Fatalf("ensureMessageStoreSchema failed: %v", err)
	}

	store := &MessageStore{db: db}
	cleanup := func() {
		_ = db.Close()
	}
	return store, cleanup
}

func TestStoreLabelAndAssociations(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	// 1. Store label
	labelID := "1"
	labelName := "Para responder"
	now := time.Now()
	err := store.StoreLabel(labelID, labelName, 5, "CUSTOM", false, now)
	if err != nil {
		t.Fatalf("StoreLabel failed: %v", err)
	}

	// Verify label inserted
	var name, listType string
	var deleted bool
	err = store.db.QueryRow("SELECT name, type, deleted FROM chat_lists WHERE id = ?", labelID).Scan(&name, &listType, &deleted)
	if err != nil {
		t.Fatalf("QueryRow chat_lists failed: %v", err)
	}
	if name != labelName || listType != "CUSTOM" || deleted {
		t.Fatalf("unexpected label row: name=%s, type=%s, deleted=%v", name, listType, deleted)
	}

	// Insert test chat
	chatJID := "554899999999@s.whatsapp.net"
	_, err = store.db.Exec("INSERT INTO chats (jid, name) VALUES (?, ?)", chatJID, "Contato Teste")
	if err != nil {
		t.Fatalf("failed to insert chat: %v", err)
	}

	// 2. Associate chat with label
	err = store.StoreChatLabelAssociation(chatJID, labelID, true, now)
	if err != nil {
		t.Fatalf("StoreChatLabelAssociation (true) failed: %v", err)
	}

	var count int
	err = store.db.QueryRow("SELECT COUNT(*) FROM chat_list_items WHERE list_id = ? AND chat_jid = ?", labelID, chatJID).Scan(&count)
	if err != nil || count != 1 {
		t.Fatalf("expected 1 association, got count=%d, err=%v", count, err)
	}

	// 3. Disassociate chat from label
	err = store.StoreChatLabelAssociation(chatJID, labelID, false, now)
	if err != nil {
		t.Fatalf("StoreChatLabelAssociation (false) failed: %v", err)
	}

	err = store.db.QueryRow("SELECT COUNT(*) FROM chat_list_items WHERE list_id = ? AND chat_jid = ?", labelID, chatJID).Scan(&count)
	if err != nil || count != 0 {
		t.Fatalf("expected 0 associations after removal, got count=%d, err=%v", count, err)
	}
}
