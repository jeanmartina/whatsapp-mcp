package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
)

// AppStateDiagnostics provides observable metrics about AppState mutations and conflict recoveries.
type AppStateDiagnostics struct {
	Collection                     string  `json:"collection"`
	Version                        uint64  `json:"version"`
	ConflictsDetected              int64   `json:"conflicts_detected"`
	RecoveriesAttempted            int64   `json:"recoveries_attempted"`
	IncrementalRecoveriesAttempted int64   `json:"incremental_recoveries_attempted"`
	FullSyncRecoveriesAttempted    int64   `json:"full_sync_recoveries_attempted"`
	PeerRecoveriesAttempted        int64   `json:"peer_recoveries_attempted"`
	RecoveriesSucceeded            int64   `json:"recoveries_succeeded"`
	RecoveriesFailed               int64   `json:"recoveries_failed"`
	LastRecoveryTime               *string `json:"last_recovery_time,omitempty"`
	LastConflictTime               *string `json:"last_conflict_time,omitempty"`
	LastError                      string  `json:"last_error,omitempty"`
}

// AppStateManager serializes AppState mutations per session/client and manages
// safe cryptographic recovery from 409 conflict and LTHash divergence through
// tiered escalation: incremental -> server snapshot full sync -> companion peer recovery.
type AppStateManager struct {
	mu          sync.Mutex
	diagnostics AppStateDiagnostics
	maxRetries  int
}

var globalAppStateManager = NewAppStateManager()

// NewAppStateManager creates a new initialized AppStateManager.
func NewAppStateManager() *AppStateManager {
	return &AppStateManager{
		diagnostics: AppStateDiagnostics{
			Collection: string(appstate.WAPatchRegularLow),
		},
		maxRetries: 3,
	}
}

// ResetDiagnostics resets diagnostic counters (used in testing).
func (m *AppStateManager) ResetDiagnostics() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.diagnostics = AppStateDiagnostics{
		Collection: string(appstate.WAPatchRegularLow),
	}
}

// GetDiagnostics returns a snapshot of current AppState synchronization metrics.
func (m *AppStateManager) GetDiagnostics(ctx context.Context, cli any) AppStateDiagnostics {
	m.mu.Lock()
	defer m.mu.Unlock()

	diag := m.diagnostics
	if c, ok := cli.(*whatsmeow.Client); ok && c != nil && c.Store != nil && c.Store.AppState != nil {
		v, _, err := c.Store.AppState.GetAppStateVersion(ctx, string(appstate.WAPatchRegularLow))
		if err == nil {
			diag.Version = v
		}
	}
	return diag
}

// isAppStateConflict checks whether an error returned by SendAppState corresponds
// to a version conflict or LTHash mismatch that requires snapshot resynchronization.
func isAppStateConflict(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "409") ||
		strings.Contains(errStr, "conflict") ||
		strings.Contains(errStr, "lthash") ||
		strings.Contains(errStr, "mismatching") ||
		strings.Contains(errStr, "hash mismatch") ||
		strings.Contains(errStr, "out of sync") ||
		errors.Is(err, whatsmeow.ErrAppStateUpdate)
}

// FetchAppStateWithLock serializes background or on-demand FetchAppState calls with AppState mutations.
func (m *AppStateManager) FetchAppStateWithLock(
	ctx context.Context,
	cli whatsAppReadClient,
	name appstate.WAPatchName,
	fullSync, onlyIfNotSynced bool,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cli.FetchAppState(ctx, name, fullSync, onlyIfNotSynced)
}

// RecoverAppState performs an explicit on-demand recovery for a collection.
func (m *AppStateManager) RecoverAppState(
	ctx context.Context,
	cli whatsAppReadClient,
	collection appstate.WAPatchName,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.diagnostics.RecoveriesAttempted++
	m.diagnostics.FullSyncRecoveriesAttempted++

	// Tier 1: Request clean server snapshot via fullSync=true (resets divergent local version & LTHash)
	syncErr := cli.FetchAppState(ctx, collection, true, false)
	if syncErr == nil {
		recTime := time.Now().UTC().Format(time.RFC3339)
		m.diagnostics.LastRecoveryTime = &recTime
		m.diagnostics.RecoveriesSucceeded++
		m.diagnostics.LastError = ""
		return nil
	}

	m.diagnostics.LastError = fmt.Sprintf("RecoverAppState server fullSync failed: %v", syncErr)

	// Tier 2: Request companion snapshot fatal recovery from primary device
	m.diagnostics.PeerRecoveriesAttempted++
	req := whatsmeow.BuildAppStateRecoveryRequest(collection)
	_, peerErr := cli.SendPeerMessage(ctx, req)
	if peerErr != nil {
		m.diagnostics.RecoveriesFailed++
		return fmt.Errorf("fullSync failed (%v) and SendPeerMessage failed: %w", syncErr, peerErr)
	}

	return nil
}

// SendAppStateWithRecovery serializes all AppState mutations and performs
// automatic, tiered, non-destructive snapshot resynchronization when conflicts or LTHash mismatches occur:
//   - Tier 1: Incremental sync (fetches missing intermediate patches from server)
//   - Tier 2: Server Snapshot Full Sync (clears divergent local version and rebuilds state from server snapshot)
//   - Tier 3: Companion Peer Recovery (requests cryptographic recovery from primary device)
func (m *AppStateManager) SendAppStateWithRecovery(
	ctx context.Context,
	cli whatsAppReadClient,
	patch appstate.PatchInfo,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := cli.SendAppState(ctx, patch)
	if err == nil {
		m.diagnostics.LastError = ""
		return nil
	}

	if !isAppStateConflict(err) {
		m.diagnostics.LastError = err.Error()
		return err
	}

	m.diagnostics.ConflictsDetected++
	nowStr := time.Now().UTC().Format(time.RFC3339)
	m.diagnostics.LastConflictTime = &nowStr
	m.diagnostics.LastError = err.Error()

	fmt.Printf("[AppState] Conflict/LTHash mismatch detected for %s: %v. Initiating tiered snapshot recovery...\n", patch.Type, err)

	for attempt := 1; attempt <= m.maxRetries; attempt++ {
		m.diagnostics.RecoveriesAttempted++

		var resyncErr error

		switch attempt {
		case 1:
			// Tier 1: Try incremental sync first to catch up on any missing remote patches
			m.diagnostics.IncrementalRecoveriesAttempted++
			fmt.Printf("[AppState] Tier 1: Attempting incremental sync for %s (attempt %d)...\n", patch.Type, attempt)
			resyncErr = cli.FetchAppState(ctx, patch.Type, false, false)

		case 2:
			// Tier 2: Escalated full snapshot resync (resets corrupt/divergent local LTHash and requests full snapshot from WhatsApp servers)
			m.diagnostics.FullSyncRecoveriesAttempted++
			fmt.Printf("[AppState] Tier 2: Attempting server full snapshot resync for %s (attempt %d)...\n", patch.Type, attempt)
			resyncErr = cli.FetchAppState(ctx, patch.Type, true, false)

		default:
			// Tier 3: Companion peer recovery request to primary device
			m.diagnostics.PeerRecoveriesAttempted++
			fmt.Printf("[AppState] Tier 3: Requesting companion snapshot recovery from primary device for %s (attempt %d)...\n", patch.Type, attempt)
			peerReq := whatsmeow.BuildAppStateRecoveryRequest(patch.Type)
			_, peerErr := cli.SendPeerMessage(ctx, peerReq)
			if peerErr != nil {
				fmt.Printf("[AppState] Failed to dispatch peer recovery request: %v\n", peerErr)
			} else {
				fmt.Printf("[AppState] Successfully dispatched peer recovery request to primary phone for %s\n", patch.Type)
			}

			// Wait briefly for peer response
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1500 * time.Millisecond):
			}

			// Try full sync again after peer recovery message
			resyncErr = cli.FetchAppState(ctx, patch.Type, true, false)
		}

		if resyncErr != nil {
			m.diagnostics.RecoveriesFailed++
			m.diagnostics.LastError = fmt.Sprintf("FetchAppState failed (attempt %d): %v", attempt, resyncErr)
			fmt.Printf("[AppState] FetchAppState failed on attempt %d: %v\n", attempt, resyncErr)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*250) * time.Millisecond):
			}
			continue
		}

		m.diagnostics.RecoveriesSucceeded++
		recTime := time.Now().UTC().Format(time.RFC3339)
		m.diagnostics.LastRecoveryTime = &recTime
		fmt.Printf("[AppState] Snapshot resync succeeded on attempt %d. Retrying patch send...\n", attempt)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt*100) * time.Millisecond):
		}

		retryErr := cli.SendAppState(ctx, patch)
		if retryErr == nil {
			fmt.Printf("[AppState] Patch successfully synchronized after recovery on attempt %d\n", attempt)
			m.diagnostics.LastError = ""
			return nil
		}

		if !isAppStateConflict(retryErr) {
			m.diagnostics.LastError = retryErr.Error()
			return retryErr
		}
		m.diagnostics.LastError = retryErr.Error()
	}

	return fmt.Errorf("failed to sync app state after %d recovery attempts: %w", m.maxRetries, err)
}
