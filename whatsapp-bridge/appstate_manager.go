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
	Collection          string  `json:"collection"`
	Version             uint64  `json:"version"`
	ConflictsDetected   int64   `json:"conflicts_detected"`
	RecoveriesAttempted int64   `json:"recoveries_attempted"`
	RecoveriesSucceeded int64   `json:"recoveries_succeeded"`
	RecoveriesFailed    int64   `json:"recoveries_failed"`
	LastRecoveryTime    *string `json:"last_recovery_time,omitempty"`
	LastConflictTime    *string `json:"last_conflict_time,omitempty"`
	LastError           string  `json:"last_error,omitempty"`
}

// AppStateManager serializes AppState mutations per session/client and manages
// safe cryptographic recovery from 409 conflict and LTHash divergence.
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
		errors.Is(err, whatsmeow.ErrAppStateUpdate)
}

// SendAppStateWithRecovery serializes all AppState mutations and performs
// automatic, non-destructive snapshot resynchronization when conflicts or LTHash mismatches occur.
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

	fmt.Printf("[AppState] Conflict/LTHash mismatch detected for %s: %v. Initiating snapshot resync...\n", patch.Type, err)

	for attempt := 1; attempt <= m.maxRetries; attempt++ {
		m.diagnostics.RecoveriesAttempted++

		// Trigger official incremental snapshot resync without destroying session, wiping versions, or disabling crypto checks
		resyncErr := cli.FetchAppState(ctx, patch.Type, false, false)
		if resyncErr != nil {
			m.diagnostics.RecoveriesFailed++
			m.diagnostics.LastError = fmt.Sprintf("FetchAppState failed (attempt %d): %v", attempt, resyncErr)
			fmt.Printf("[AppState] FetchAppState failed on attempt %d: %v\n", attempt, resyncErr)

			// Step 2 of official recovery: request companion snapshot recovery from primary phone
			fmt.Printf("[AppState] Requesting companion snapshot recovery from primary device for %s...\n", patch.Type)
			peerReq := whatsmeow.BuildAppStateRecoveryRequest(patch.Type)
			_, peerErr := cli.SendPeerMessage(ctx, peerReq)
			if peerErr != nil {
				fmt.Printf("[AppState] Failed to dispatch peer recovery request: %v\n", peerErr)
			} else {
				fmt.Printf("[AppState] Successfully dispatched peer recovery request to primary phone for %s\n", patch.Type)
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*300) * time.Millisecond):
			}
			continue
		}

		m.diagnostics.RecoveriesSucceeded++
		recTime := time.Now().UTC().Format(time.RFC3339)
		m.diagnostics.LastRecoveryTime = &recTime
		fmt.Printf("[AppState] Full snapshot resync succeeded on attempt %d. Retrying patch send...\n", attempt)

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
