/*
Copyright (c) 2026 Diagrid Inc.
Licensed under the MIT License.
*/

package api

// StagingObserver observes the set of Job names which are currently staged
// on this replica. A Job is staged when its trigger returned
// TriggerResponseResult_UNDELIVERABLE, and stays staged until a matching
// prefix is registered with DeliverablePrefixes, the Job is updated or
// deleted, or the replica stops owning it.
//
// Replaying the calls in order always yields exactly the set of Job names
// the replica holds in staging: Staged adds a name, Unstaged removes names,
// and Reset empties the set. A name is reported once while staged, however
// many versions of the Job are parked at the same time.
//
// Methods are called synchronously while the staging lock is held, so calls
// are strictly ordered but every implementation must be fast and must not
// block. Implementations must not call back into the cron instance, or the
// engine will deadlock.
type StagingObserver interface {
	// Staged is called when the named Job is parked in staging because it was
	// undeliverable.
	Staged(name string)

	// Unstaged is called with the Job names which are no longer staged,
	// either because a matching prefix became deliverable, or the Job was
	// updated or deleted. The slice must not be retained after the call.
	Unstaged(names []string)

	// Reset is called when the whole staged set has been dropped, which
	// happens whenever the engine shuts down: on leadership rebalance (after
	// which the engine starts again with an empty set) and on final shutdown.
	Reset()
}
