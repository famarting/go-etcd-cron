/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package staging

import (
	"strings"
	"sync"

	"github.com/diagridio/go-etcd-cron/api"
)

// Staging handles the staging queue for the Job scheduler. It tracks the
// currently deliverable job name prefixes and the counters that have been
// staged for later triggering. The consumer can signal that a job name
// prefix is deliverable, and the staging queue will enqueue all counters that
// match that prefix.
type Staging struct {
	// consumer has signalled that the job is currently undeliverable. When the
	// consumer signals a prefix has become deliverable, counters in that prefix
	// will be enqueued. Indexed by the Job Name.
	staged map[int64]string

	// activeConsumerPrefixes tracks the job name prefixes which are currently
	// deliverable. Since consumer may indicate the same prefix is deliverable
	// multiple times due to pooling, we track the length, and remove the prefix
	// when the length is 0. Indexed by the prefix strings.
	deliverablePrefixes map[string]*uint64

	// prefixLengths tracks the count of distinct registered prefixes for each
	// prefix length. Stage() iterates this set rather than the full prefix map
	// and probes deliverablePrefixes with jobName[:length] for each length.
	prefixLengths map[int]int

	// observer, if set, is told about every change to the set of staged Job
	// names. stagedNames counts the staged entries per Job name, since more
	// than one version of a Job may be staged at once, so that the observer
	// sees each name once. Only maintained when observer is set.
	observer    api.StagingObserver
	stagedNames map[string]int

	// closed is set once the staged set has been dropped. No further changes
	// are reported to the observer after that.
	closed bool

	lock sync.Mutex
}

type Options struct {
	// Observer is an optional observer of the staged Job names.
	Observer api.StagingObserver
}

func New(opts Options) *Staging {
	s := &Staging{
		deliverablePrefixes: make(map[string]*uint64),
		staged:              make(map[int64]string),
		prefixLengths:       make(map[int]int),
		observer:            opts.Observer,
	}
	if s.observer != nil {
		s.stagedNames = make(map[string]int)
	}
	return s
}

// DeliverablePrefixes adds the job name prefixes that can currently be
// delivered by the consumer. When the returned `CancelCauseFunc` is called,
// the prefixes registered are released indicating that these prefixes can no
// longer be delivered. Multiple of the same prefix can be added and are
// tracked as a pool, meaning the prefix is still active if at least one
// instance is still registered.
func (s *Staging) DeliverablePrefixes(prefixes ...string) []int64 {
	s.lock.Lock()
	defer s.lock.Unlock()

	var toEnqueue []int64
	var unstaged []string
	for _, prefix := range prefixes {
		if _, ok := s.deliverablePrefixes[prefix]; !ok {
			s.deliverablePrefixes[prefix] = new(uint64)
			s.prefixLengths[len(prefix)]++

			for modRevision, jobName := range s.staged {
				if strings.HasPrefix(jobName, prefix) {
					delete(s.staged, modRevision)
					toEnqueue = append(toEnqueue, modRevision)
					if s.forgetName(jobName) {
						unstaged = append(unstaged, jobName)
					}
				}
			}
		}

		(*s.deliverablePrefixes[prefix])++
	}

	if len(unstaged) > 0 && !s.closed {
		s.observer.Unstaged(unstaged)
	}

	return toEnqueue
}

// UnDeliverablePrefixes removes the job name prefixes that can no longer be
// delivered by the job handler.
func (s *Staging) UnDeliverablePrefixes(prefixes ...string) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, prefix := range prefixes {
		if i, ok := s.deliverablePrefixes[prefix]; ok {
			// If the number of active consumers drops to 0, remove the prefix from
			// the deliverable prefixes set.
			(*i)--
			if *i <= 0 {
				delete(s.deliverablePrefixes, prefix)
				s.prefixLengths[len(prefix)]--
				if s.prefixLengths[len(prefix)] == 0 {
					delete(s.prefixLengths, len(prefix))
				}
			}
		}
	}
}

// hasMatchingPrefix reports whether any registered deliverable prefix is a
// prefix of jobName. It probes deliverablePrefixes with jobName[:length] for
// every distinct registered prefix length, giving O(L) lookups where L is
// the number of distinct prefix lengths currently registered.
func (s *Staging) hasMatchingPrefix(jobName string) bool {
	for length := range s.prefixLengths {
		if length > len(jobName) {
			continue
		}
		if _, ok := s.deliverablePrefixes[jobName[:length]]; ok {
			return true
		}
	}

	return false
}

// Stage adds the counter (job) to the staging queue. Accounting for control
// loop ordering returns false if the counter can actually be delivered now
// based on the current deliverable prefixes and should be immediately
// re-queued at the current count.
func (s *Staging) Stage(modRevision int64, jobName string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Check if the job is actually now deliverable.
	if s.hasMatchingPrefix(jobName) {
		return false
	}

	if _, ok := s.staged[modRevision]; ok {
		return true
	}

	s.staged[modRevision] = jobName

	if s.observer != nil {
		s.stagedNames[jobName]++
		if s.stagedNames[jobName] == 1 && !s.closed {
			s.observer.Staged(jobName)
		}
	}

	return true
}

// Unstage removes the job from the staging queue as it can now be delivered.
func (s *Staging) Unstage(modRevision int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	jobName, ok := s.staged[modRevision]
	if !ok {
		return
	}

	delete(s.staged, modRevision)

	if s.forgetName(jobName) && !s.closed {
		s.observer.Unstaged([]string{jobName})
	}
}

// Close drops the staged set, reporting the reset to the observer. Called
// when the engine which owns this staging queue shuts down. No further
// changes are reported to the observer after Close.
func (s *Staging) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.closed {
		return
	}

	s.closed = true
	clear(s.staged)
	clear(s.stagedNames)

	if s.observer != nil {
		s.observer.Reset()
	}
}

// forgetName decrements the staged entry count of the given Job name, and
// reports whether the name is no longer staged at all. Always returns false
// when there is no observer.
func (s *Staging) forgetName(jobName string) bool {
	if s.observer == nil {
		return false
	}

	s.stagedNames[jobName]--
	if s.stagedNames[jobName] > 0 {
		return false
	}

	delete(s.stagedNames, jobName)
	return true
}
