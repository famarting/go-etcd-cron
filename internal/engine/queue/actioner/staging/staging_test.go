/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package staging

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_DeliverablePrefixes(t *testing.T) {
	t.Parallel()

	t.Run("registering empty prefixes should add nothing", func(t *testing.T) {
		t.Parallel()

		s := New(Options{})
		assert.Empty(t, s.deliverablePrefixes)

		assert.Empty(t, s.DeliverablePrefixes())
		assert.Empty(t, s.deliverablePrefixes)
		s.UnDeliverablePrefixes()
		assert.Empty(t, s.DeliverablePrefixes())
	})

	t.Run("registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		s := New(Options{})
		assert.Empty(t, s.deliverablePrefixes)

		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("foo")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("abc")
		assert.Empty(t, s.deliverablePrefixes)
	})

	t.Run("multiple: registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		s := New(Options{})
		assert.Empty(t, s.deliverablePrefixes)

		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Len(t, s.deliverablePrefixes, 1)
		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Len(t, s.deliverablePrefixes, 1)

		s.UnDeliverablePrefixes("abc")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("abc")
		assert.Empty(t, s.deliverablePrefixes)
	})

	t.Run("multiple with diff prefixes: registering and cancelling should add then remove the prefix", func(t *testing.T) {
		t.Parallel()

		s := New(Options{})
		assert.Empty(t, s.deliverablePrefixes)

		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Len(t, s.deliverablePrefixes, 1)
		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Len(t, s.deliverablePrefixes, 1)
		assert.Empty(t, s.DeliverablePrefixes("def"))
		assert.Len(t, s.deliverablePrefixes, 2)
		assert.Empty(t, s.DeliverablePrefixes("def"))
		assert.Len(t, s.deliverablePrefixes, 2)

		s.UnDeliverablePrefixes("abc")
		assert.Len(t, s.deliverablePrefixes, 2)
		s.UnDeliverablePrefixes("def")
		assert.Len(t, s.deliverablePrefixes, 2)
		s.UnDeliverablePrefixes("def")
		assert.Len(t, s.deliverablePrefixes, 1)
		s.UnDeliverablePrefixes("abc")
		assert.Empty(t, s.deliverablePrefixes)
	})

	t.Run("staged counters should be enqueued if they match an added prefix", func(t *testing.T) {
		t.Parallel()

		s := New(Options{})
		s.staged = map[int64]string{
			123: "abc123", 234: "abc234",
			124: "def123", 235: "def234",
			125: "xyz123", 236: "xyz234",
		}

		assert.ElementsMatch(t,
			[]int64{123, 234, 125, 236},
			s.DeliverablePrefixes("abc", "xyz"),
		)
		assert.Equal(t,
			map[int64]string{124: "def123", 235: "def234"},
			s.staged,
		)

		assert.ElementsMatch(t,
			[]int64{124, 235},
			s.DeliverablePrefixes("d"),
		)
		assert.Empty(t, s.staged)
	})
}

func Test_Stage(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		id                  int64
		jobName             string
		deliverablePrefixes []string
		expStaged           bool
	}{
		"no deliverable prefixes, should stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{},
			expStaged:           true,
		},
		"deliverable prefixes but different, should stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "cba"},
			expStaged:           true,
		},
		"deliverable prefixes and matches, should not stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{"abc123"},
			expStaged:           false,
		},
		"multiple deliverable prefixes and matches, should not stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "abc123", "cba"},
			expStaged:           false,
		},
		"multiple deliverable prefixes and matches on prefix, should not stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "cba", "abc"},
			expStaged:           false,
		},
		"multiple deliverable prefixes and not matches on prefix, should stage": {
			id:                  123,
			jobName:             "abc123",
			deliverablePrefixes: []string{"def", "cba", "abc1234"},
			expStaged:           true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := New(Options{})
			s.DeliverablePrefixes(test.deliverablePrefixes...)

			got := s.Stage(test.id, test.jobName)

			assert.Equal(t, test.expStaged, got)
			assert.Equal(t, test.expStaged, len(s.staged) == 1)
		})
	}
}

// recorder is a StagingObserver which records every call, and replays them
// into the set of staged names.
type recorder struct {
	events []string
	names  map[string]struct{}
}

func newRecorder() *recorder {
	return &recorder{names: make(map[string]struct{})}
}

func (r *recorder) Staged(name string) {
	r.events = append(r.events, "staged:"+name)
	r.names[name] = struct{}{}
}

func (r *recorder) Unstaged(names []string) {
	for _, name := range names {
		r.events = append(r.events, "unstaged:"+name)
		delete(r.names, name)
	}
}

func (r *recorder) Reset() {
	r.events = append(r.events, "reset")
	clear(r.names)
}

// stagedNames returns the distinct names held in the staging queue.
func stagedNames(s *Staging) map[string]struct{} {
	names := make(map[string]struct{})
	for _, name := range s.staged {
		names[name] = struct{}{}
	}
	return names
}

func Test_Observer(t *testing.T) {
	t.Parallel()

	t.Run("staging a job reports it once", func(t *testing.T) {
		t.Parallel()

		r := newRecorder()
		s := New(Options{Observer: r})

		assert.True(t, s.Stage(1, "abc1"))
		assert.True(t, s.Stage(1, "abc1"))
		assert.True(t, s.Stage(2, "abc2"))
		assert.Equal(t, []string{"staged:abc1", "staged:abc2"}, r.events)
		assert.Equal(t, stagedNames(s), r.names)
	})

	t.Run("a job which is deliverable when staged is not reported", func(t *testing.T) {
		t.Parallel()

		r := newRecorder()
		s := New(Options{Observer: r})

		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.False(t, s.Stage(1, "abc1"))
		assert.Empty(t, r.events)
		assert.Empty(t, s.staged)
	})

	t.Run("releasing a prefix reports the matching names as unstaged", func(t *testing.T) {
		t.Parallel()

		r := newRecorder()
		s := New(Options{Observer: r})

		s.Stage(1, "abc1")
		s.Stage(2, "abc2")
		s.Stage(3, "def1")
		r.events = nil

		assert.ElementsMatch(t, []int64{1, 2}, s.DeliverablePrefixes("abc", "xyz"))
		assert.ElementsMatch(t, []string{"unstaged:abc1", "unstaged:abc2"}, r.events)
		assert.Equal(t, map[string]struct{}{"def1": {}}, r.names)
		assert.Equal(t, stagedNames(s), r.names)

		// Registering the same prefix again releases nothing new.
		r.events = nil
		assert.Empty(t, s.DeliverablePrefixes("abc"))
		assert.Empty(t, r.events)
	})

	t.Run("unstaging a job reports it, and unstaging an unknown job does not", func(t *testing.T) {
		t.Parallel()

		r := newRecorder()
		s := New(Options{Observer: r})

		s.Stage(1, "abc1")
		r.events = nil

		s.Unstage(2)
		assert.Empty(t, r.events)

		s.Unstage(1)
		assert.Equal(t, []string{"unstaged:abc1"}, r.events)
		assert.Empty(t, r.names)

		s.Unstage(1)
		assert.Equal(t, []string{"unstaged:abc1"}, r.events)
	})

	t.Run("a name staged under two versions is reported once, and unstaged with its last version", func(t *testing.T) {
		t.Parallel()

		r := newRecorder()
		s := New(Options{Observer: r})

		s.Stage(1, "abc1")
		s.Stage(2, "abc1")
		assert.Equal(t, []string{"staged:abc1"}, r.events)

		s.Unstage(1)
		assert.Equal(t, []string{"staged:abc1"}, r.events)
		assert.Equal(t, stagedNames(s), r.names)

		s.Unstage(2)
		assert.Equal(t, []string{"staged:abc1", "unstaged:abc1"}, r.events)
		assert.Empty(t, r.names)
	})

	t.Run("a name staged under two versions released together is reported once", func(t *testing.T) {
		t.Parallel()

		r := newRecorder()
		s := New(Options{Observer: r})

		s.Stage(1, "abc1")
		s.Stage(2, "abc1")
		r.events = nil

		assert.ElementsMatch(t, []int64{1, 2}, s.DeliverablePrefixes("abc"))
		assert.Equal(t, []string{"unstaged:abc1"}, r.events)
	})

	t.Run("close drops the staged set, reports a reset once, and reports nothing after", func(t *testing.T) {
		t.Parallel()

		r := newRecorder()
		s := New(Options{Observer: r})

		s.Stage(1, "abc1")
		s.Stage(2, "def1")
		r.events = nil

		s.Close()
		assert.Equal(t, []string{"reset"}, r.events)
		assert.Empty(t, s.staged)
		assert.Empty(t, r.names)

		s.Close()
		s.Stage(3, "abc3")
		s.DeliverablePrefixes("abc")
		s.Unstage(3)
		assert.Equal(t, []string{"reset"}, r.events)
	})

	t.Run("no observer should still stage, release and close", func(t *testing.T) {
		t.Parallel()

		s := New(Options{})
		assert.True(t, s.Stage(1, "abc1"))
		assert.True(t, s.Stage(2, "abc1"))
		s.Unstage(1)
		assert.Equal(t, []int64{2}, s.DeliverablePrefixes("abc"))
		assert.Nil(t, s.stagedNames)
		s.Close()
		assert.Empty(t, s.staged)
	})
}
