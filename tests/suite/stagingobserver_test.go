/*
Copyright (c) 2026 Diagrid Inc.
Licensed under the MIT License.
*/

package suite

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/kit/ptr"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
	"github.com/diagridio/go-etcd-cron/tests/framework/etcd"
)

// stagingRecorder replays StagingObserver calls into a set of names.
type stagingRecorder struct {
	lock   sync.Mutex
	names  map[string]struct{}
	calls  map[string]int
	resets int
}

func newStagingRecorder() *stagingRecorder {
	return &stagingRecorder{
		names: make(map[string]struct{}),
		calls: make(map[string]int),
	}
}

func (s *stagingRecorder) Staged(name string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.names[name] = struct{}{}
	s.calls[name]++
}

func (s *stagingRecorder) Unstaged(names []string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, name := range names {
		delete(s.names, name)
	}
}

func (s *stagingRecorder) Reset() {
	s.lock.Lock()
	defer s.lock.Unlock()
	clear(s.names)
	s.resets++
}

func (s *stagingRecorder) staged() []string {
	s.lock.Lock()
	defer s.lock.Unlock()
	names := make([]string, 0, len(s.names))
	for name := range s.names {
		names = append(names, name)
	}
	return names
}

func (s *stagingRecorder) stagedCalls(name string) int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.calls[name]
}

func (s *stagingRecorder) resetCount() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.resets
}

func Test_StagingObserver(t *testing.T) {
	t.Parallel()

	client := etcd.EmbeddedBareClient(t)

	// Jobs under "a||" succeed once delivered is set, everything else is
	// undeliverable.
	var delivered atomic.Bool
	triggerFn := func(req *api.TriggerRequest, fn func(*api.TriggerResponse)) {
		if delivered.Load() && strings.HasPrefix(req.GetName(), "a||") {
			fn(&api.TriggerResponse{Result: api.TriggerResponseResult_SUCCESS})
			return
		}
		fn(&api.TriggerResponse{Result: api.TriggerResponseResult_UNDELIVERABLE})
	}

	newCron := func(id string, obs api.StagingObserver) (api.Interface, func()) {
		cr, err := cron.New(cron.Options{
			Log:             logr.Discard(),
			Client:          client,
			Namespace:       "abc",
			ID:              id,
			TriggerFn:       triggerFn,
			StagingObserver: obs,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() { errCh <- cr.Run(ctx) }()

		var once sync.Once
		stop := func() {
			once.Do(func() {
				cancel()
				select {
				case err := <-errCh:
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Error("timeout waiting for cron to stop")
				}
			})
		}
		t.Cleanup(stop)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.True(c, cr.IsElected())
		}, time.Second*10, time.Millisecond*10)

		return cr, stop
	}

	rec0 := newStagingRecorder()
	cr0, stop0 := newCron("0", rec0)

	job := &api.Job{
		Schedule: ptr.Of("@every 1h"),
		DueTime:  ptr.Of(time.Now().Format(time.RFC3339)),
	}

	names := make([]string, 0, 10)
	for i := range 5 {
		names = append(names, "a||"+strconv.Itoa(i), "b||"+strconv.Itoa(i))
	}
	for _, name := range names {
		require.NoError(t, cr0.Add(t.Context(), name, job))
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ElementsMatch(c, names, rec0.staged())
	}, time.Second*10, time.Millisecond*10)

	// Deleting a staged job unstages it.
	require.NoError(t, cr0.Delete(t.Context(), "a||0"))
	names = names[1:]
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ElementsMatch(c, names, rec0.staged())
	}, time.Second*10, time.Millisecond*10)

	// Re-putting a staged job unstages its old version, and the new version
	// is staged again when it triggers.
	require.Equal(t, 1, rec0.stagedCalls("b||0"))
	require.NoError(t, cr0.Add(t.Context(), "b||0", job))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 2, rec0.stagedCalls("b||0"))
		assert.ElementsMatch(c, names, rec0.staged())
	}, time.Second*10, time.Millisecond*10)

	// Registering a deliverable prefix releases the matching jobs, which now
	// trigger successfully and are scheduled for their next tick.
	delivered.Store(true)
	cancel, err := cr0.DeliverablePrefixes(t.Context(), "a||")
	require.NoError(t, err)
	t.Cleanup(func() { cancel(nil) })

	var remaining []string
	for _, name := range names {
		if !strings.HasPrefix(name, "a||") {
			remaining = append(remaining, name)
		}
	}

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ElementsMatch(c, remaining, rec0.staged())
	}, time.Second*10, time.Millisecond*10)
	require.Equal(t, 0, rec0.resetCount())

	// Scaling up rebalances the partitions. The first replica's engine is
	// rebuilt, its staged set is reset, and each replica then stages the jobs
	// of its own partition. Across both, the staged set is the same.
	rec1 := newStagingRecorder()
	_, stop1 := newCron("1", rec1)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, rec0.resetCount(), 1)
		assert.ElementsMatch(c, remaining, append(rec0.staged(), rec1.staged()...))
		assert.NotEmpty(c, rec1.staged())
	}, time.Second*20, time.Millisecond*10)

	// Final shutdown resets the staged set.
	resets := rec1.resetCount()
	stop1()
	assert.Empty(t, rec1.staged())
	assert.Equal(t, resets+1, rec1.resetCount())

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ElementsMatch(c, remaining, rec0.staged())
	}, time.Second*20, time.Millisecond*10)

	stop0()
	assert.Empty(t, rec0.staged())
}
