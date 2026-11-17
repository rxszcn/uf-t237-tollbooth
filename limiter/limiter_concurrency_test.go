package limiter

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// The numbers are deliberately modest so the test stays cheap, but each round
// releases every worker at the same instant, which is what makes the
// lookup-or-create window of a rule actually race.
const (
	concurrentRounds           = 40
	concurrentWorkers          = 16
	concurrentEntriesPerWorker = 32
)

// runConcurrentRegistration fans out one registration per worker, all released
// simultaneously, so the lookup-or-create window of a rule actually races.
func runConcurrentRegistration(register func(entries []string), idPrefix string) {
	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(concurrentWorkers)

	for worker := 0; worker < concurrentWorkers; worker++ {
		go func(worker int) {
			defer wg.Done()

			entries := make([]string, 0, concurrentEntriesPerWorker)
			for i := 0; i < concurrentEntriesPerWorker; i++ {
				entries = append(entries, fmt.Sprintf("%s-worker-%d-entry-%d", idPrefix, worker, i))
			}

			<-start
			register(entries)
		}(worker)
	}

	close(start)
	wg.Wait()
}

// countUnique returns the number of distinct registered entries.
func countUnique(entries []string) int {
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		seen[entry] = struct{}{}
	}
	return len(seen)
}

// TestSetHeaderConcurrentRegistrationLosesNoEntries is a statistical test for the
// "register a header rule from inside the handler, per request, from many workers
// warming up at the same time" wiring. Every registration must survive: the old
// read-then-publish implementation lost whole batches because the last writer
// assigned its freshly built cache object over the earlier one.
func TestSetHeaderConcurrentRegistrationLosesNoEntries(t *testing.T) {
	const header = "X-Customer-ID"
	wantPerRound := concurrentWorkers * concurrentEntriesPerWorker
	wantTotal := wantPerRound * concurrentRounds

	lost := 0

	for round := 0; round < concurrentRounds; round++ {
		lmt := New(nil).SetMax(1)
		prefix := fmt.Sprintf("header-round-%d", round)

		runConcurrentRegistration(func(entries []string) {
			lmt.SetHeader(header, entries)
		}, prefix)

		got := countUnique(lmt.GetHeader(header))
		if got > wantPerRound {
			t.Fatalf("unexpected duplicate registrations in round %d: got %d distinct entries, want at most %d", round, got, wantPerRound)
		}
		lost += wantPerRound - got
	}

	if lost != 0 {
		t.Fatalf("concurrent SetHeader lost %d of %d entries over %d rounds", lost, wantTotal, concurrentRounds)
	}
}

// TestSetContextValueConcurrentRegistrationLosesNoEntries covers the same
// registration window for context-value rules, which shared the identical
// read-then-assign implementation.
func TestSetContextValueConcurrentRegistrationLosesNoEntries(t *testing.T) {
	const contextKey = "API-access-level"
	wantPerRound := concurrentWorkers * concurrentEntriesPerWorker
	wantTotal := wantPerRound * concurrentRounds

	lost := 0

	for round := 0; round < concurrentRounds; round++ {
		lmt := New(nil).SetMax(1)
		prefix := fmt.Sprintf("context-round-%d", round)

		runConcurrentRegistration(func(entries []string) {
			lmt.SetContextValue(contextKey, entries)
		}, prefix)

		got := countUnique(lmt.GetContextValue(contextKey))
		if got > wantPerRound {
			t.Fatalf("unexpected duplicate registrations in round %d: got %d distinct entries, want at most %d", round, got, wantPerRound)
		}
		lost += wantPerRound - got
	}

	if lost != 0 {
		t.Fatalf("concurrent SetContextValue lost %d of %d entries over %d rounds", lost, wantTotal, concurrentRounds)
	}
}

// TestSetHeaderConcurrentRegistrationWithReadersIsConsistent exercises the
// registration window while other goroutines read the rule. The rule starts
// empty on purpose: the lost-update window opens exactly when several callers
// find no rule yet and each builds its own cache object. The observed entry
// count must never decrease, and no registration may be lost.
func TestSetHeaderConcurrentRegistrationWithReadersIsConsistent(t *testing.T) {
	const header = "X-Customer-ID"

	lmt := New(nil).SetMax(1)

	start := make(chan struct{})
	done := make(chan struct{})

	shrunk := make(chan string, 1)

	// Reader: sample the visible rule until the registrations are done. An
	// additive merge can only grow the view, so a decrease reports a wholesale
	// replacement.
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()

		previous := 0
		for {
			select {
			case <-done:
				return
			default:
			}

			current := countUnique(lmt.GetHeader(header))
			if current < previous {
				select {
				case shrunk <- fmt.Sprintf("rule shrank from %d to %d entries while registrations were in flight", previous, current):
				default:
				}
				return
			}
			previous = current
			runtime.Gosched()
		}
	}()

	var wg sync.WaitGroup
	wg.Add(concurrentWorkers)

	for worker := 0; worker < concurrentWorkers; worker++ {
		go func(worker int) {
			defer wg.Done()

			entries := make([]string, 0, concurrentEntriesPerWorker)
			for i := 0; i < concurrentEntriesPerWorker; i++ {
				entries = append(entries, fmt.Sprintf("worker-%d-entry-%d", worker, i))
			}

			<-start
			lmt.SetHeader(header, entries)
		}(worker)
	}

	close(start)
	wg.Wait()
	close(done)
	readers.Wait()

	select {
	case msg := <-shrunk:
		t.Error(msg)
	default:
	}

	want := concurrentWorkers * concurrentEntriesPerWorker
	if got := countUnique(lmt.GetHeader(header)); got != want {
		t.Fatalf("rule lost entries under concurrent reads: got %d, want %d", got, want)
	}
}
