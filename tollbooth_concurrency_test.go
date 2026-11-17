package tollbooth

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/didip/tollbooth/v8/limiter"
)

// These tests model the officially endorsed wiring from issue 66: the handler
// registers the tenant rules on every request and then delegates to the limiter.
// Several workers warm the limiter up at the same moment, which is exactly the
// window where the previous read-then-assign registration silently dropped
// entries: the registration that lost the race had its whole cache object
// overwritten by the one that wrote last, so its tenants were never limited.
const (
	endToEndWorkers          = 12
	endToEndTenantsPerWorker = 25
)

// tenantIDs returns the tenant ids owned by a single worker.
func tenantIDs(prefix string, worker int) []string {
	ids := make([]string, 0, endToEndTenantsPerWorker)
	for i := 0; i < endToEndTenantsPerWorker; i++ {
		ids = append(ids, fmt.Sprintf("%s-w%d-t%d", prefix, worker, i))
	}
	return ids
}

// countDistinct returns the number of distinct ids in the slice.
func countDistinct(ids []string) int {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		seen[id] = struct{}{}
	}
	return len(seen)
}

// warmUpConcurrently releases all workers at the same instant and lets each of
// them register its whole tenant set once, exactly as the handler does when it
// replays its configuration on every request.
func warmUpConcurrently(register func(worker int)) {
	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(endToEndWorkers)

	for worker := 0; worker < endToEndWorkers; worker++ {
		go func(worker int) {
			defer wg.Done()
			<-start
			register(worker)
		}(worker)
	}

	close(start)
	wg.Wait()
}

// TestConcurrentHeaderRegistration_LostIDsAreStillLimited is the statistical
// counterpart of Test_Issue66_CustomRateLimitByHeaderValues: after a concurrent
// warm-up, every single registered tenant must be limited on its second request.
// Any tenant whose registration was lost passes both requests indefinitely, which
// is the "limit silently disabled for some tenants" symptom reported against the
// per-request registration wiring.
func TestConcurrentHeaderRegistration_LostIDsAreStillLimited(t *testing.T) {
	const prefix = "tenant"

	allocationLimiter := NewLimiter(1, nil).SetMethods([]string{http.MethodPost}).
		SetIPLookup(limiter.IPLookup{Name: "RemoteAddr"})

	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

	// The upstream-endorsed shape: register inside the handler, per request.
	// Each request replays the tenant set owned by its worker.
	handler := func(worker int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			allocationLimiter.SetHeader(issue66HeaderKey, tenantIDs(prefix, worker))
			LimitFuncHandler(allocationLimiter, next).ServeHTTP(w, r)
		}
	}

	warmUpConcurrently(func(worker int) {
		req := httptest.NewRequest(http.MethodPost, "/warmup", nil)
		req.RemoteAddr = "127.0.0.1:1234"
		// The warm-up request carries one of its own worker's ids; the token
		// bucket it consumes lives under /warmup, so it cannot mask the /allocate
		// checks below.
		req.Header.Set(issue66HeaderKey, tenantIDs(prefix, worker)[0])
		handler(worker).ServeHTTP(httptest.NewRecorder(), req)
	})

	// Every worker's tenant set must be registered in full.
	registered := allocationLimiter.GetHeader(issue66HeaderKey)
	wantRegistered := endToEndWorkers * endToEndTenantsPerWorker
	if got := countDistinct(registered); got != wantRegistered {
		t.Fatalf("concurrent registration kept %d of %d tenant ids before any request was checked",
			got, wantRegistered)
	}

	limited := LimitFuncHandler(allocationLimiter, next)

	newRequest := func(tenant string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/allocate", nil)
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set(issue66HeaderKey, tenant)
		return req
	}

	unlimited := 0

	for worker := 0; worker < endToEndWorkers; worker++ {
		for _, tenant := range tenantIDs(prefix, worker) {
			first := httptest.NewRecorder()
			limited.ServeHTTP(first, newRequest(tenant))
			if first.Code != http.StatusOK {
				t.Fatalf("tenant %s must pass the limiter the first time, got %d", tenant, first.Code)
			}

			second := httptest.NewRecorder()
			limited.ServeHTTP(second, newRequest(tenant))
			if second.Code != http.StatusTooManyRequests {
				unlimited++
			}
		}
	}

	if unlimited != 0 {
		t.Fatalf("%d of %d registered tenant ids passed the limiter twice: their registration was lost",
			unlimited, wantRegistered)
	}
}

// TestConcurrentContextValueRegistration_AllRegisteredValuesAreLimited mirrors
// the header case for context-value rules: every registered value must still be
// enforceable after a concurrent warm-up.
func TestConcurrentContextValueRegistration_AllRegisteredValuesAreLimited(t *testing.T) {
	const (
		contextKey = "API-access-level"
		prefix     = "context-tenant"
	)

	lmt := NewLimiter(1, nil).SetMethods([]string{http.MethodGet}).
		SetIPLookup(limiter.IPLookup{Name: "RemoteAddr"})

	warmUpConcurrently(func(worker int) {
		lmt.SetContextValue(contextKey, tenantIDs(prefix, worker))
	})

	wantRegistered := endToEndWorkers * endToEndTenantsPerWorker
	if got := countDistinct(lmt.GetContextValue(contextKey)); got != wantRegistered {
		t.Fatalf("concurrent context-value registration kept %d of %d values", got, wantRegistered)
	}

	unlimited := 0

	for worker := 0; worker < endToEndWorkers; worker++ {
		for _, value := range tenantIDs(prefix, worker) {
			req := httptest.NewRequest(http.MethodGet, "/allocate", nil)
			req.RemoteAddr = "127.0.0.1:1234"
			//nolint:revive,staticcheck // limiter.SetContextValue requires string as a key, so we have to live with that
			req = req.WithContext(context.WithValue(req.Context(), contextKey, value))

			keys := BuildKeys(lmt, req)[0]
			if err := LimitByKeys(lmt, keys); err != nil {
				t.Fatalf("registered context value %s must pass the limiter the first time: %v", value, err)
			}
			if err := LimitByKeys(lmt, keys); err == nil {
				unlimited++
			}
		}
	}

	if unlimited != 0 {
		t.Fatalf("%d of %d registered context values passed the limiter twice: their registration was lost",
			unlimited, wantRegistered)
	}
}
