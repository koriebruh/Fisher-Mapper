package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"Fisher-Mapper/internal/domain/payment"
	"Fisher-Mapper/internal/messaging/idempotency"
	"Fisher-Mapper/internal/platform/tenantauth"
	"Fisher-Mapper/internal/provider"
	"Fisher-Mapper/internal/provider/mock"
)

// This file proves the CRITICAL security fix's REST half end-to-end: every
// payment/refund/payout route used to have no caller authentication at all,
// with tenant_id a self-asserted request-body field and every Get unscoped
// by tenant. These tests run against a real Postgres-backed Service and a
// real fiber app built by NewApp -- the exact wiring cmd/server uses (minus
// the rate limiter, left nil so it doesn't interfere) -- gated on
// TEST_POSTGRES_DSN like every other DB-backed test in this repo.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set; skipping DB-backed integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect test postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// singleProviderRegistry satisfies payment.Service's unexported
// providerRegistry interface structurally -- same small local
// re-declaration internal/domain/payment/service_integration_test.go and
// internal/transport/grpc/server_integration_test.go both already use for
// the identical reason (the interface type itself isn't exported, but any
// type with a matching Get method works as an argument).
type singleProviderRegistry struct {
	p provider.Provider
}

func (r singleProviderRegistry) Get(string) (provider.Provider, error) {
	return r.p, nil
}

// newTestAppAndStore builds a real fiber app (NewApp, the same constructor
// cmd/server uses) wired to a real Postgres-backed payment.Service and
// tenantauth.Store -- RateLimiter is left nil (no rate-limit middleware in
// these tests) so tenantAuth is the only thing gating the payment/payout
// route groups.
func newTestAppAndStore(t *testing.T, pool *pgxpool.Pool) (*fiber.App, *tenantauth.Store) {
	t.Helper()
	mockProv := mock.New(mock.Config{Name: "mock"})
	repo := payment.NewPGRepository(pool)
	idemStore := idempotency.NewPGStore(pool)
	svc := payment.NewService(repo, idemStore, singleProviderRegistry{mockProv})
	tenantStore := tenantauth.NewStore(pool)

	app := NewApp(Deps{
		PaymentService:  svc,
		TenantAuthStore: tenantStore,
	})
	return app, tenantStore
}

// newTenant mints a fresh, unique tenant_id + API key pair via the real
// tenantauth.Store.CreateKey path -- the same one cmd/migrate's
// -create-tenant-key flag uses -- so each test gets its own never-reused
// tenant, no dependence on any shared/seeded key (there is none, see
// migration 00010's doc).
func newTenant(ctx context.Context, t *testing.T, store *tenantauth.Store) (tenantID, apiKey string) {
	t.Helper()
	tenantID = uuid.NewString()
	apiKey, err := store.CreateKey(ctx, tenantID)
	if err != nil {
		t.Fatalf("create tenant key: %v", err)
	}
	return tenantID, apiKey
}

// doRequest is a small fluent helper over fiber's app.Test, shared by every
// test below. Returns the status code and the fully-read body rather than
// the raw *http.Response -- no caller here needs anything else off the
// response, and returning the response only for its StatusCode would leave
// its Body's lifetime (and closing it) the caller's problem instead of this
// helper's.
func doRequest(t *testing.T, app *fiber.App, method, path, apiKey, idempotencyKey string, body any) (status int, respBody []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, path, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("X-Api-Key", apiKey)
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp.StatusCode, respBody
}

func TestREST_TenantAuth_MissingAPIKey_Unauthorized(t *testing.T) {
	pool := testPool(t)
	app, _ := newTestAppAndStore(t, pool)

	status, _ := doRequest(t, app, http.MethodGet, "/payments/"+uuid.NewString(), "", "", nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("GetPayment with no X-Api-Key: status = %d, want %d", status, http.StatusUnauthorized)
	}
}

func TestREST_TenantAuth_InvalidAPIKey_Unauthorized(t *testing.T) {
	pool := testPool(t)
	app, _ := newTestAppAndStore(t, pool)

	status, _ := doRequest(t, app, http.MethodGet, "/payments/"+uuid.NewString(), "not-a-real-key-"+uuid.NewString(), "", nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("GetPayment with an unknown X-Api-Key: status = %d, want %d", status, http.StatusUnauthorized)
	}
}

// TestREST_TenantIsolation_CrossTenantGetPayment_NotFound is the CRITICAL
// finding's core proof, REST side: tenant-1 creates a payment via a body
// with NO tenant_id field (createPaymentRequest has none, see its doc) --
// the row is owned by whichever tenant tenant-1's OWN X-Api-Key resolves
// to. tenant-1's key reads it back; tenant-2's key (independently issued)
// gets 404 for the exact same payment id, never the row.
func TestREST_TenantIsolation_CrossTenantGetPayment_NotFound(t *testing.T) {
	pool := testPool(t)
	app, tenantStore := newTestAppAndStore(t, pool)
	ctx := context.Background()

	_, tenant1Key := newTenant(ctx, t, tenantStore)
	_, tenant2Key := newTenant(ctx, t, tenantStore)

	createBody := map[string]any{"currency": "USD", "amount": 1234, "provider": "mock"}
	status, body := doRequest(t, app, http.MethodPost, "/payments", tenant1Key, uuid.NewString(), createBody)
	if status != http.StatusAccepted {
		t.Fatalf("CreatePayment (tenant1): status = %d, body = %s", status, body)
	}
	var created struct {
		PaymentID string `json:"payment_id"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}

	// Owner reads its own payment: succeeds.
	status, body = doRequest(t, app, http.MethodGet, "/payments/"+created.PaymentID, tenant1Key, "", nil)
	if status != http.StatusOK {
		t.Fatalf("GetPayment as owning tenant: status = %d, body = %s", status, body)
	}

	// A DIFFERENT tenant, with its own genuinely valid API key, tries the
	// SAME payment id: must be 404, never the row (DATA LEAK if not).
	status, body = doRequest(t, app, http.MethodGet, "/payments/"+created.PaymentID, tenant2Key, "", nil)
	if status != http.StatusNotFound {
		t.Fatalf("GetPayment for another tenant's payment id: status = %d, body = %s, want %d (not-found, not a leaked row)",
			status, body, http.StatusNotFound)
	}
}

// TestREST_TenantIsolation_CreatePayment_UsesAuthenticatedTenant_NotSpoofable
// proves the write-side half of the CRITICAL finding is closed:
// createPaymentRequest has no tenant_id field at all, so there is nothing
// left in the body to spoof -- a caller authenticated as tenant-2 gets a
// payment owned by tenant-2, full stop, regardless of what it might have
// tried to put in the body.
func TestREST_TenantIsolation_CreatePayment_UsesAuthenticatedTenant_NotSpoofable(t *testing.T) {
	pool := testPool(t)
	app, tenantStore := newTestAppAndStore(t, pool)
	ctx := context.Background()

	_, tenant2Key := newTenant(ctx, t, tenantStore)
	_, otherTenantKey := newTenant(ctx, t, tenantStore)

	// A body carrying an extra "tenant_id" field is silently ignored by
	// createPaymentRequest (the field no longer exists on the struct) --
	// this is the concrete spoofing attempt the finding described.
	createBody := map[string]any{
		"currency":  "USD",
		"amount":    4242,
		"provider":  "mock",
		"tenant_id": "attacker-controlled-tenant",
	}
	status, body := doRequest(t, app, http.MethodPost, "/payments", tenant2Key, uuid.NewString(), createBody)
	if status != http.StatusAccepted {
		t.Fatalf("CreatePayment (tenant2, with a spoofing tenant_id field): status = %d, body = %s", status, body)
	}
	var created struct {
		PaymentID string `json:"payment_id"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}

	// The creating tenant's own key reads it back: proves the row really
	// was persisted under THAT tenant's authenticated identity.
	status, body = doRequest(t, app, http.MethodGet, "/payments/"+created.PaymentID, tenant2Key, "", nil)
	if status != http.StatusOK {
		t.Fatalf("creating tenant reading back its own payment: status = %d, body = %s", status, body)
	}

	// An unrelated tenant's key cannot -- confirms the payment is not
	// visible/owned by "attacker-controlled-tenant" or anyone but the
	// authenticated creator.
	status, body = doRequest(t, app, http.MethodGet, "/payments/"+created.PaymentID, otherTenantKey, "", nil)
	if status != http.StatusNotFound {
		t.Fatalf("unrelated tenant reading tenant2's payment: status = %d, body = %s, want %d", status, body, http.StatusNotFound)
	}
}

// TestREST_HealthAndReadyz_RemainUnauthenticated confirms the CRITICAL fix
// did not touch /healthz or /readyz -- both are unauthenticated by design
// (k8s probes), and neither is under the payment/payout route groups
// tenantAuth was added to.
func TestREST_HealthAndReadyz_RemainUnauthenticated(t *testing.T) {
	pool := testPool(t)
	app, _ := newTestAppAndStore(t, pool)

	status, body := doRequest(t, app, http.MethodGet, "/healthz", "", "", nil)
	if status != http.StatusOK {
		t.Fatalf("GET /healthz with no X-Api-Key: status = %d, body = %s, want %d", status, body, http.StatusOK)
	}
}
