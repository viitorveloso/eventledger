package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"eventledger/internal/app"
	"eventledger/internal/bus"
	"eventledger/internal/domain"
	"eventledger/internal/eventstore"
	"eventledger/internal/query"
)

type nopBus struct{}

func (nopBus) Publish(domain.Event)  {}
func (nopBus) Subscribe(bus.Handler) {}

func testServer(t *testing.T) http.Handler {
	t.Helper()
	a := app.New(eventstore.NewMemory(), nopBus{})
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	// query.New(nil): estes testes só exercitam o command side (POSTs).
	s := NewServer(a, query.New(nil), NewMemIdempotency(), NewRateLimiter(1000, 1000), log)
	return s.Handler()
}

func post(h http.Handler, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestIdempotencyKeyIsRequired(t *testing.T) {
	h := testServer(t)
	res := post(h, "/accounts", "", `{"owner":"lucas"}`)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", res.Code)
	}
}

func TestIdempotencyKeyIsValidated(t *testing.T) {
	h := testServer(t)

	res := post(h, "/accounts", "   ", `{"owner":"lucas"}`)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("blank key: status = %d, want 400", res.Code)
	}

	res = post(h, "/accounts", strings.Repeat("x", 129), `{"owner":"lucas"}`)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("long key: status = %d, want 400", res.Code)
	}
}

func TestIdempotentReplayDoesNotReexecute(t *testing.T) {
	h := testServer(t)
	body := `{"owner":"lucas","initial_deposit_cents":1000}`

	first := post(h, "/accounts", "key-1", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first: status = %d, body = %s", first.Code, first.Body)
	}
	second := post(h, "/accounts", "key-1", body)
	if second.Code != http.StatusCreated {
		t.Fatalf("replay: status = %d", second.Code)
	}
	if second.Header().Get("Idempotency-Replay") != "true" {
		t.Fatal("replay must be marked with Idempotency-Replay header")
	}

	// Mesmo account_id nas duas respostas = o comando NÃO rodou duas vezes
	// (uma segunda execução geraria outro UUID).
	var r1, r2 struct {
		AccountID string `json:"account_id"`
	}
	_ = json.Unmarshal(first.Body.Bytes(), &r1)
	_ = json.Unmarshal(second.Body.Bytes(), &r2)
	if r1.AccountID == "" || r1.AccountID != r2.AccountID {
		t.Fatalf("replay returned different result: %q vs %q", r1.AccountID, r2.AccountID)
	}
}

func TestIdempotencyKeyReusedWithDifferentPayload(t *testing.T) {
	h := testServer(t)
	if res := post(h, "/accounts", "key-2", `{"owner":"a"}`); res.Code != http.StatusCreated {
		t.Fatalf("first: %d", res.Code)
	}
	res := post(h, "/accounts", "key-2", `{"owner":"b"}`)
	if res.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", res.Code)
	}
}

func TestDomainErrorsMapToStatusCodes(t *testing.T) {
	h := testServer(t)

	// Cria conta com R$ 10,00.
	res := post(h, "/accounts", "k-acc", `{"owner":"lucas","initial_deposit_cents":1000}`)
	var acc struct {
		AccountID string `json:"account_id"`
	}
	_ = json.Unmarshal(res.Body.Bytes(), &acc)

	// Saque acima do saldo → 422.
	res = post(h, "/accounts/"+acc.AccountID+"/withdrawals", "k-w1", `{"amount_cents":99999}`)
	if res.Code != http.StatusUnprocessableEntity {
		t.Fatalf("insufficient funds: status = %d, want 422", res.Code)
	}
	// Valor inválido → 400.
	res = post(h, "/accounts/"+acc.AccountID+"/deposits", "k-d1", `{"amount_cents":-5}`)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("invalid amount: status = %d, want 400", res.Code)
	}
	// Conta inexistente → 404.
	res = post(h, "/accounts/"+domain.NewID()+"/deposits", "k-d2", `{"amount_cents":100}`)
	if res.Code != http.StatusNotFound {
		t.Fatalf("missing account: status = %d, want 404", res.Code)
	}
	// Campo desconhecido → 400 (DisallowUnknownFields).
	res = post(h, "/accounts", "k-bad", `{"owner":"x","typo_field":1}`)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("unknown field: status = %d, want 400", res.Code)
	}
	res = post(h, "/accounts", "k-owner", `{"owner":"   "}`)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("missing owner: status = %d, want 400", res.Code)
	}
	res = post(h, "/accounts", "k-initial", `{"owner":"x","initial_deposit_cents":-1}`)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("negative initial deposit: status = %d, want 400", res.Code)
	}
	res = post(h, "/accounts", "k-multi", `{"owner":"x"} {"owner":"y"}`)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("multiple JSON values: status = %d, want 400", res.Code)
	}
}

func TestInvalidStatementLimitFailsBeforeQuery(t *testing.T) {
	h := testServer(t)
	req := httptest.NewRequest(http.MethodGet, "/accounts/acc/statement?limit=abc", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestHealthzReportsUnavailableWhenDBIsMissing(t *testing.T) {
	h := testServer(t)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}
