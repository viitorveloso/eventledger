// Package api expõe o serviço via HTTP (stdlib net/http, Go 1.22 patterns).
//
// Escrita: POST /accounts, /deposits, /withdrawals, /transfers → command side.
// Leitura: GET /balance, /statement, /transfers/{id} → read models, apenas.
// Auditoria: GET /accounts/{id}/events → stream imutável do agregado.
package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"eventledger/internal/app"
	"eventledger/internal/domain"
	"eventledger/internal/query"
)

// commandTimeout limita o tempo de qualquer comando. Se o banco travar,
// o context cancela a operação e libera o handler — nada fica pendurado.
const commandTimeout = 5 * time.Second

type Server struct {
	app     *app.App
	queries *query.Queries
	idem    IdempotencyStore
	limiter *RateLimiter
	log     *slog.Logger
	mux     *http.ServeMux
}

func NewServer(a *app.App, q *query.Queries, idem IdempotencyStore, rl *RateLimiter, log *slog.Logger) *Server {
	s := &Server{app: a, queries: q, idem: idem, limiter: rl, log: log, mux: http.NewServeMux()}

	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := s.queries.Ping(ctx); err != nil {
			s.log.Error("health check failed", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Command side (todas exigem Idempotency-Key)
	s.mux.Handle("POST /accounts", s.idempotent(s.handleOpenAccount))
	s.mux.Handle("POST /accounts/{id}/deposits", s.idempotent(s.handleDeposit))
	s.mux.Handle("POST /accounts/{id}/withdrawals", s.idempotent(s.handleWithdraw))
	s.mux.Handle("POST /transfers", s.idempotent(s.handleTransfer))

	// Query side
	s.mux.HandleFunc("GET /accounts/{id}/balance", s.handleBalance)
	s.mux.HandleFunc("GET /accounts/{id}/statement", s.handleStatement)
	s.mux.HandleFunc("GET /accounts/{id}/events", s.handleEvents)
	s.mux.HandleFunc("GET /transfers/{id}", s.handleGetTransfer)

	return s
}

func (s *Server) Handler() http.Handler {
	return s.withLogging(s.withRateLimit(s.mux))
}

// ---- Command handlers ----

func (s *Server) handleOpenAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Owner          string `json:"owner"`
		InitialDeposit int64  `json:"initial_deposit_cents"`
	}
	if !decode(w, r, &req) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), commandTimeout)
	defer cancel()

	id, err := s.app.OpenAccount(ctx, req.Owner, req.InitialDeposit)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"account_id": id})
}

func (s *Server) handleDeposit(w http.ResponseWriter, r *http.Request) {
	s.handleMovement(w, r, s.app.Deposit)
}

func (s *Server) handleWithdraw(w http.ResponseWriter, r *http.Request) {
	s.handleMovement(w, r, s.app.Withdraw)
}

func (s *Server) handleMovement(w http.ResponseWriter, r *http.Request,
	fn func(ctx context.Context, id string, amount int64, desc string) error) {
	var req struct {
		Amount      int64  `json:"amount_cents"`
		Description string `json:"description"`
	}
	if !decode(w, r, &req) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), commandTimeout)
	defer cancel()

	if err := fn(ctx, r.PathValue("id"), req.Amount, req.Description); err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "accepted"})
}

func (s *Server) handleTransfer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From   string `json:"from_account_id"`
		To     string `json:"to_account_id"`
		Amount int64  `json:"amount_cents"`
	}
	if !decode(w, r, &req) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), commandTimeout)
	defer cancel()

	transferID, err := s.app.StartTransfer(ctx, req.From, req.To, req.Amount)
	if err != nil {
		s.writeError(w, err)
		return
	}
	// 202: o débito foi validado e gravado; o crédito conclui de forma
	// assíncrona. O cliente acompanha em GET /transfers/{id}.
	writeJSON(w, http.StatusAccepted, map[string]string{
		"transfer_id": transferID,
		"status":      "pending",
	})
}

// ---- Query handlers ----

func (s *Server) handleBalance(w http.ResponseWriter, r *http.Request) {
	b, err := s.queries.Balance(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (s *Server) handleStatement(w http.ResponseWriter, r *http.Request) {
	before := time.Now().UTC().Add(time.Second)
	if v := r.URL.Query().Get("before"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errBody("invalid 'before': use RFC3339"))
			return
		}
		before = t
	}
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 100 {
			writeJSON(w, http.StatusBadRequest, errBody("invalid 'limit': use an integer between 1 and 100"))
			return
		}
		limit = n
	}
	entries, err := s.queries.Statement(r.Context(), r.PathValue("id"), limit, before)
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	events, err := s.app.Store().EventsByAggregate(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeError(w, err)
		return
	}
	if len(events) == 0 {
		s.writeError(w, domain.ErrAccountNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (s *Server) handleGetTransfer(w http.ResponseWriter, r *http.Request) {
	t, err := s.queries.Transfer(r.Context(), r.PathValue("id"))
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, errBody("transfer not found"))
		return
	}
	if err != nil {
		s.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// ---- Middlewares ----

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &recorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("http", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration_ms", time.Since(start).Milliseconds())
	})
}

func (s *Server) withRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.limiter.Allow(clientIP(r)) {
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusTooManyRequests, errBody("rate limit exceeded"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// idempotent implementa o protocolo Idempotency-Key em torno de um handler
// de comando: reserva → executa → grava resposta; replays não reexecutam.
func (s *Server) idempotent(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if err := validateIdempotencyKey(key); err != nil {
			writeJSON(w, http.StatusBadRequest, errBody(err.Error()))
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errBody("cannot read body"))
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		reqHash := hashRequest(r.Method, r.URL.Path, body)

		inserted, rec, err := s.idem.Reserve(r.Context(), key, reqHash)
		if err != nil {
			s.writeError(w, err)
			return
		}
		if !inserted {
			switch {
			case rec.RequestHash != "" && rec.RequestHash != reqHash:
				// Mesma chave, payload diferente: uso incorreto do cliente.
				writeJSON(w, http.StatusUnprocessableEntity,
					errBody("Idempotency-Key already used with a different request"))
			case rec.StatusCode == 0:
				// Original ainda em voo: não execute duas vezes.
				w.Header().Set("Retry-After", "1")
				writeJSON(w, http.StatusConflict, errBody("original request still processing"))
			default: // replay da resposta original
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Idempotency-Replay", "true")
				w.WriteHeader(rec.StatusCode)
				_, _ = w.Write([]byte(rec.Body))
			}
			return
		}

		buf := &recorder{ResponseWriter: w, status: http.StatusOK, capture: &bytes.Buffer{}}
		next(buf, r)
		if err := s.idem.Complete(r.Context(), key, buf.status, buf.capture.String()); err != nil {
			s.log.Error("idempotency complete failed", "key", key, "err", err)
		}
	})
}

// ---- Helpers ----

type recorder struct {
	http.ResponseWriter
	status  int
	capture *bytes.Buffer
}

func (r *recorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.capture != nil {
		r.capture.Write(b)
	}
	return r.ResponseWriter.Write(b)
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid JSON body: "+err.Error()))
		return false
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		writeJSON(w, http.StatusBadRequest, errBody("invalid JSON body: multiple JSON values"))
		return false
	}
	return true
}

func validateIdempotencyKey(key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return errors.New("Idempotency-Key header is required")
	}
	if len(key) > 128 {
		return errors.New("Idempotency-Key header is too long")
	}
	return nil
}

func (s *Server) writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrAccountNotFound):
		writeJSON(w, http.StatusNotFound, errBody(err.Error()))
	case errors.Is(err, domain.ErrInsufficientFunds):
		writeJSON(w, http.StatusUnprocessableEntity, errBody(err.Error()))
	case errors.Is(err, domain.ErrInvalidAmount),
		errors.Is(err, domain.ErrOwnerRequired),
		errors.Is(err, domain.ErrSameAccount),
		errors.Is(err, domain.ErrAmountTooLarge):
		writeJSON(w, http.StatusBadRequest, errBody(err.Error()))
	case errors.Is(err, domain.ErrAccountClosed):
		writeJSON(w, http.StatusConflict, errBody(err.Error()))
	case errors.Is(err, app.ErrTooManyConflicts):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusConflict, errBody(err.Error()))
	case errors.Is(err, context.DeadlineExceeded):
		writeJSON(w, http.StatusGatewayTimeout, errBody("operation timed out"))
	default:
		s.log.Error("internal error", "err", err)
		writeJSON(w, http.StatusInternalServerError, errBody("internal error"))
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string { return map[string]string{"error": msg} }

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
