package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"sync"
)

// IdempotencyStore guarda o resultado de cada Idempotency-Key.
//
// O protocolo tem duas fases para fechar a corrida de requests simultâneos
// com a MESMA chave:
//  1. Reserve: INSERT da chave com status 0 ("processando"). Só um request
//     consegue; os demais enxergam a reserva e respondem 409 até o dono
//     concluir.
//  2. Complete: grava status + body finais. Replays futuros devolvem a
//     resposta original sem reexecutar o comando.
type IdempotencyStore interface {
	// Reserve tenta registrar a chave. Retorna:
	//   inserted=true  → este request é o dono, deve executar o comando
	//   inserted=false → chave já existe; rec traz o estado atual
	Reserve(ctx context.Context, key, requestHash string) (inserted bool, rec IdempotencyRecord, err error)
	Complete(ctx context.Context, key string, statusCode int, body string) error
}

type IdempotencyRecord struct {
	RequestHash string
	StatusCode  int // 0 = ainda processando
	Body        string
}

func hashRequest(method, path string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// ---- Postgres ----

type PGIdempotency struct{ db *sql.DB }

func NewPGIdempotency(db *sql.DB) *PGIdempotency { return &PGIdempotency{db: db} }

func (s *PGIdempotency) Reserve(ctx context.Context, key, requestHash string) (bool, IdempotencyRecord, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2)
		 ON CONFLICT (key) DO NOTHING`, key, requestHash)
	if err != nil {
		return false, IdempotencyRecord{}, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return true, IdempotencyRecord{}, nil
	}
	var rec IdempotencyRecord
	err = s.db.QueryRowContext(ctx,
		`SELECT request_hash, status_code, response_body FROM idempotency_keys WHERE key = $1`, key,
	).Scan(&rec.RequestHash, &rec.StatusCode, &rec.Body)
	if errors.Is(err, sql.ErrNoRows) {
		// Janela minúscula: linha sumiu entre INSERT e SELECT (limpeza).
		// Tratar como "processando" é o lado seguro.
		return false, IdempotencyRecord{}, nil
	}
	return false, rec, err
}

func (s *PGIdempotency) Complete(ctx context.Context, key string, statusCode int, body string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE idempotency_keys SET status_code = $2, response_body = $3 WHERE key = $1`,
		key, statusCode, body)
	return err
}

// ---- Memória (testes) ----

type MemIdempotency struct {
	mu   sync.Mutex
	recs map[string]*IdempotencyRecord
}

func NewMemIdempotency() *MemIdempotency {
	return &MemIdempotency{recs: make(map[string]*IdempotencyRecord)}
}

func (s *MemIdempotency) Reserve(_ context.Context, key, requestHash string) (bool, IdempotencyRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.recs[key]; ok {
		return false, *rec, nil
	}
	s.recs[key] = &IdempotencyRecord{RequestHash: requestHash}
	return true, IdempotencyRecord{}, nil
}

func (s *MemIdempotency) Complete(_ context.Context, key string, statusCode int, body string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.recs[key]; ok {
		rec.StatusCode = statusCode
		rec.Body = body
	}
	return nil
}
