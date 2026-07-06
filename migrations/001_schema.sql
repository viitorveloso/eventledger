-- ============================================================
-- WRITE SIDE (Event Store)
-- ============================================================

-- Append-only event log. Nunca sofre UPDATE ou DELETE.
-- UNIQUE (aggregate_id, version) é o coração do optimistic locking:
-- dois writers concorrentes no mesmo agregado colidem aqui e um deles
-- recebe erro de unicidade, forçando reload + retry.
CREATE TABLE IF NOT EXISTS events (
    seq          BIGSERIAL PRIMARY KEY,           -- ordem global de gravação
    event_id     UUID        NOT NULL UNIQUE,
    aggregate_id UUID        NOT NULL,
    version      BIGINT      NOT NULL,
    type         TEXT        NOT NULL,
    data         JSONB       NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, version),
    CHECK (version > 0)
);

CREATE INDEX IF NOT EXISTS idx_events_aggregate ON events (aggregate_id, version);
CREATE INDEX IF NOT EXISTS idx_events_type      ON events (type);
-- Dedup de saga por chave de negócio (transfer_id dentro do payload)
CREATE INDEX IF NOT EXISTS idx_events_transfer  ON events ((data ->> 'transfer_id'));

-- Snapshots: evita reconstruir agregados com milhares de eventos.
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_id UUID PRIMARY KEY,
    version      BIGINT      NOT NULL,
    state        JSONB       NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (version > 0)
);

-- Idempotência de comandos HTTP (header Idempotency-Key).
-- status_code = 0 significa "em processamento" (reserva pessimista da chave).
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           TEXT PRIMARY KEY,
    request_hash  TEXT        NOT NULL,
    status_code   INT         NOT NULL DEFAULT 0,
    response_body TEXT        NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- READ SIDE (Projeções / Materialized Views)
-- Alimentado exclusivamente pelo projetor, nunca pela API de escrita.
-- ============================================================

CREATE TABLE IF NOT EXISTS account_balances (
    account_id   UUID PRIMARY KEY,
    owner        TEXT        NOT NULL,
    balance      BIGINT      NOT NULL DEFAULT 0,   -- centavos
    open         BOOLEAN     NOT NULL DEFAULT TRUE,
    last_version BIGINT      NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (last_version >= 0)
);

CREATE TABLE IF NOT EXISTS statement_entries (
    event_id     UUID PRIMARY KEY,                 -- PK = dedup natural
    account_id   UUID        NOT NULL,
    kind         TEXT        NOT NULL,             -- deposit|withdrawal|transfer_out|transfer_in|transfer_reversal
    amount       BIGINT      NOT NULL,             -- assinado: negativo = saída
    counterparty UUID,
    transfer_id  UUID,
    description  TEXT        NOT NULL DEFAULT '',
    occurred_at  TIMESTAMPTZ NOT NULL,
    CHECK (kind IN ('deposit', 'withdrawal', 'transfer_out', 'transfer_in', 'transfer_reversal')),
    CHECK (amount <> 0)
);

CREATE INDEX IF NOT EXISTS idx_statement_account
    ON statement_entries (account_id, occurred_at DESC, event_id);

CREATE TABLE IF NOT EXISTS transfers (
    transfer_id UUID PRIMARY KEY,
    from_id     UUID        NOT NULL,
    to_id       UUID        NOT NULL,
    amount      BIGINT      NOT NULL,
    status      TEXT        NOT NULL,              -- pending|completed|reversed
    reason      TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (amount > 0),
    CHECK (status IN ('pending', 'completed', 'reversed'))
);

-- Dedup do projetor: garante projeção idempotente sob entrega at-least-once.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
