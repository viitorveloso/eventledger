#!/usr/bin/env bash
# Demonstração de ponta a ponta contra a API em localhost:8080.
# Suba antes com: make up   (ou: docker compose up -d)
set -euo pipefail

API="${API:-http://localhost:8080}"

pretty() { python3 -m json.tool 2>/dev/null || cat; }
say()    { printf "\n\033[1;36m▸ %s\033[0m\n" "$*"; }

say "Criando conta da Alice com R\$ 200,00 de depósito inicial"
ALICE=$(curl -s -X POST "$API/accounts" \
  -H 'Idempotency-Key: demo-alice' \
  -d '{"owner":"alice","initial_deposit_cents":20000}' | grep -o '"account_id":"[^"]*"' | cut -d'"' -f4)
echo "alice = $ALICE"

say "Criando conta do Bob (saldo zero)"
BOB=$(curl -s -X POST "$API/accounts" \
  -H 'Idempotency-Key: demo-bob' \
  -d '{"owner":"bob"}' | grep -o '"account_id":"[^"]*"' | cut -d'"' -f4)
echo "bob = $BOB"

say "Transferindo R\$ 80,00 de Alice para Bob (débito síncrono, crédito via saga)"
RESP=$(curl -s -X POST "$API/transfers" \
  -H 'Idempotency-Key: demo-t1' \
  -d "{\"from_account_id\":\"$ALICE\",\"to_account_id\":\"$BOB\",\"amount_cents\":8000}")
echo "$RESP" | pretty
TID=$(echo "$RESP" | grep -o '"transfer_id":"[^"]*"' | cut -d'"' -f4)
sleep 1

say "Reenviando a MESMA request (mesma Idempotency-Key): replay, não reexecuta"
curl -s -D - -X POST "$API/transfers" \
  -H 'Idempotency-Key: demo-t1' \
  -d "{\"from_account_id\":\"$ALICE\",\"to_account_id\":\"$BOB\",\"amount_cents\":8000}" \
  | grep -iE "^HTTP|^idempotency-replay|transfer_id"

say "Status da transferência (read model)"
curl -s "$API/transfers/$TID" | pretty

say "Saldos (read models, alimentados pelo projetor)"
curl -s "$API/accounts/$ALICE/balance" | pretty
curl -s "$API/accounts/$BOB/balance" | pretty

say "Transferência SEM saldo: rejeitada de forma síncrona (HTTP 422)"
curl -s -w "\nHTTP %{http_code}\n" -X POST "$API/transfers" \
  -H 'Idempotency-Key: demo-t2' \
  -d "{\"from_account_id\":\"$ALICE\",\"to_account_id\":\"$BOB\",\"amount_cents\":99999999}"

say "Transferência para conta INEXISTENTE: débito ok, saga detecta e ESTORNA"
RESP=$(curl -s -X POST "$API/transfers" \
  -H 'Idempotency-Key: demo-t3' \
  -d "{\"from_account_id\":\"$ALICE\",\"to_account_id\":\"00000000-0000-4000-8000-000000000000\",\"amount_cents\":3000}")
GTID=$(echo "$RESP" | grep -o '"transfer_id":"[^"]*"' | cut -d'"' -f4)
sleep 1
curl -s "$API/transfers/$GTID" | pretty

say "Saldo da Alice após o estorno (dinheiro voltou)"
curl -s "$API/accounts/$ALICE/balance" | pretty

say "Trilha de auditoria imutável da Alice (o event stream é a fonte da verdade)"
curl -s "$API/accounts/$ALICE/events" | pretty

say "Extrato paginado"
curl -s "$API/accounts/$ALICE/statement?limit=10" | pretty

printf "\n\033[1;32m✓ demo concluída\033[0m\n"
