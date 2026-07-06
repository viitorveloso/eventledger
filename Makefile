.PHONY: up down run test test-all demo vet

## sobe tudo com Docker (API em :8080)
up:
	docker compose up --build -d

down:
	docker compose down -v

## roda a API local (precisa de Postgres em localhost:5432)
run:
	go run ./cmd/api

## testes de unidade + concorrência com race detector (sem infra)
test:
	go test -race ./...

## suíte completa: inclui integração e e2e contra Postgres real
test-all:
	TEST_DATABASE_URL="postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable" \
		go test -race -count=1 ./...

vet:
	go vet ./...

## roteiro de demonstração contra a API em :8080
demo:
	./scripts/demo.sh
