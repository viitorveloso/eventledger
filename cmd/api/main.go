// eventledger — ledger bancário com Event Sourcing + CQRS.
//
// Ordem de boot (importa!):
//  1. migra schema
//  2. catch-up do projetor  (eventos gravados e não projetados)
//  3. sobe bus + saga workers
//  4. catch-up da saga      (transferências debitadas e não concluídas)
//  5. abre a API
//
// Assim, um crash em qualquer ponto do fluxo é retomado no próximo boot
// antes de aceitar tráfego novo.
package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"eventledger/internal/api"
	"eventledger/internal/app"
	"eventledger/internal/bus"
	"eventledger/internal/projection"
	"eventledger/internal/query"
	"eventledger/internal/saga"

	pgstore "eventledger/internal/eventstore"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	dsn := envOr("DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable")
	addr := envOr("ADDR", ":8080")

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fatal(log, "open db", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := waitForDB(db, 15*time.Second); err != nil {
		fatal(log, "db unreachable", err)
	}
	if err := migrate(db); err != nil {
		fatal(log, "migrate", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store := pgstore.NewPostgres(db)
	eventBus := bus.NewInMemory(8, 1024)
	application := app.New(store, eventBus)
	projector := projection.New(db, log)
	transferSaga := saga.New(application, store, log)

	// --- catch-up antes de aceitar tráfego ---
	pending, err := store.LoadUnprocessed(ctx)
	if err != nil {
		fatal(log, "load unprocessed", err)
	}
	if len(pending) > 0 {
		log.Info("projector catch-up", "events", len(pending))
		if err := projector.CatchUp(ctx, pending); err != nil {
			fatal(log, "projector catch-up", err)
		}
	}
	// --- pipeline assíncrono ---
	eventBus.Subscribe(projector.Handle)
	eventBus.Subscribe(transferSaga.Handle)
	eventBus.Start(ctx)
	transferSaga.Start(ctx, 4)

	if err := transferSaga.CatchUp(ctx); err != nil {
		fatal(log, "saga catch-up", err)
	}

	// --- HTTP ---
	server := api.NewServer(
		application,
		query.New(db),
		api.NewPGIdempotency(db),
		api.NewRateLimiter(50, 100), // 50 req/s, burst 100, por IP
		log,
	)
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal(log, "http server", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	// Ordem do shutdown: para de aceitar requests → drena bus → drena saga.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	eventBus.Close()
	transferSaga.Close()
	log.Info("bye")
}

func migrate(db *sql.DB) error {
	schema, err := os.ReadFile("migrations/001_schema.sql")
	if err != nil {
		return err
	}
	_, err = db.Exec(string(schema))
	return err
}

func waitForDB(db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if err := db.Ping(); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timeout waiting for database")
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fatal(log *slog.Logger, msg string, err error) {
	log.Error(msg, "err", err)
	os.Exit(1)
}
