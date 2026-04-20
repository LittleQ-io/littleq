//go:build integration

package postgres_test

import (
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"

	lqpg "github.com/LittleQ-io/littleq/postgres"
	"github.com/LittleQ-io/littleq/storetest"
)

func TestPostgresStore(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://littleq_test:littleq_test@localhost:5433/littleq_test?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if err := db.Ping(); err != nil {
		t.Fatalf("postgres ping: %v", err)
	}

	const hbTTL = 150 * time.Millisecond

	store, err := lqpg.New(db, dsn, lqpg.WithHeartbeatTTL(hbTTL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	storetest.RunRepositoryTests(t, store, storetest.TestConfig[int64]{
		HeartbeatTTL: hbTTL,
	})
}
