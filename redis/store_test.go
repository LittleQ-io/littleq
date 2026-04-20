//go:build integration

package redis_test

import (
	"os"
	"testing"
	"time"

	rdb "github.com/redis/go-redis/v9"

	lqredis "github.com/LittleQ-io/littleq/redis"
	"github.com/LittleQ-io/littleq/storetest"
)

func TestRedisStore(t *testing.T) {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6399"
	}

	opt, err := rdb.ParseURL(url)
	if err != nil {
		t.Fatalf("parse REDIS_URL: %v", err)
	}
	client := rdb.NewClient(opt)
	t.Cleanup(func() { client.Close() })

	const hbTTL = 150 * time.Millisecond

	store, err := lqredis.New(client, lqredis.WithHeartbeatTTL(hbTTL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	storetest.RunRepositoryTests(t, store, storetest.TestConfig[string]{
		HeartbeatTTL: hbTTL,
	})
}
