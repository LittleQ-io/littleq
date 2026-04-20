//go:build integration

package mongo_test

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	lqmongo "github.com/LittleQ-io/littleq/mongo"
	"github.com/LittleQ-io/littleq/storetest"
)

func TestMongoStore(t *testing.T) {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27019/?directConnection=true"
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("mongo.Connect: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(context.Background()) })

	if err := client.Ping(context.Background(), nil); err != nil {
		t.Fatalf("mongo ping: %v", err)
	}

	db := client.Database("littleq_test")
	t.Cleanup(func() { db.Drop(context.Background()) })

	const hbTTL = 150 * time.Millisecond

	store, err := lqmongo.New(db, lqmongo.WithHeartbeatTTL(hbTTL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	storetest.RunRepositoryTests(t, store, storetest.TestConfig[string]{
		HeartbeatTTL: hbTTL,
	})
}
