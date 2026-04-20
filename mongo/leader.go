package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/LittleQ-io/littleq/internal/uid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mgopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type leaderDoc struct {
	ID        string    `bson:"_id"`
	NodeID    string    `bson:"node_id"`
	ExpiresAt time.Time `bson:"expires_at"`
}

// Campaign blocks until this node acquires the leader lease or ctx is cancelled.
func (s *Store) Campaign(ctx context.Context) error {
	nodeID := uid.New()
	refresh := s.opts.leaderRefresh

	for {
		ok, err := s.tryAcquireLeader(ctx, nodeID)
		if err != nil {
			return fmt.Errorf("littleq/mongo: campaign: %w", err)
		}
		if ok {
			s.ldrID.Store(&nodeID)
			go s.refreshLeaseLoop(ctx, nodeID)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(refresh):
		}
	}
}

func (s *Store) tryAcquireLeader(ctx context.Context, nodeID string) (bool, error) {
	now := time.Now()
	expires := now.Add(s.opts.leaderTTL)

	// Single atomic upsert: match either a non-existent document or one whose
	// lease has expired. If another node holds a live lease, the filter does
	// not match, Upsert fails with duplicate-key on _id, and we report !acquired.
	filter := bson.D{
		{Key: "_id", Value: s.opts.leaderKey},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "expires_at", Value: bson.D{{Key: "$lt", Value: now}}}},
			bson.D{{Key: "node_id", Value: nodeID}},
		}},
	}
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "node_id", Value: nodeID},
		{Key: "expires_at", Value: expires},
	}}}
	opts := mgopts.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(mgopts.After)

	var out leaderDoc
	err := s.leader.FindOneAndUpdate(ctx, filter, update, opts).Decode(&out)
	if err == nil {
		return out.NodeID == nodeID, nil
	}
	if mongo.IsDuplicateKeyError(err) {
		// Another live leader owns the key.
		return false, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	return false, err
}

func (s *Store) refreshLeaseLoop(ctx context.Context, nodeID string) {
	ticker := time.NewTicker(s.opts.leaderRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.ldrID.Store(nil)
			return
		case <-ticker.C:
			res, err := s.leader.UpdateOne(ctx,
				bson.D{{Key: "_id", Value: s.opts.leaderKey}, {Key: "node_id", Value: nodeID}},
				bson.D{{Key: "$set", Value: bson.D{
					{Key: "expires_at", Value: time.Now().Add(s.opts.leaderTTL)},
				}}},
			)
			if err != nil || res.MatchedCount == 0 {
				s.ldrID.Store(nil)
				return
			}
		}
	}
}

// Resign voluntarily relinquishes the leader lease.
func (s *Store) Resign(ctx context.Context) error {
	p := s.ldrID.Load()
	if p == nil {
		return nil
	}
	nodeID := *p
	s.ldrID.Store(nil)
	_, err := s.leader.DeleteOne(ctx, bson.D{
		{Key: "_id", Value: s.opts.leaderKey},
		{Key: "node_id", Value: nodeID},
	})
	return err
}

// IsLeader reports whether this node currently holds the lease.
func (s *Store) IsLeader() bool {
	return s.ldrID.Load() != nil
}
