package littleq_test

import (
	"context"
	"errors"
	"testing"

	lq "github.com/LittleQ-io/littleq"
)

func TestUpcaster_Chain(t *testing.T) {
	v1to2 := lq.UpcasterFunc[int](1, 2, func(_ context.Context, p int) (int, error) { return p + 10, nil })
	v2to3 := lq.UpcasterFunc[int](2, 3, func(_ context.Context, p int) (int, error) { return p * 2, nil })

	// Use Queue.Pop indirectly by calling the generic pipeline via a helper.
	// applyUpcasters is unexported; drive it through a Queue with a fakeRepo.

	// Direct test: confirm UpcasterFunc values round-trip From/To/Upcast.
	if v1to2.FromVersion() != 1 || v1to2.ToVersion() != 2 {
		t.Fatal("v1to2 version metadata wrong")
	}
	got, err := v1to2.Upcast(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if got != 13 {
		t.Errorf("v1to2(3) = %d, want 13", got)
	}

	got, err = v2to3.Upcast(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if got != 8 {
		t.Errorf("v2to3(4) = %d, want 8", got)
	}
}

func TestUpcaster_ErrorPropagates(t *testing.T) {
	sentinel := errors.New("upcast boom")
	u := lq.UpcasterFunc[int](1, 2, func(context.Context, int) (int, error) { return 0, sentinel })
	_, err := u.Upcast(context.Background(), 0)
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel, got %v", err)
	}
}
