package uid_test

import (
	"sort"
	"testing"
	"time"

	"github.com/LittleQ-io/littleq/internal/uid"
)

func TestNew_Format(t *testing.T) {
	id := uid.New()
	if len(id) != 32 {
		t.Fatalf("id length = %d, want 32", len(id))
	}
	for _, r := range id {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			t.Errorf("id contains non-hex char %q", r)
		}
	}
}

func TestNew_TimeOrdered(t *testing.T) {
	ids := make([]string, 8)
	for i := range ids {
		ids[i] = uid.New()
		time.Sleep(time.Microsecond)
	}
	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sort.Strings(sorted)
	for i := range ids {
		if ids[i] != sorted[i] {
			t.Fatalf("ids not sorted ascending at %d: orig=%v sorted=%v", i, ids, sorted)
		}
	}
}

func TestNew_Unique(t *testing.T) {
	seen := map[string]struct{}{}
	const n = 10_000
	for i := 0; i < n; i++ {
		id := uid.New()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %q after %d generations", id, i)
		}
		seen[id] = struct{}{}
	}
}
