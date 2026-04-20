package littleq_test

import (
	"encoding/json"
	"testing"

	lq "github.com/LittleQ-io/littleq"
)

type demoPayload struct {
	N int    `json:"n"`
	S string `json:"s"`
}

func TestJSONCodec_Roundtrip(t *testing.T) {
	c := lq.JSONCodec[demoPayload]{}
	in := demoPayload{N: 42, S: "hi"}
	raw, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out demoPayload
	if err := c.Unmarshal(raw, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if out != in {
		t.Errorf("roundtrip = %+v, want %+v", out, in)
	}
}

func TestJSONCodec_UnmarshalError(t *testing.T) {
	c := lq.JSONCodec[demoPayload]{}
	var out demoPayload
	if err := c.Unmarshal(json.RawMessage("not-json"), &out); err == nil {
		t.Error("expected error on invalid JSON")
	}
}
