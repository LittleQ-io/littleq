package littleq

import "encoding/json"

// PayloadCodec bridges a caller's domain type T and the backend's native payload type P.
type PayloadCodec[T, P any] interface {
	Marshal(v T) (P, error)
	Unmarshal(p P, v *T) error
}

// JSONCodec is a zero-size codec that marshals T to json.RawMessage.
type JSONCodec[T any] struct{}

var _ PayloadCodec[any, json.RawMessage] = JSONCodec[any]{}

func (JSONCodec[T]) Marshal(v T) (json.RawMessage, error) {
	return json.Marshal(v)
}

func (JSONCodec[T]) Unmarshal(p json.RawMessage, v *T) error {
	return json.Unmarshal(p, v)
}
