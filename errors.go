package littleq

import "errors"

// Sentinel errors returned by the littleq library. Callers should test these
// with errors.Is rather than by string or direct equality.
//
// Backend implementations wrap these with a context-specific prefix (e.g.
// "littleq/redis: heartbeat: ...") via fmt.Errorf so the sentinel remains
// matchable through errors.Is.
var (
	// ErrClosed is returned when an operation is attempted on a closed Store.
	ErrClosed = errors.New("littleq: store closed")
	// ErrNotFound is returned when a referenced task does not exist.
	ErrNotFound = errors.New("littleq: task not found")
	// ErrCapabilityMismatch is returned when a worker's capabilities do not
	// satisfy a task's requirements. Backends may surface this transparently
	// by releasing the task and continuing to wait.
	ErrCapabilityMismatch = errors.New("littleq: worker capabilities do not match task requirements")
	// ErrAlreadyClaimed is returned when an operation targets a task that
	// has already been claimed by another worker.
	ErrAlreadyClaimed = errors.New("littleq: task already claimed by another worker")
	// ErrNotOwner is returned when Heartbeat, Complete or Fail are invoked
	// with a worker ID that does not match the current task owner.
	ErrNotOwner = errors.New("littleq: worker does not own this task")
	// ErrDuplicate is returned when Push is called with a non-empty DedupKey
	// that already exists in the queue. The returned ID is the existing task's.
	ErrDuplicate = errors.New("littleq: duplicate task (dedup_key already exists)")
	// ErrInvalidPolicy is returned when a Policy contains contradictory values.
	ErrInvalidPolicy = errors.New("littleq: invalid policy")
	// ErrInvalidPriority is returned when a priority value is out of range.
	ErrInvalidPriority = errors.New("littleq: priority must be >= 0")
	// ErrInvalidArgument is returned for generic argument validation failures.
	ErrInvalidArgument = errors.New("littleq: invalid argument")
	// ErrNoUpcaster is returned when applyUpcasters cannot find a chain link.
	ErrNoUpcaster = errors.New("littleq: no upcaster available for schema version")
	// ErrNotSupported is returned by optional operations not implemented by
	// the active backend.
	ErrNotSupported = errors.New("littleq: operation not supported by this backend")
	// ErrEncode is returned when a PayloadCodec fails to marshal a value.
	ErrEncode = errors.New("littleq: encode failed")
	// ErrDecode is returned when a PayloadCodec fails to unmarshal a value.
	ErrDecode = errors.New("littleq: decode failed")
)
