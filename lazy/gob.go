package lazy

import "errors"

// ErrGobLazyFrame is returned when a LazyFrame is gob-encoded. A plan can
// hold Go functions (map UDFs), open scan sources and in-memory frames, so
// it has no faithful serialized form.
var ErrGobLazyFrame = errors.New("a golars LazyFrame can't be serialized: " +
	"call Collect and keep the DataFrame, or rebuild the plan from a " +
	"package-level var declaration")

// GobEncode implements [encoding/gob.GobEncoder] so that gob reports why a
// LazyFrame can't be saved instead of "type has no exported fields". It
// always returns [ErrGobLazyFrame].
func (lf LazyFrame) GobEncode() ([]byte, error) { return nil, ErrGobLazyFrame }

// GobDecode implements [encoding/gob.GobDecoder]. It always returns
// [ErrGobLazyFrame].
func (lf *LazyFrame) GobDecode([]byte) error { return ErrGobLazyFrame }
