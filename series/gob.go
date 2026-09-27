package series

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/internal/arrowgob"
)

// GobEncode implements [encoding/gob.GobEncoder]. The series is written as
// a one-column Arrow IPC stream, so its exact arrow type (dictionary, time
// zone, nested types) and its name round-trip.
func (s *Series) GobEncode() ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("series: GobEncode of a nil *Series")
	}
	return arrowgob.Encode([]string{s.name}, []*arrow.Chunked{s.data}, s.Len())
}

// GobDecode implements [encoding/gob.GobDecoder]. It replaces the contents
// of s with the decoded series. The previous data is not released, since
// other series may share it.
func (s *Series) GobDecode(data []byte) error {
	names, cols, _, err := arrowgob.Decode(data)
	if err != nil {
		return fmt.Errorf("series: GobDecode: %w", err)
	}
	if len(cols) != 1 {
		for _, c := range cols {
			c.Release()
		}
		return fmt.Errorf("series: GobDecode: want 1 column, got %d", len(cols))
	}
	s.name, s.data = names[0], cols[0]
	return nil
}
