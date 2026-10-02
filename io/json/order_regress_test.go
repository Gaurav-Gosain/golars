package json_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Gaurav-Gosain/golars/internal/testutil"
	"github.com/Gaurav-Gosain/golars/io/json"
)

// polars keeps JSON keys in first-seen document order:
// read_ndjson('{"b":1,"a":2}\n{"c":3,"a":4}') has columns [b, a, c].
func TestJSONColumnOrderFirstSeen(t *testing.T) {
	mem := testutil.NewCheckedAllocator(t)
	ctx := context.Background()
	for range 20 { // map iteration order is random; repeat to catch it
		nd, err := json.ReadNDJSON(ctx, strings.NewReader("{\"b\":1,\"a\":2,\"z\":0}\n{\"c\":3,\"a\":4}\n"), json.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(nd.ColumnNames(), ","); got != "b,a,z,c" {
			t.Fatalf("ndjson columns = %s, want b,a,z,c", got)
		}
		nd.Release()
		arr, err := json.Read(ctx, strings.NewReader(`[{"b":1,"a":2,"z":0},{"c":3,"a":4}]`), json.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(arr.ColumnNames(), ","); got != "b,a,z,c" {
			t.Fatalf("array columns = %s, want b,a,z,c", got)
		}
		arr.Release()
		obj, err := json.Read(ctx, strings.NewReader(`{"b":[1],"a":[2],"z":[0]}`), json.WithAllocator(mem))
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(obj.ColumnNames(), ","); got != "b,a,z" {
			t.Fatalf("object columns = %s, want b,a,z", got)
		}
		obj.Release()
	}
}
