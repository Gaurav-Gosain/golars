// Package json reads and writes JSON and newline-delimited JSON (NDJSON),
// mirroring polars' pl.read_json / pl.read_ndjson / pl.write_json.
//
// Read accepts a single JSON document that is either:
//   - an array of objects: `[{"a": 1, "b": "x"}, ...]`
//   - an object whose values are equal-length arrays: `{"a": [1, 2], "b": ["x", "y"]}`
//
// ReadNDJSON accepts newline-delimited JSON where each line is one object.
//
// Column types are inferred from observed values in a two-pass scan:
//
//  1. first pass: learn column names + promote numeric types (int → float
//     when any value is fractional, to match polars' type coercion)
//  2. second pass: materialize typed slices
//
// Unrecognized or heterogeneous columns fall back to string.
package json

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

// Option configures a Read call.
type Option func(*config)

type config struct {
	alloc      memory.Allocator
	httpClient *http.Client
}

func resolve(opts []Option) config {
	c := config{alloc: memory.DefaultAllocator, httpClient: http.DefaultClient}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithAllocator overrides the allocator used for output buffers.
func WithAllocator(a memory.Allocator) Option { return func(c *config) { c.alloc = a } }

// WithHTTPClient overrides the HTTP client used by ReadURL/ReadNDJSONURL.
func WithHTTPClient(h *http.Client) Option { return func(c *config) { c.httpClient = h } }

// inferredType tracks the best-fit dtype for a column during inference.
type inferredType int

const (
	tUnknown inferredType = iota
	tBool
	tInt
	tFloat
	tString
	tMixed // fell back to string
)

func promote(a, b inferredType) inferredType {
	if a == tUnknown {
		return b
	}
	if b == tUnknown {
		return a
	}
	if a == b {
		return a
	}
	// int and float → float
	if (a == tInt && b == tFloat) || (a == tFloat && b == tInt) {
		return tFloat
	}
	// any disagreement falls back to string.
	return tMixed
}

func inferValue(v any) inferredType {
	if v == nil {
		return tUnknown
	}
	switch t := v.(type) {
	case bool:
		return tBool
	case float64:
		if t == math.Trunc(t) && !math.IsInf(t, 0) && !math.IsNaN(t) {
			return tInt
		}
		return tFloat
	case string:
		return tString
	}
	return tMixed
}

// Read parses a single JSON document from r (object-array or
// object-of-arrays) into a DataFrame. The entire document is buffered.
func Read(ctx context.Context, r io.Reader, opts ...Option) (*dataframe.DataFrame, error) {
	cfg := resolve(opts)
	// Peek the first non-whitespace byte to decide shape.
	br := bufio.NewReader(r)
	b, err := skipWS(br)
	if err != nil {
		return nil, fmt.Errorf("json.Read: %w", err)
	}
	if err := br.UnreadByte(); err != nil {
		return nil, fmt.Errorf("json.Read: %w", err)
	}
	switch b {
	case '[':
		var raws []json.RawMessage
		if err := json.NewDecoder(br).Decode(&raws); err != nil {
			return nil, fmt.Errorf("json.Read: %w", err)
		}
		var ord keyOrder
		rows := make([]map[string]any, len(raws))
		for i, raw := range raws {
			if err := json.Unmarshal(raw, &rows[i]); err != nil {
				return nil, fmt.Errorf("json.Read: %w", err)
			}
			if err := ord.add(rows[i], raw); err != nil {
				return nil, fmt.Errorf("json.Read: %w", err)
			}
		}
		return buildFromRowsOrdered(ctx, rows, ord.names, cfg)
	case '{':
		var raw json.RawMessage
		if err := json.NewDecoder(br).Decode(&raw); err != nil {
			return nil, fmt.Errorf("json.Read: object-of-arrays: %w", err)
		}
		var cols map[string][]any
		if err := json.Unmarshal(raw, &cols); err != nil {
			return nil, fmt.Errorf("json.Read: object-of-arrays: %w", err)
		}
		keys, err := objectKeys(raw)
		if err != nil {
			return nil, fmt.Errorf("json.Read: object-of-arrays: %w", err)
		}
		var ord keyOrder
		ord.addKeys(keys)
		return buildFromColumnsOrdered(ctx, cols, ord.names, cfg)
	default:
		return nil, fmt.Errorf("json.Read: expected '[' or '{' at document start, got %q", b)
	}
}

// ReadString is a convenience that parses a JSON string.
func ReadString(ctx context.Context, s string, opts ...Option) (*dataframe.DataFrame, error) {
	return Read(ctx, strings.NewReader(s), opts...)
}

// ReadFile reads a JSON document from path.
func ReadFile(ctx context.Context, path string, opts ...Option) (*dataframe.DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Read(ctx, f, opts...)
}

// ReadURL fetches JSON from an http(s) URL and reads it into a DataFrame.
func ReadURL(ctx context.Context, url string, opts ...Option) (*dataframe.DataFrame, error) {
	cfg := resolve(opts)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := cfg.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("json.ReadURL: status %s for %s", resp.Status, url)
	}
	return Read(ctx, resp.Body, opts...)
}

// ReadNDJSON parses newline-delimited JSON (one object per line) into a
// DataFrame. Blank lines are skipped. A trailing newline is optional.
func ReadNDJSON(ctx context.Context, r io.Reader, opts ...Option) (*dataframe.DataFrame, error) {
	cfg := resolve(opts)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1<<20), 1<<26) // up to 64 MiB per line
	var rows []map[string]any
	var ord keyOrder
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		// Skip whitespace-only lines.
		trimmed := trimSpaces(line)
		if len(trimmed) == 0 {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal(trimmed, &row); err != nil {
			return nil, fmt.Errorf("json.ReadNDJSON: line %d: %w", len(rows)+1, err)
		}
		if err := ord.add(row, trimmed); err != nil {
			return nil, fmt.Errorf("json.ReadNDJSON: line %d: %w", len(rows)+1, err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("json.ReadNDJSON: %w", err)
	}
	return buildFromRowsOrdered(ctx, rows, ord.names, cfg)
}

// ReadNDJSONString is a convenience that parses an NDJSON-formatted string.
func ReadNDJSONString(ctx context.Context, s string, opts ...Option) (*dataframe.DataFrame, error) {
	return ReadNDJSON(ctx, strings.NewReader(s), opts...)
}

// ReadNDJSONFile reads newline-delimited JSON from path.
func ReadNDJSONFile(ctx context.Context, path string, opts ...Option) (*dataframe.DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ReadNDJSON(ctx, f, opts...)
}

// ReadNDJSONURL fetches NDJSON from an http(s) URL.
func ReadNDJSONURL(ctx context.Context, url string, opts ...Option) (*dataframe.DataFrame, error) {
	cfg := resolve(opts)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := cfg.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("json.ReadNDJSONURL: status %s for %s", resp.Status, url)
	}
	return ReadNDJSON(ctx, resp.Body, opts...)
}

// Write writes df as a JSON array of objects (one object per row), with
// keys in column order as polars.DataFrame.write_json does. NaN and
// infinities have no JSON form and are written as null, also matching
// polars. The array is followed by a newline.
func Write(ctx context.Context, w io.Writer, df *dataframe.DataFrame) error {
	rw, err := newRowWriter(df)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(w)
	bw.WriteByte('[')
	for i := range df.Height() {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if i > 0 {
			bw.WriteByte(',')
		}
		if err := rw.writeRow(bw, i); err != nil {
			return err
		}
	}
	bw.WriteString("]\n")
	return bw.Flush()
}

// rowWriter encodes one DataFrame row as a JSON object. Keys are
// pre-encoded once; values go through encoding/json.
type rowWriter struct {
	keys [][]byte
	cols []arrow.Array
}

func newRowWriter(df *dataframe.DataFrame) (*rowWriter, error) {
	rw := &rowWriter{}
	if df.Height() == 0 {
		return rw, nil // a zero-row series may have no chunk to read
	}
	for _, s := range df.Columns() {
		key, err := json.Marshal(s.Name())
		if err != nil {
			return nil, err
		}
		rw.keys = append(rw.keys, key)
		rw.cols = append(rw.cols, s.Chunk(0))
	}
	return rw, nil
}

func (rw *rowWriter) writeRow(bw *bufio.Writer, i int) error {
	bw.WriteByte('{')
	for c, col := range rw.cols {
		if c > 0 {
			bw.WriteByte(',')
		}
		bw.Write(rw.keys[c])
		bw.WriteByte(':')
		v := arrowCellToGo(col, i)
		if f, isFloat := v.(float64); isFloat && (math.IsNaN(f) || math.IsInf(f, 0)) {
			v = nil
		}
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("json: column %s row %d: %w", rw.keys[c], i, err)
		}
		bw.Write(b)
	}
	bw.WriteByte('}')
	return nil
}

// WriteFile writes df as a JSON array of objects to path.
func WriteFile(ctx context.Context, path string, df *dataframe.DataFrame) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := Write(ctx, f, df); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// WriteNDJSONFile writes df as newline-delimited JSON to path.
func WriteNDJSONFile(ctx context.Context, path string, df *dataframe.DataFrame) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := WriteNDJSON(ctx, f, df); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// WriteNDJSON writes df as newline-delimited JSON, one object per row
// with keys in column order. NaN and infinities are written as null.
func WriteNDJSON(ctx context.Context, w io.Writer, df *dataframe.DataFrame) error {
	rw, err := newRowWriter(df)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(w)
	for i := range df.Height() {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := rw.writeRow(bw, i); err != nil {
			return err
		}
		bw.WriteByte('\n')
	}
	return bw.Flush()
}

// keyOrder collects object keys in first-seen document order across
// rows. Go maps lose key order, so a row that introduces a new key is
// re-scanned for its key order; rows with only known keys cost a map
// lookup per key.
type keyOrder struct {
	names []string
	seen  map[string]struct{}
}

func (o *keyOrder) add(row map[string]any, raw []byte) error {
	fresh := false
	for k := range row {
		if _, ok := o.seen[k]; !ok {
			fresh = true
			break
		}
	}
	if !fresh {
		return nil
	}
	keys, err := objectKeys(raw)
	if err != nil {
		return err
	}
	o.addKeys(keys)
	return nil
}

func (o *keyOrder) addKeys(keys []string) {
	if o.seen == nil {
		o.seen = map[string]struct{}{}
	}
	for _, k := range keys {
		if _, ok := o.seen[k]; !ok {
			o.seen[k] = struct{}{}
			o.names = append(o.names, k)
		}
	}
}

// objectKeys returns the top-level keys of a JSON object in document
// order.
func objectKeys(raw []byte) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	t, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("expected a JSON object")
	}
	var keys []string
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return nil, err
		}
		k, _ := t.(string)
		keys = append(keys, k)
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return nil, err
		}
	}
	return keys, nil
}

// buildFromRowsOrdered materializes rows into a DataFrame with columns in
// colNames order (first appearance in the document, like polars).
// Missing fields become nulls; extra fields extend the schema.
func buildFromRowsOrdered(ctx context.Context, rows []map[string]any, colNames []string, cfg config) (*dataframe.DataFrame, error) {
	_ = ctx
	if len(rows) == 0 {
		return dataframe.New()
	}
	types := map[string]inferredType{}
	for _, r := range rows {
		for k, v := range r {
			types[k] = promote(types[k], inferValue(v))
		}
	}
	// Build each column.
	ss := make([]*series.Series, 0, len(colNames))
	release := func() {
		for _, s := range ss {
			s.Release()
		}
	}
	for _, name := range colNames {
		s, err := materializeCol(name, rows, types[name], cfg.alloc, true)
		if err != nil {
			release()
			return nil, err
		}
		ss = append(ss, s)
	}
	return dataframe.New(ss...)
}

// buildFromColumnsOrdered handles the object-of-arrays input shape, with
// columns in document key order like polars.
func buildFromColumnsOrdered(ctx context.Context, cols map[string][]any, names []string, cfg config) (*dataframe.DataFrame, error) {
	_ = ctx
	if len(cols) == 0 {
		return dataframe.New()
	}
	ss := make([]*series.Series, 0, len(names))
	release := func() {
		for _, s := range ss {
			s.Release()
		}
	}
	for _, name := range names {
		arr := cols[name]
		t := tUnknown
		for _, v := range arr {
			t = promote(t, inferValue(v))
		}
		s, err := materializeColFromArr(name, arr, t, cfg.alloc)
		if err != nil {
			release()
			return nil, err
		}
		ss = append(ss, s)
	}
	return dataframe.New(ss...)
}

func materializeCol(name string, rows []map[string]any, t inferredType, mem memory.Allocator, byRow bool) (*series.Series, error) {
	n := len(rows)
	switch t {
	case tBool:
		vals := make([]bool, n)
		valid := make([]bool, n)
		for i, r := range rows {
			v, ok := r[name]
			if !ok || v == nil {
				continue
			}
			if b, ok2 := v.(bool); ok2 {
				vals[i] = b
				valid[i] = true
			}
		}
		return series.FromBool(name, vals, compactValid(valid), series.WithAllocator(mem))
	case tInt:
		vals := make([]int64, n)
		valid := make([]bool, n)
		for i, r := range rows {
			v, ok := r[name]
			if !ok || v == nil {
				continue
			}
			if f, ok2 := v.(float64); ok2 {
				vals[i] = int64(f)
				valid[i] = true
			}
		}
		return series.FromInt64(name, vals, compactValid(valid), series.WithAllocator(mem))
	case tFloat:
		vals := make([]float64, n)
		valid := make([]bool, n)
		for i, r := range rows {
			v, ok := r[name]
			if !ok || v == nil {
				continue
			}
			if f, ok2 := v.(float64); ok2 {
				vals[i] = f
				valid[i] = true
			}
		}
		return series.FromFloat64(name, vals, compactValid(valid), series.WithAllocator(mem))
	case tString, tMixed, tUnknown:
		vals := make([]string, n)
		valid := make([]bool, n)
		for i, r := range rows {
			v, ok := r[name]
			if !ok || v == nil {
				continue
			}
			vals[i] = fmt.Sprint(v)
			valid[i] = true
		}
		return series.FromString(name, vals, compactValid(valid), series.WithAllocator(mem))
	}
	return nil, fmt.Errorf("json: unexpected inferred type %v for column %q", t, name)
}

func materializeColFromArr(name string, arr []any, t inferredType, mem memory.Allocator) (*series.Series, error) {
	n := len(arr)
	switch t {
	case tBool:
		vals := make([]bool, n)
		valid := make([]bool, n)
		for i, v := range arr {
			if v == nil {
				continue
			}
			if b, ok := v.(bool); ok {
				vals[i] = b
				valid[i] = true
			}
		}
		return series.FromBool(name, vals, compactValid(valid), series.WithAllocator(mem))
	case tInt:
		vals := make([]int64, n)
		valid := make([]bool, n)
		for i, v := range arr {
			if v == nil {
				continue
			}
			if f, ok := v.(float64); ok {
				vals[i] = int64(f)
				valid[i] = true
			}
		}
		return series.FromInt64(name, vals, compactValid(valid), series.WithAllocator(mem))
	case tFloat:
		vals := make([]float64, n)
		valid := make([]bool, n)
		for i, v := range arr {
			if v == nil {
				continue
			}
			if f, ok := v.(float64); ok {
				vals[i] = f
				valid[i] = true
			}
		}
		return series.FromFloat64(name, vals, compactValid(valid), series.WithAllocator(mem))
	case tString, tMixed, tUnknown:
		vals := make([]string, n)
		valid := make([]bool, n)
		for i, v := range arr {
			if v == nil {
				continue
			}
			vals[i] = fmt.Sprint(v)
			valid[i] = true
		}
		return series.FromString(name, vals, compactValid(valid), series.WithAllocator(mem))
	}
	return nil, fmt.Errorf("json: unexpected inferred type %v for column %q", t, name)
}

// arrowCellToGo converts an arrow cell at position i to a Go value suitable
// for encoding/json. Numeric types come out as int64/float64 so JSON shows
// real numbers (not quoted strings). Invalid cells return nil (-> JSON null).
func arrowCellToGo(arr any, i int) any {
	type indexable interface {
		IsValid(int) bool
	}
	if a, ok := arr.(indexable); ok && !a.IsValid(i) {
		return nil
	}
	switch a := arr.(type) {
	case *array.Int8:
		return int64(a.Value(i))
	case *array.Int16:
		return int64(a.Value(i))
	case *array.Int32:
		return int64(a.Value(i))
	case *array.Int64:
		return a.Value(i)
	case *array.Uint8:
		return int64(a.Value(i))
	case *array.Uint16:
		return int64(a.Value(i))
	case *array.Uint32:
		return int64(a.Value(i))
	case *array.Uint64:
		return a.Value(i)
	case *array.Float32:
		return float64(a.Value(i))
	case *array.Float64:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	case *array.String:
		return a.Value(i)
	}
	// Fallback to string representation for unsupported types.
	type valuer interface {
		ValueStr(int) string
	}
	if v, ok := arr.(valuer); ok {
		return v.ValueStr(i)
	}
	return nil
}

// compactValid returns nil when every entry is true (avoiding an unused
// validity bitmap), otherwise returns valid unchanged.
func compactValid(valid []bool) []bool {
	for _, v := range valid {
		if !v {
			return valid
		}
	}
	return nil
}

func skipWS(br *bufio.Reader) (byte, error) {
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		return b, nil
	}
}

func trimSpaces(b []byte) []byte {
	i := 0
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\r') {
		i++
	}
	j := len(b)
	for j > i && (b[j-1] == ' ' || b[j-1] == '\t' || b[j-1] == '\r') {
		j--
	}
	return b[i:j]
}
