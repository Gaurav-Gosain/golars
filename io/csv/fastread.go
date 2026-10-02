package csv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/internal/mmapfile"
	"github.com/Gaurav-Gosain/golars/series"
)

// inferRows is how many data rows type inference samples (polars'
// infer_schema_length default).
const inferRows = 100

// targetChunkBytes is the input size each parse task aims for.
const targetChunkBytes = 512 << 10

// maxStringBytes bounds the data of a single-chunk string column (int32
// offsets). Larger columns keep one arrow chunk per parse task. It is a
// variable so tests can exercise that path.
var maxStringBytes = math.MaxInt32

// errFallback signals that the input needs the arrow-based reader.
var errFallback = errors.New("csv: fallback to arrow reader")

var errInvalidUTF8 = errors.New("invalid UTF-8 in a string column")

func firstInvalidUTF8(b []byte) int {
	for i := 0; i < len(b); {
		r, size := utf8.DecodeRune(b[i:])
		if r == utf8.RuneError && size <= 1 {
			return i
		}
		i += size
	}
	return 0
}

// colSpec describes one output column.
type colSpec struct {
	name     string
	src      int // field index in the file
	kind     kind
	dt       arrow.DataType
	explicit bool     // from WithSchema: parse failures are errors
	samples  []string // inference sample (non-null values)
}

// chunkCol accumulates one column of one parse task.
type chunkCol struct {
	kind    kind
	i64     []int64
	f64     []float64
	i32     []int32
	bits    []byte // boolean values
	valid   []byte // validity, allocated on the first null
	nulls   int
	offs    []int32 // string offsets, len ub+1
	data    []byte  // string bytes
	bld     array.Builder
	failed  bool
	failVal string
	failPos int
	ub      int
}

type chunkState struct {
	lo, hi int // byte range of the task in the input
	ub     int // upper bound on records in the range
	ubOff  int // row offset of the task in the fixed-width buffers
	rows   int
	rowOff int
	cols   []chunkCol
	err    error
	errPos int
}

type fastReader struct {
	cfg      config
	data     []byte
	body     int // offset of the first data record
	delim    byte
	quotes   bool
	ncols    int
	colMap   []int // file field index -> output column or -1
	specs    []colSpec
	nulls    []string
	checkUTF bool
}

// readNative parses data with the native parallel reader. It returns
// errFallback when the input uses a feature it does not support.
func readNative(ctx context.Context, data []byte, cfg config) (*dataframe.DataFrame, error) {
	// Per-column type overrides (used by try_parse_dates=false) are only
	// implemented by the arrow reader.
	if len(cfg.columnTypes) > 0 {
		return nil, errFallback
	}
	if cfg.delimiter <= 0 || cfg.delimiter >= utf8.RuneSelf || cfg.delimiter == '"' || cfg.delimiter == '\n' || cfg.delimiter == '\r' {
		return nil, errFallback
	}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	r := &fastReader{
		cfg:    cfg,
		data:   data,
		delim:  byte(cfg.delimiter),
		quotes: bytes.IndexByte(data, '"') >= 0,
		nulls:  cfg.nullValues,
	}
	if err := r.readHeader(); err != nil {
		return nil, err
	}
	if r.specs == nil {
		return nil, errFallback
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		df, retry, err := r.parseAll(ctx)
		if err != nil || !retry {
			return df, err
		}
	}
}

func (r *fastReader) lineAt(pos int) int {
	return 1 + bytes.Count(r.data[:pos], []byte{'\n'})
}

func (r *fastReader) readHeader() error {
	cfg := r.cfg
	tok := tokenizer{data: r.data, delim: r.delim, quotes: r.quotes}
	pos := skipBlank(r.data, 0)
	var sch *arrow.Schema = cfg.schema

	if pos >= len(r.data) {
		if sch != nil {
			r.ncols = sch.NumFields()
			r.body = len(r.data)
			return r.buildSpecs(sch, nil)
		}
		return errors.New("csv: empty input, no header row")
	}
	first, next, err := tok.record(pos, nil)
	if err != nil {
		return fmt.Errorf("csv: line %d: %w", r.lineAt(pos), err)
	}
	r.ncols = len(first)
	var names []string
	if cfg.hasHeader {
		names = first
		r.body = next
	} else {
		r.body = pos
		if sch == nil {
			names = make([]string, len(first))
			for i := range names {
				names[i] = fmt.Sprintf("f%d", i)
			}
		}
	}
	if sch != nil && sch.NumFields() != r.ncols {
		return fmt.Errorf("csv: schema has %d fields but the file has %d columns", sch.NumFields(), r.ncols)
	}
	return r.buildSpecs(sch, names)
}

func (r *fastReader) buildSpecs(sch *arrow.Schema, names []string) error {
	if names == nil {
		names = make([]string, sch.NumFields())
		for i := range names {
			names[i] = sch.Field(i).Name
		}
	}
	var order []int
	if len(r.cfg.includeCols) > 0 {
		idx := make(map[string]int, len(names))
		for i, n := range names {
			if _, dup := idx[n]; !dup {
				idx[n] = i
			}
		}
		for _, n := range r.cfg.includeCols {
			i, ok := idx[n]
			if !ok {
				return fmt.Errorf("csv: column %q in included columns does not exist in the CSV file", n)
			}
			order = append(order, i)
		}
	} else {
		order = make([]int, len(names))
		for i := range order {
			order[i] = i
		}
	}
	r.colMap = make([]int, r.ncols)
	for i := range r.colMap {
		r.colMap[i] = -1
	}
	r.specs = make([]colSpec, len(order))
	for o, i := range order {
		if r.colMap[i] >= 0 {
			return fmt.Errorf("csv: column %q included more than once", names[i])
		}
		r.colMap[i] = o
		sp := colSpec{name: names[i], src: i}
		if sch != nil {
			sp.dt = sch.Field(i).Type
			sp.kind = kindFor(sp.dt)
			sp.explicit = true
		}
		r.specs[o] = sp
	}
	if sch == nil {
		return r.infer()
	}
	return nil
}

// infer samples the first inferRows records and picks, per column, the
// first kind in inferOrder that parses every non-null sample.
func (r *fastReader) infer() error {
	tok := tokenizer{data: r.data, delim: r.delim, quotes: r.quotes}
	pos := r.body
	var rec []string
	for range inferRows {
		pos = skipBlank(r.data, pos)
		if pos >= len(r.data) {
			break
		}
		start := pos
		var err error
		rec, pos, err = tok.record(pos, rec)
		if err != nil {
			return fmt.Errorf("csv: line %d: %w", r.lineAt(start), err)
		}
		if len(rec) != r.ncols {
			return fmt.Errorf("csv: line %d: expected %d fields, found %d", r.lineAt(start), r.ncols, len(rec))
		}
		for i, v := range rec {
			if o := r.colMap[i]; o >= 0 && !r.isNullStr(v, false) {
				r.specs[o].samples = append(r.specs[o].samples, v)
			}
		}
	}
	for i := range r.specs {
		sp := &r.specs[i]
		sp.kind = kString
		if len(sp.samples) > 0 {
			sp.kind = pickKind(sp.samples, 0, "")
		}
		sp.dt = kindArrow(sp.kind)
	}
	return nil
}

// pickKind returns the first kind at or after inferOrder[from] that
// parses extra (when non-empty) and every sample.
func pickKind(samples []string, from int, extra string) kind {
	for _, k := range inferOrder[from:] {
		if extra != "" && !parsesAs(extra, k) {
			continue
		}
		ok := true
		for _, s := range samples {
			if !parsesAs(s, k) {
				ok = false
				break
			}
		}
		if ok {
			return k
		}
	}
	return kString
}

func (r *fastReader) isNullStr(v string, stringCol bool) bool {
	if v == "" && !stringCol {
		return true
	}
	for _, n := range r.nulls {
		if v == n {
			return true
		}
	}
	return false
}

func parallelFor(n int, f func(i int)) {
	workers := min(n, runtime.GOMAXPROCS(0))
	if workers <= 1 {
		for i := range n {
			f(i)
		}
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	var fault atomic.Pointer[error]
	for range workers {
		wg.Go(func() {
			// The input may be a memory mapping. A fault (the file
			// shrank) is recovered here and re-raised in the caller,
			// whose guard turns it into an error.
			var err error
			defer func() {
				if err != nil {
					fault.CompareAndSwap(nil, &err)
				}
			}()
			defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
			defer mmapfile.Recover(&err)
			for fault.Load() == nil {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				f(i)
			}
		})
	}
	wg.Wait()
	if err := fault.Load(); err != nil {
		panic(faultPanic{*err})
	}
}

// faultPanic carries a memory fault from a worker to the goroutine that
// started it. It satisfies mmapfile.IsFault.
type faultPanic struct{ err error }

func (f faultPanic) Error() string { return f.err.Error() }
func (f faultPanic) Unwrap() error { return f.err }
func (f faultPanic) Addr() uintptr { return 0 }

// fixedBuffers holds the per-column fixed-width output buffers shared by
// all parse tasks (each task writes its own row range).
type fixedBuffers struct {
	bufs []*memory.Buffer
}

func (fb *fixedBuffers) release() {
	for i, b := range fb.bufs {
		if b != nil {
			b.Release()
			fb.bufs[i] = nil
		}
	}
}

func fixedWidth(k kind) int {
	switch k {
	case kInt64, kFloat64:
		return 8
	case kDate32:
		return 4
	}
	return 0
}

// parseAll runs one full parallel parse. retry is true when inferred
// column kinds were widened and the input must be parsed again.
func (r *fastReader) parseAll(ctx context.Context) (df *dataframe.DataFrame, retry bool, err error) {
	body := r.data[r.body:]
	n := 1
	if len(body) > targetChunkBytes {
		n = min(len(body)/targetChunkBytes, 4*runtime.GOMAXPROCS(0))
	}
	r.checkUTF = false
	for _, sp := range r.specs {
		if sp.kind == kString && !sp.explicit {
			r.checkUTF = true
		}
	}

	chunks, err := r.parseChunks(body, n)
	if err != nil && n > 1 && r.quotes {
		// A quote-parity split can be wrong for input with bare quotes.
		// A single sequential task is always correct.
		chunks, err = r.parseChunks(body, 1)
	}
	if err != nil {
		return nil, false, err
	}
	fb := chunks.fb
	defer fb.release()

	// Widen inferred columns that failed to parse and try again.
	widened := false
	for _, cs := range chunks.cs {
		for t := range cs.cols {
			c := &cs.cols[t]
			if !c.failed {
				continue
			}
			sp := &r.specs[t]
			if sp.explicit {
				chunks.release()
				return nil, false, fmt.Errorf("csv: line %d: column %q: cannot parse %q as %s",
					r.lineAt(c.failPos), sp.name, c.failVal, sp.dt)
			}
			cur := 0
			for i, k := range inferOrder {
				if k == sp.kind {
					cur = i
				}
			}
			nk := pickKind(sp.samples, cur+1, c.failVal)
			if nk != sp.kind {
				sp.kind = nk
				sp.dt = kindArrow(nk)
				widened = true
			}
		}
	}
	if widened {
		chunks.release()
		return nil, true, nil
	}
	if err := ctx.Err(); err != nil {
		chunks.release()
		return nil, false, err
	}
	df, err = r.assemble(chunks)
	return df, false, err
}

type parsed struct {
	cs []*chunkState
	fb *fixedBuffers
}

func (p *parsed) release() {
	for _, cs := range p.cs {
		for i := range cs.cols {
			if b := cs.cols[i].bld; b != nil {
				b.Release()
				cs.cols[i].bld = nil
			}
		}
	}
	p.fb.release()
}

func (r *fastReader) parseChunks(body []byte, n int) (*parsed, error) {
	points := splitPoints(body, n, r.quotes)
	cs := make([]*chunkState, len(points)-1)
	for i := range cs {
		cs[i] = &chunkState{lo: r.body + points[i], hi: r.body + points[i+1]}
	}
	parallelFor(len(cs), func(i int) {
		c := cs[i]
		seg := r.data[c.lo:c.hi]
		c.ub = bytes.Count(seg, []byte{'\n'})
		if len(seg) > 0 && seg[len(seg)-1] != '\n' {
			c.ub++
		}
	})
	total := 0
	for _, c := range cs {
		c.ubOff = total
		total += c.ub
	}
	fb := &fixedBuffers{bufs: make([]*memory.Buffer, len(r.specs))}
	for t, sp := range r.specs {
		if w := fixedWidth(sp.kind); w > 0 {
			b := memory.NewResizableBuffer(r.cfg.alloc)
			b.Resize(total * w)
			fb.bufs[t] = b
		}
	}
	p := &parsed{cs: cs, fb: fb}
	parallelFor(len(cs), func(i int) { r.parseChunk(cs[i], fb) })
	for _, c := range cs {
		if c.err != nil {
			p.release()
			return nil, fmt.Errorf("csv: line %d: %w", r.lineAt(c.errPos), c.err)
		}
	}
	return p, nil
}

func (r *fastReader) initChunkCols(c *chunkState, fb *fixedBuffers) {
	c.cols = make([]chunkCol, len(r.specs))
	for t, sp := range r.specs {
		col := &c.cols[t]
		col.kind = sp.kind
		col.ub = c.ub
		switch sp.kind {
		case kInt64:
			col.i64 = arrow.Int64Traits.CastFromBytes(fb.bufs[t].Bytes())[c.ubOff : c.ubOff+c.ub]
		case kFloat64:
			col.f64 = arrow.Float64Traits.CastFromBytes(fb.bufs[t].Bytes())[c.ubOff : c.ubOff+c.ub]
		case kDate32:
			col.i32 = arrow.Int32Traits.CastFromBytes(fb.bufs[t].Bytes())[c.ubOff : c.ubOff+c.ub]
		case kBool:
			col.bits = make([]byte, bitutil.BytesForBits(int64(c.ub)))
		case kString:
			col.offs = make([]int32, 1, c.ub+1)
			est := 0
			for _, s := range sp.samples {
				est += len(s)
			}
			if len(sp.samples) > 0 {
				est = est / len(sp.samples) * c.ub * 9 / 8
			}
			col.data = make([]byte, 0, max(est, 64))
		default:
			col.bld = array.NewBuilder(r.cfg.alloc, sp.dt)
			col.bld.Reserve(c.ub)
		}
	}
}

func (r *fastReader) parseChunk(c *chunkState, fb *fixedBuffers) {
	if r.checkUTF && !utf8.Valid(r.data[c.lo:c.hi]) {
		c.err = errInvalidUTF8
		c.errPos = c.lo + firstInvalidUTF8(r.data[c.lo:c.hi])
		return
	}
	r.initChunkCols(c, fb)
	tok := tokenizer{data: r.data[:c.hi], delim: r.delim, quotes: r.quotes}
	colMap := r.colMap
	ncols := r.ncols
	fastNum := !numericNull(r.nulls)
	pos := c.lo
	row := 0
	for {
		pos = skipBlank(tok.data, pos)
		if pos >= c.hi {
			break
		}
		start := pos
		f := 0
		for {
			// Numbers parse in the same pass that finds the end of the
			// field; anything unusual goes through the tokenizer.
			if fastNum && f < ncols {
				if t := colMap[f]; t >= 0 {
					col := &c.cols[t]
					var next int
					var eor, ok bool
					switch col.kind {
					case kInt64:
						var v int64
						if v, next, eor, ok = intPrefix(tok.data, pos, r.delim); ok {
							col.i64[row] = v
						}
					case kFloat64:
						var v float64
						if v, next, eor, ok = floatPrefix(tok.data, pos, r.delim); ok {
							col.f64[row] = v
						}
					}
					if ok {
						if col.valid != nil {
							bitutil.SetBit(col.valid, row)
						}
						pos = next
						f++
						if eor {
							break
						}
						continue
					}
				}
			}
			field, next, eor, err := tok.field(pos)
			if err != nil {
				c.err, c.errPos = err, start
				return
			}
			pos = next
			if f < ncols {
				if t := colMap[f]; t >= 0 {
					col := &c.cols[t]
					if !r.appendValue(col, field, row) {
						col.failed = true
						col.failVal = string(field)
						col.failPos = start
						return
					}
				}
			}
			f++
			if eor {
				break
			}
		}
		if f != ncols {
			c.err = fmt.Errorf("expected %d fields, found %d", ncols, f)
			c.errPos = start
			return
		}
		row++
	}
	c.rows = row
}

func (col *chunkCol) setNull(row int) {
	if col.valid == nil {
		col.valid = make([]byte, bitutil.BytesForBits(int64(col.ub)))
		bitutil.SetBitsTo(col.valid, 0, int64(row), true)
	}
	col.nulls++
}

// appendValue stores field as row of col. It returns false when the
// value does not parse as the column kind.
func (r *fastReader) appendValue(col *chunkCol, f []byte, row int) bool {
	switch col.kind {
	case kInt64:
		if len(f) == 0 || (r.nulls != nil && r.isNullBytes(f)) {
			col.i64[row] = 0
			col.setNull(row)
			return true
		}
		v, ok := parseInt64(f)
		if !ok {
			return false
		}
		col.i64[row] = v
	case kFloat64:
		if len(f) == 0 || (r.nulls != nil && r.isNullBytes(f)) {
			col.f64[row] = 0
			col.setNull(row)
			return true
		}
		v, ok := parseFloat64(f)
		if !ok {
			return false
		}
		col.f64[row] = v
	case kString:
		if r.nulls != nil && r.isNullBytes(f) {
			col.offs = append(col.offs, int32(len(col.data)))
			col.setNull(row)
			return true
		}
		col.data = append(col.data, f...)
		col.offs = append(col.offs, int32(len(col.data)))
	case kBool:
		if len(f) == 0 || (r.nulls != nil && r.isNullBytes(f)) {
			col.setNull(row)
			return true
		}
		v, ok := parseBool(f, true)
		if !ok {
			return false
		}
		if v {
			bitutil.SetBit(col.bits, row)
		}
	case kDate32:
		if len(f) == 0 || (r.nulls != nil && r.isNullBytes(f)) {
			col.i32[row] = 0
			col.setNull(row)
			return true
		}
		v, ok := parseDate32(f)
		if !ok {
			return false
		}
		col.i32[row] = v
	default:
		if len(f) == 0 || (r.nulls != nil && r.isNullBytes(f)) {
			col.bld.AppendNull()
			return true
		}
		if err := col.bld.AppendValueFromString(string(f)); err != nil {
			return false
		}
		return true
	}
	if col.valid != nil {
		bitutil.SetBit(col.valid, row)
	}
	return true
}

func (r *fastReader) isNullBytes(f []byte) bool {
	s := unsafeString(f)
	for _, n := range r.nulls {
		if s == n {
			return true
		}
	}
	return false
}

// assemble stitches the per-task results into one arrow array per
// column and builds the DataFrame.
func (r *fastReader) assemble(p *parsed) (*dataframe.DataFrame, error) {
	defer p.release()
	total := 0
	contiguous := true
	for _, c := range p.cs {
		c.rowOff = total
		total += c.rows
		if c.rows != c.ub {
			contiguous = false
		}
	}
	cols := make([]*series.Series, len(r.specs))
	errs := make([]error, len(r.specs))
	parallelFor(len(r.specs), func(t int) {
		cols[t], errs[t] = r.assembleColumn(p, t, total, contiguous)
	})
	if err := errors.Join(errs...); err != nil {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
		return nil, err
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		for _, c := range cols {
			c.Release()
		}
		return nil, fmt.Errorf("csv: build dataframe: %w", err)
	}
	return df, nil
}

func (r *fastReader) validity(p *parsed, t, total int) (*memory.Buffer, int) {
	nulls := 0
	for _, c := range p.cs {
		nulls += c.cols[t].nulls
	}
	if nulls == 0 {
		return nil, 0
	}
	vb := memory.NewResizableBuffer(r.cfg.alloc)
	vb.Resize(int(bitutil.BytesForBits(int64(total))))
	bits := vb.Bytes()
	for _, c := range p.cs {
		col := &c.cols[t]
		if col.valid == nil {
			bitutil.SetBitsTo(bits, int64(c.rowOff), int64(c.rows), true)
		} else {
			bitutil.CopyBitmap(col.valid, 0, c.rows, bits, c.rowOff)
		}
	}
	return vb, nulls
}

func (r *fastReader) assembleColumn(p *parsed, t, total int, contiguous bool) (*series.Series, error) {
	sp := r.specs[t]
	if total == 0 {
		return series.Empty(sp.name, dtype.FromArrow(sp.dt)), nil
	}
	var arr arrow.Array
	switch sp.kind {
	case kInt64, kFloat64, kDate32:
		w := fixedWidth(sp.kind)
		buf := p.fb.bufs[t]
		if !contiguous {
			b := buf.Bytes()
			for _, c := range p.cs {
				copy(b[c.rowOff*w:(c.rowOff+c.rows)*w], b[c.ubOff*w:(c.ubOff+c.rows)*w])
			}
		}
		buf.ResizeNoShrink(total * w)
		vb, nulls := r.validity(p, t, total)
		data := array.NewData(sp.dt, total, []*memory.Buffer{vb, buf}, nil, nulls, 0)
		arr = array.MakeFromData(data)
		data.Release()
		if vb != nil {
			vb.Release()
		}
	case kBool:
		bb := memory.NewResizableBuffer(r.cfg.alloc)
		bb.Resize(int(bitutil.BytesForBits(int64(total))))
		for _, c := range p.cs {
			bitutil.CopyBitmap(c.cols[t].bits, 0, c.rows, bb.Bytes(), c.rowOff)
		}
		vb, nulls := r.validity(p, t, total)
		data := array.NewData(sp.dt, total, []*memory.Buffer{vb, bb}, nil, nulls, 0)
		arr = array.MakeFromData(data)
		data.Release()
		bb.Release()
		if vb != nil {
			vb.Release()
		}
	case kString:
		return r.assembleString(p, t, total)
	default:
		arrs := make([]arrow.Array, 0, len(p.cs))
		for _, c := range p.cs {
			arrs = append(arrs, c.cols[t].bld.NewArray())
		}
		if len(arrs) == 1 {
			arr = arrs[0]
		} else {
			var err error
			arr, err = array.Concatenate(arrs, r.cfg.alloc)
			for _, a := range arrs {
				a.Release()
			}
			if err != nil {
				return nil, fmt.Errorf("csv: column %q: %w", sp.name, err)
			}
		}
	}
	return series.New(sp.name, arr)
}

func (r *fastReader) assembleString(p *parsed, t, total int) (*series.Series, error) {
	sp := r.specs[t]
	dataLen := 0
	for _, c := range p.cs {
		dataLen += len(c.cols[t].data)
	}
	if dataLen > maxStringBytes {
		// Too large for int32 offsets in one array: one chunk per task.
		var arrs []arrow.Array
		for _, c := range p.cs {
			if c.rows == 0 {
				continue
			}
			col := &c.cols[t]
			ob := memory.NewResizableBuffer(r.cfg.alloc)
			ob.Resize((c.rows + 1) * 4)
			copy(arrow.Int32Traits.CastFromBytes(ob.Bytes()), col.offs)
			db := memory.NewResizableBuffer(r.cfg.alloc)
			db.Resize(len(col.data))
			copy(db.Bytes(), col.data)
			var vb *memory.Buffer
			if col.valid != nil {
				vb = memory.NewResizableBuffer(r.cfg.alloc)
				vb.Resize(int(bitutil.BytesForBits(int64(c.rows))))
				bitutil.CopyBitmap(col.valid, 0, c.rows, vb.Bytes(), 0)
			}
			data := array.NewData(sp.dt, c.rows, []*memory.Buffer{vb, ob, db}, nil, col.nulls, 0)
			arrs = append(arrs, array.MakeFromData(data))
			data.Release()
			ob.Release()
			db.Release()
			if vb != nil {
				vb.Release()
			}
		}
		return series.New(sp.name, arrs...)
	}
	ob := memory.NewResizableBuffer(r.cfg.alloc)
	ob.Resize((total + 1) * 4)
	db := memory.NewResizableBuffer(r.cfg.alloc)
	db.Resize(dataLen)
	offs := arrow.Int32Traits.CastFromBytes(ob.Bytes())
	dst := db.Bytes()
	base := make([]int, len(p.cs))
	acc := 0
	for i, c := range p.cs {
		base[i] = acc
		acc += len(c.cols[t].data)
	}
	parallelFor(len(p.cs), func(i int) {
		c := p.cs[i]
		col := &c.cols[t]
		copy(dst[base[i]:], col.data)
		b := int32(base[i])
		out := offs[c.rowOff : c.rowOff+c.rows]
		for j := range out {
			out[j] = b + col.offs[j]
		}
	})
	offs[total] = int32(dataLen)
	vb, nulls := r.validity(p, t, total)
	data := array.NewData(sp.dt, total, []*memory.Buffer{vb, ob, db}, nil, nulls, 0)
	arr := array.MakeFromData(data)
	data.Release()
	ob.Release()
	db.Release()
	if vb != nil {
		vb.Release()
	}
	return series.New(sp.name, arr)
}
