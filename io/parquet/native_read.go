package parquet

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	pqschema "github.com/apache/arrow-go/v18/parquet/schema"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/internal/mmapfile"
	"github.com/Gaurav-Gosain/golars/series"
)

// The native reader decodes flat columns (primitive, string, binary and
// temporal types with at most one definition level) straight from the
// page bytes into one contiguous arrow buffer per column. Column chunks
// of every row group decode in parallel. Anything it does not handle
// (nested types, dictionary or decimal arrow types, delta and
// byte-stream-split encodings, encryption) goes through pqarrow.

// errUnsupported makes the whole read fall back to pqarrow. It is
// returned when a page uses a feature the column metadata did not
// announce.
var errUnsupported = errors.New("parquet: unsupported by the native reader")

// pqarrowReads counts columns and whole reads that went through pqarrow
// (tests check that the native path was taken).
var pqarrowReads atomic.Int64

type nkind uint8

const (
	nkFixed  nkind = iota // fixed-width values copied at their physical width
	nkNarrow              // INT32 values narrowed to 1 or 2 bytes
	nkBool
	nkBytes
)

type nativeCol struct {
	field  arrow.Field
	leaf   int
	kind   nkind
	pw     int // physical value width in bytes (4 or 8)
	ow     int // output value width in bytes
	maxDef int16
	large  bool // 64-bit string offsets

	total int
	vals  *memory.Buffer // fixed-width values or bool bits
	offs  *memory.Buffer // string offsets
	tasks []*colTask
}

type colTask struct {
	col    *nativeCol
	rg     int
	rowOff int
	rows   int
	size   int64

	valid []byte // task-local validity, nil when the task has no nulls
	nulls int
	bits  []byte // task-local boolean values
	data  []byte // task-local string bytes
}

// nativePlan splits the projected fields into natively decoded columns
// and pqarrow leaves.
type nativePlan struct {
	fields   []arrow.Field
	native   []*nativeCol // by output position, nil for fallback fields
	fbLeaves []int
	fbPos    []int // output positions of fallback fields
}

func planNative(pf *file.Reader, m *pqarrow.SchemaManifest, names []string) (*nativePlan, error) {
	idx, err := projectFields(m, names)
	if err != nil {
		return nil, err
	}
	md := pf.MetaData()
	old := md.WriterVersion() != nil && md.WriterVersion().LessThan(metadata.Parquet816FixedVersion)
	p := &nativePlan{native: make([]*nativeCol, len(idx))}
	for o, i := range idx {
		sf := &m.Fields[i]
		p.fields = append(p.fields, *sf.Field)
		var nc *nativeCol
		if !old && sf.IsLeaf() && len(sf.Children) == 0 {
			nc = classify(sf, md)
		}
		if nc == nil {
			p.fbLeaves = appendLeaves(p.fbLeaves, sf)
			pqarrowReads.Add(1)
			p.fbPos = append(p.fbPos, o)
			continue
		}
		p.native[o] = nc
	}
	return p, nil
}

// projectFields returns manifest field indices in output order.
func projectFields(m *pqarrow.SchemaManifest, names []string) ([]int, error) {
	if names == nil {
		out := make([]int, len(m.Fields))
		for i := range out {
			out[i] = i
		}
		return out, nil
	}
	byName := make(map[string]int, len(m.Fields))
	for i := range m.Fields {
		byName[m.Fields[i].Field.Name] = i
	}
	seen := make(map[string]bool, len(names))
	out := make([]int, 0, len(names))
	for _, n := range names {
		i, ok := byName[n]
		if !ok {
			return nil, fmt.Errorf("column %q not found", n)
		}
		if seen[n] {
			return nil, fmt.Errorf("column %q requested more than once", n)
		}
		seen[n] = true
		out = append(out, i)
	}
	return out, nil
}

// classify returns a native column for sf or nil when pqarrow must read
// it.
func classify(sf *pqarrow.SchemaField, md *metadata.FileMetaData) *nativeCol {
	leaf := sf.ColIndex
	if leaf < 0 || leaf >= md.Schema.NumColumns() {
		return nil
	}
	desc := md.Schema.Column(leaf)
	if desc.MaxRepetitionLevel() != 0 || desc.MaxDefinitionLevel() > 1 {
		return nil
	}
	nc := &nativeCol{field: *sf.Field, leaf: leaf, maxDef: desc.MaxDefinitionLevel()}
	phys := desc.PhysicalType()
	lt := desc.LogicalType()
	unitOK := func(u arrow.TimeUnit) bool {
		var pu pqschema.TimeUnitType
		switch l := lt.(type) {
		case pqschema.TimestampLogicalType:
			pu = l.TimeUnit()
		case pqschema.TimeLogicalType:
			pu = l.TimeUnit()
		default:
			return false
		}
		switch u {
		case arrow.Millisecond:
			return pu == pqschema.TimeUnitMillis
		case arrow.Microsecond:
			return pu == pqschema.TimeUnitMicros
		case arrow.Nanosecond:
			return pu == pqschema.TimeUnitNanos
		}
		return false
	}
	ok := false
	switch t := sf.Field.Type.(type) {
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Uint8Type, *arrow.Uint16Type:
		if phys == parquet.Types.Int32 {
			nc.kind, nc.pw, nc.ow = nkNarrow, 4, t.(arrow.FixedWidthDataType).BitWidth()/8
			ok = true
		}
	case *arrow.Int32Type, *arrow.Uint32Type, *arrow.Date32Type:
		ok = phys == parquet.Types.Int32
		nc.kind, nc.pw, nc.ow = nkFixed, 4, 4
	case *arrow.Time32Type:
		ok = phys == parquet.Types.Int32 && unitOK(t.Unit)
		nc.kind, nc.pw, nc.ow = nkFixed, 4, 4
	case *arrow.Int64Type, *arrow.Uint64Type:
		ok = phys == parquet.Types.Int64
		nc.kind, nc.pw, nc.ow = nkFixed, 8, 8
	case *arrow.TimestampType:
		ok = phys == parquet.Types.Int64 && unitOK(t.Unit)
		nc.kind, nc.pw, nc.ow = nkFixed, 8, 8
	case *arrow.Time64Type:
		ok = phys == parquet.Types.Int64 && unitOK(t.Unit)
		nc.kind, nc.pw, nc.ow = nkFixed, 8, 8
	case *arrow.Float32Type:
		ok = phys == parquet.Types.Float
		nc.kind, nc.pw, nc.ow = nkFixed, 4, 4
	case *arrow.Float64Type:
		ok = phys == parquet.Types.Double
		nc.kind, nc.pw, nc.ow = nkFixed, 8, 8
	case *arrow.BooleanType:
		ok = phys == parquet.Types.Boolean
		nc.kind = nkBool
	case *arrow.StringType, *arrow.BinaryType:
		ok = phys == parquet.Types.ByteArray
		nc.kind = nkBytes
	case *arrow.LargeStringType, *arrow.LargeBinaryType:
		ok = phys == parquet.Types.ByteArray
		nc.kind, nc.large = nkBytes, true
	}
	if !ok {
		return nil
	}
	for r := range md.NumRowGroups() {
		cc, err := md.RowGroup(r).ColumnChunk(leaf)
		if err != nil || cc.CryptoMetadata() != nil || cc.FilePath() != "" {
			return nil
		}
		switch cc.Compression() {
		case compress.Codecs.Uncompressed, compress.Codecs.Snappy, compress.Codecs.Gzip,
			compress.Codecs.Brotli, compress.Codecs.Zstd, compress.Codecs.Lz4Raw:
		default:
			return nil
		}
		for _, e := range cc.Encodings() {
			switch int32(e) {
			case encPlain, encPlainDict, encRLEDict, encRLE:
			default:
				return nil
			}
		}
	}
	return nc
}

// readNativeTable reads the projected columns of pf. Fallback columns
// are decoded by pqarrow concurrently with the native ones.
func readNativeTable(ctx context.Context, r io.ReaderAt, pf *file.Reader, fr *pqarrow.FileReader, cfg config) (*dataframe.DataFrame, error) {
	return readNativeRowGroups(ctx, r, pf, fr, cfg, nil)
}

// readNativeRowGroups is readNativeTable restricted to the row groups
// rgs (nil means all).
func readNativeRowGroups(ctx context.Context, r io.ReaderAt, pf *file.Reader, fr *pqarrow.FileReader, cfg config,
	rgs []int) (*dataframe.DataFrame, error) {
	p, err := planNative(pf, fr.Manifest, cfg.columns)
	if err != nil {
		return nil, err
	}
	md := pf.MetaData()
	if rgs == nil {
		rgs = make([]int, md.NumRowGroups())
		for i := range rgs {
			rgs[i] = i
		}
	}

	var fbTable arrow.Table
	var fbErr error
	var fbWG sync.WaitGroup
	if len(p.fbLeaves) > 0 {
		fbWG.Go(func() {
			fbTable, fbErr = readLeaves(ctx, fr, p.fbLeaves, rgs)
		})
	}

	var tasks []*colTask
	for _, nc := range p.native {
		if nc == nil {
			continue
		}
		off := 0
		for _, rg := range rgs {
			rgm := md.RowGroup(rg)
			cc, err := rgm.ColumnChunk(nc.leaf)
			if err != nil {
				fbWG.Wait()
				releaseTable(fbTable)
				return nil, err
			}
			t := &colTask{col: nc, rg: rg, rowOff: off, rows: int(rgm.NumRows()), size: cc.TotalCompressedSize()}
			off += t.rows
			nc.tasks = append(nc.tasks, t)
			tasks = append(tasks, t)
		}
		nc.total = off
		nc.alloc(cfg.alloc)
	}
	// Largest chunks first so the tail of the schedule is short.
	slices.SortStableFunc(tasks, func(a, b *colTask) int { return cmp.Compare(b.size, a.size) })

	errs := make([]error, len(tasks))
	var next atomic.Int64
	var wg sync.WaitGroup
	workers := min(len(tasks), runtime.GOMAXPROCS(0))
	for range workers {
		wg.Go(func() {
			d := decoderPool.Get().(*chunkDecoder)
			d.r, d.md = r, md
			defer func() {
				d.r, d.md, d.t, d.codec = nil, nil, nil, nil
				d.dict, d.dictOffs, d.dictData = nil, nil, nil
				decoderPool.Put(d)
			}()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(tasks) || ctx.Err() != nil {
					return
				}
				errs[i] = d.safeDecode(tasks[i])
			}
		})
	}
	wg.Wait()
	fbWG.Wait()

	cols := make([]*series.Series, len(p.fields))
	release := func() {
		for _, nc := range p.native {
			if nc != nil {
				nc.release()
			}
		}
		releaseTable(fbTable)
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}
	if err := errors.Join(append(errs, fbErr, ctx.Err())...); err != nil {
		release()
		return nil, err
	}
	for o, nc := range p.native {
		if nc == nil {
			continue
		}
		s, err := nc.finish(cfg.alloc)
		if err != nil {
			release()
			return nil, err
		}
		cols[o] = s
	}
	for i, o := range p.fbPos {
		col := fbTable.Column(i)
		cols[o] = series.FromChunked(p.fields[o].Name, col.Data())
	}
	releaseTable(fbTable)
	fbTable = nil
	for _, nc := range p.native {
		if nc != nil {
			nc.release()
		}
	}
	df, err := dataframe.New(cols...)
	if err != nil {
		for _, c := range cols {
			c.Release()
		}
		return nil, err
	}
	return df, nil
}

func releaseTable(t arrow.Table) {
	if t != nil {
		t.Release()
	}
}

// readLeaves decodes the given leaves of the row groups rgs with
// pqarrow.
func readLeaves(ctx context.Context, fr *pqarrow.FileReader, leaves []int, rgs []int) (arrow.Table, error) {
	if len(rgs) <= 1 {
		return fr.ReadRowGroups(ctx, leaves, rgs)
	}
	return readRowGroupsParallel(ctx, fr, leaves, rgs)
}

func (nc *nativeCol) alloc(mem memory.Allocator) {
	switch nc.kind {
	case nkFixed, nkNarrow:
		nc.vals = memory.NewResizableBuffer(mem)
		nc.vals.Resize(nc.total * nc.ow)
	case nkBytes:
		w := 4
		if nc.large {
			w = 8
		}
		nc.offs = memory.NewResizableBuffer(mem)
		nc.offs.Resize((nc.total + 1) * w)
	}
}

func (nc *nativeCol) release() {
	if nc.vals != nil {
		nc.vals.Release()
		nc.vals = nil
	}
	if nc.offs != nil {
		nc.offs.Release()
		nc.offs = nil
	}
}

// validity merges the task-local validity bitmaps.
func (nc *nativeCol) validity(mem memory.Allocator) (*memory.Buffer, int) {
	nulls := 0
	for _, t := range nc.tasks {
		nulls += t.nulls
	}
	if nulls == 0 {
		return nil, 0
	}
	vb := memory.NewResizableBuffer(mem)
	vb.Resize(int(bitutil.BytesForBits(int64(nc.total))))
	bits := vb.Bytes()
	for _, t := range nc.tasks {
		if t.valid == nil {
			bitutil.SetBitsTo(bits, int64(t.rowOff), int64(t.rows), true)
		} else {
			bitutil.CopyBitmap(t.valid, 0, t.rows, bits, t.rowOff)
		}
	}
	return vb, nulls
}

// finish builds the output series. Buffers move into the array.
func (nc *nativeCol) finish(mem memory.Allocator) (*series.Series, error) {
	vb, nulls := nc.validity(mem)
	defer func() {
		if vb != nil {
			vb.Release()
		}
	}()
	var bufs []*memory.Buffer
	switch nc.kind {
	case nkFixed, nkNarrow:
		bufs = []*memory.Buffer{vb, nc.vals}
	case nkBool:
		bb := memory.NewResizableBuffer(mem)
		bb.Resize(int(bitutil.BytesForBits(int64(nc.total))))
		for _, t := range nc.tasks {
			if t.rows > 0 {
				bitutil.CopyBitmap(t.bits, 0, t.rows, bb.Bytes(), t.rowOff)
			}
		}
		defer bb.Release()
		bufs = []*memory.Buffer{vb, bb}
	case nkBytes:
		db, err := nc.joinBytes(mem)
		if err != nil {
			return nil, err
		}
		if db == nil {
			return nc.bytesChunks(mem, vb)
		}
		defer db.Release()
		bufs = []*memory.Buffer{vb, nc.offs, db}
	}
	data := array.NewData(nc.field.Type, nc.total, bufs, nil, nulls, 0)
	arr := array.MakeFromData(data)
	data.Release()
	return series.New(nc.field.Name, arr)
}

// joinBytes concatenates the task-local string data and rebases the
// offsets. It returns nil when the data does not fit 32-bit offsets.
func (nc *nativeCol) joinBytes(mem memory.Allocator) (*memory.Buffer, error) {
	total := 0
	base := make([]int, len(nc.tasks))
	for i, t := range nc.tasks {
		base[i] = total
		total += len(t.data)
	}
	if !nc.large && total > math.MaxInt32 {
		return nil, nil
	}
	db := memory.NewResizableBuffer(mem)
	db.Resize(total)
	dst := db.Bytes()
	parallelTasks(len(nc.tasks), func(i int) {
		t := nc.tasks[i]
		copy(dst[base[i]:], t.data)
		if nc.large {
			offs := arrow.Int64Traits.CastFromBytes(nc.offs.Bytes())[t.rowOff : t.rowOff+t.rows]
			b := int64(base[i])
			for j := range offs {
				offs[j] += b
			}
		} else {
			offs := arrow.Int32Traits.CastFromBytes(nc.offs.Bytes())[t.rowOff : t.rowOff+t.rows]
			b := int32(base[i])
			for j := range offs {
				offs[j] += b
			}
		}
	})
	if nc.large {
		arrow.Int64Traits.CastFromBytes(nc.offs.Bytes())[nc.total] = int64(total)
	} else {
		arrow.Int32Traits.CastFromBytes(nc.offs.Bytes())[nc.total] = int32(total)
	}
	return db, nil
}

// bytesChunks emits one array per row group for string columns whose
// data exceeds 32-bit offsets.
func (nc *nativeCol) bytesChunks(mem memory.Allocator, vb *memory.Buffer) (*series.Series, error) {
	var arrs []arrow.Array
	all := arrow.Int32Traits.CastFromBytes(nc.offs.Bytes())
	for _, t := range nc.tasks {
		ob := memory.NewResizableBuffer(mem)
		ob.Resize((t.rows + 1) * 4)
		o := arrow.Int32Traits.CastFromBytes(ob.Bytes())
		copy(o, all[t.rowOff:t.rowOff+t.rows])
		o[t.rows] = int32(len(t.data))
		db := memory.NewBufferBytes(t.data)
		var tv *memory.Buffer
		if t.valid != nil {
			tv = memory.NewBufferBytes(t.valid)
		}
		data := array.NewData(nc.field.Type, t.rows, []*memory.Buffer{tv, ob, db}, nil, t.nulls, 0)
		arrs = append(arrs, array.MakeFromData(data))
		data.Release()
		ob.Release()
	}
	return series.New(nc.field.Name, arrs...)
}

func parallelTasks(n int, f func(i int)) {
	workers := min(n, runtime.GOMAXPROCS(0))
	if workers <= 1 {
		for i := range n {
			f(i)
		}
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				f(i)
			}
		})
	}
	wg.Wait()
}

// chunkDecoder holds one worker's scratch buffers.
type chunkDecoder struct {
	r  io.ReaderAt
	md *metadata.FileMetaData

	raw   []byte // compressed column chunk
	page  []byte // decompressed page
	dictB []byte // decompressed dictionary page
	dense []byte // non-null values before scattering
	pv    []byte // page validity bits
	idx   []uint32

	// Current chunk.
	t        *colTask
	codec    compress.Codec
	row      int
	dict     []byte  // fixed-width dictionary values
	dictN    int     // dictionary entries
	dictOffs []int32 // byte dictionary offsets (dictN+1)
	dictData []byte
}

// sliceReader is an in-memory (often memory mapped) input. The native
// reader decodes pages straight out of b; everything else (footer,
// pqarrow columns) reads through the fault-safe mmapfile.Reader.
type sliceReader struct {
	*mmapfile.Reader
	b []byte
}

func newSliceReader(b []byte) *sliceReader { return &sliceReader{Reader: mmapfile.NewReader(b), b: b} }

// decoderPool keeps worker scratch buffers warm across reads. Fresh
// buffers cost a page fault per 16 KiB on first touch, which showed up
// as a large share of the read time.
var decoderPool = sync.Pool{New: func() any { return new(chunkDecoder) }}

func grow(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n, n+n/8)
	}
	return b[:n]
}

// safeDecode decodes t and turns a memory fault (a mapped file that
// shrank underneath the reader) into an error.
func (d *chunkDecoder) safeDecode(t *colTask) (err error) {
	if _, ok := d.r.(*sliceReader); ok {
		defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
		defer mmapfile.Recover(&err)
	}
	return d.decode(t)
}

func (d *chunkDecoder) decode(t *colTask) error {
	nc := t.col
	rgm := d.md.RowGroup(t.rg)
	cc, err := rgm.ColumnChunk(nc.leaf)
	if err != nil {
		return err
	}
	start := cc.DataPageOffset()
	if cc.HasDictionaryPage() && cc.DictionaryPageOffset() > 0 && cc.DictionaryPageOffset() < start {
		start = cc.DictionaryPageOffset()
	}
	n := cc.TotalCompressedSize()
	if start < 0 || n < 0 || n > 1<<40 {
		return fmt.Errorf("parquet: invalid column chunk range %d+%d", start, n)
	}
	var raw []byte
	if sr, ok := d.r.(*sliceReader); ok {
		if start+n > int64(len(sr.b)) {
			return fmt.Errorf("parquet: column chunk %d+%d past the end of the file", start, n)
		}
		raw = sr.b[start : start+n]
	} else {
		d.raw = grow(d.raw, int(n))
		if got, err := d.r.ReadAt(d.raw, start); got != int(n) {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return fmt.Errorf("parquet: read column chunk: %w", err)
		}
		raw = d.raw
	}
	d.t = t
	d.row = 0
	d.dict, d.dictN, d.dictOffs, d.dictData = nil, 0, nil, nil
	d.codec = nil
	if c := cc.Compression(); c != compress.Codecs.Uncompressed {
		if d.codec, err = compress.GetCodec(c); err != nil {
			return errUnsupported
		}
	}
	if nc.kind == nkBool {
		t.bits = make([]byte, bitutil.BytesForBits(int64(t.rows)))
	}
	if nc.kind == nkBytes {
		// PLAIN pages hold 4 length bytes per value; the rest is data.
		t.data = make([]byte, 0, max(int(cc.TotalUncompressedSize())-4*t.rows, 0))
	}
	buf := raw
	for len(buf) > 0 && d.row < t.rows {
		h, hl, err := readPageHeader(buf)
		if err != nil {
			return err
		}
		buf = buf[hl:]
		if int(h.compressed) > len(buf) {
			return fmt.Errorf("parquet: page of %d bytes overruns column chunk", h.compressed)
		}
		body := buf[:h.compressed]
		buf = buf[h.compressed:]
		switch h.typ {
		case pageDictionary:
			err = d.dictPage(h, body)
		case pageData:
			err = d.dataPageV1(h, body)
		case pageDataV2:
			err = d.dataPageV2(h, body)
		}
		if err != nil {
			return err
		}
	}
	if d.row != t.rows {
		return fmt.Errorf("parquet: column %q row group %d: decoded %d of %d rows", nc.field.Name, t.rg, d.row, t.rows)
	}
	d.t = nil
	return nil
}

func (d *chunkDecoder) decompress(dst []byte, src []byte, n int) ([]byte, error) {
	if n == 0 {
		return dst[:0], nil
	}
	if d.codec == nil {
		if len(src) != n {
			return nil, fmt.Errorf("parquet: page size %d, expected %d", len(src), n)
		}
		return src, nil
	}
	dst = grow(dst, n)
	out, err := compress.Decode(d.codec, dst, src)
	if err != nil {
		return nil, fmt.Errorf("parquet: decompress page: %w", err)
	}
	if len(out) != n {
		return nil, fmt.Errorf("parquet: decompressed page is %d bytes, expected %d", len(out), n)
	}
	return out, nil
}

func (d *chunkDecoder) dictPage(h pageHeader, body []byte) error {
	if h.encoding != encPlain && h.encoding != encPlainDict {
		return errUnsupported
	}
	b, err := d.decompress(d.dictB, body, int(h.uncompressed))
	if err != nil {
		return err
	}
	if d.codec != nil {
		d.dictB = b
	}
	n := int(h.numValues)
	nc := d.t.col
	switch nc.kind {
	case nkFixed, nkNarrow:
		if len(b) < n*nc.pw {
			return errors.New("parquet: short dictionary page")
		}
		d.dict, d.dictN = b[:n*nc.pw], n
	case nkBool:
		return errUnsupported
	case nkBytes:
		offs := make([]int32, n+1)
		data := make([]byte, 0, max(len(b)-4*n, 0))
		pos := 0
		for i := range n {
			if pos+4 > len(b) {
				return errors.New("parquet: short dictionary page")
			}
			l := int(uint32(b[pos]) | uint32(b[pos+1])<<8 | uint32(b[pos+2])<<16 | uint32(b[pos+3])<<24)
			pos += 4
			if l < 0 || pos+l > len(b) {
				return errors.New("parquet: short dictionary page")
			}
			offs[i] = int32(len(data))
			data = append(data, b[pos:pos+l]...)
			pos += l
		}
		offs[n] = int32(len(data))
		d.dictOffs, d.dictData, d.dictN = offs, data, n
	}
	return nil
}

func (d *chunkDecoder) dataPageV1(h pageHeader, body []byte) error {
	b, err := d.decompress(d.page, body, int(h.uncompressed))
	if err != nil {
		return err
	}
	if d.codec != nil {
		d.page = b
	}
	n := int(h.numValues)
	var levels []byte
	if d.t.col.maxDef > 0 {
		if h.defEncoding != encRLE {
			return errUnsupported
		}
		if len(b) < 4 {
			return errors.New("parquet: short data page")
		}
		l := int(uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24)
		if l < 0 || 4+l > len(b) {
			return errors.New("parquet: short data page")
		}
		levels = b[4 : 4+l]
		b = b[4+l:]
	}
	return d.values(h.encoding, n, levels, b)
}

func (d *chunkDecoder) dataPageV2(h pageHeader, body []byte) error {
	if h.repLen != 0 {
		return errUnsupported
	}
	ll := int(h.defLen)
	if ll > len(body) || ll > int(h.uncompressed) {
		return errors.New("parquet: short data page")
	}
	levels := body[:ll]
	vals := body[ll:]
	if h.v2Compressed && d.codec != nil {
		var err error
		vals, err = d.decompress(d.page, vals, int(h.uncompressed)-ll)
		if err != nil {
			return err
		}
		d.page = vals
	}
	if d.t.col.maxDef == 0 {
		levels = nil
	}
	return d.values(h.encoding, int(h.numValues), levels, vals)
}

// values decodes one page of n rows. levels holds the RLE definition
// levels (nil for required columns) and vals the encoded values.
func (d *chunkDecoder) values(enc int32, n int, levels, vals []byte) error {
	t := d.t
	nc := t.col
	if n == 0 {
		return nil
	}
	if d.row+n > t.rows {
		return fmt.Errorf("parquet: column %q row group %d: page overruns the row count", nc.field.Name, t.rg)
	}
	nn := n
	if levels != nil {
		d.pv = grow(d.pv, int(bitutil.BytesForBits(int64(n))))
		rd := newRLEReader(levels, 1)
		ones, err := rd.readBits(d.pv, 0, n)
		if err != nil {
			return err
		}
		nn = ones
		if nn < n {
			if t.valid == nil {
				t.valid = make([]byte, bitutil.BytesForBits(int64(t.rows)))
				bitutil.SetBitsTo(t.valid, 0, int64(d.row), true)
			}
			bitutil.CopyBitmap(d.pv, 0, n, t.valid, d.row)
			t.nulls += n - nn
		} else if t.valid != nil {
			bitutil.SetBitsTo(t.valid, int64(d.row), int64(n), true)
		}
	} else if t.valid != nil {
		bitutil.SetBitsTo(t.valid, int64(d.row), int64(n), true)
	}
	if nn == 0 {
		d.allNull(n)
		d.row += n
		return nil
	}
	var err error
	switch nc.kind {
	case nkFixed, nkNarrow:
		err = d.fixedValues(enc, n, nn, vals)
	case nkBool:
		err = d.boolValues(enc, n, nn, vals)
	case nkBytes:
		err = d.bytesValues(enc, n, nn, vals)
	}
	if err != nil {
		return err
	}
	d.row += n
	return nil
}

// allNull fills a page of n null rows.
func (d *chunkDecoder) allNull(n int) {
	t := d.t
	nc := t.col
	base := t.rowOff + d.row
	switch nc.kind {
	case nkFixed, nkNarrow:
		clear(nc.vals.Bytes()[base*nc.ow : (base+n)*nc.ow])
	case nkBytes:
		if nc.large {
			o := arrow.Int64Traits.CastFromBytes(nc.offs.Bytes())[base : base+n]
			for i := range o {
				o[i] = int64(len(t.data))
			}
		} else {
			o := arrow.Int32Traits.CastFromBytes(nc.offs.Bytes())[base : base+n]
			for i := range o {
				o[i] = int32(len(t.data))
			}
		}
	}
}

func (d *chunkDecoder) fixedValues(enc int32, n, nn int, vals []byte) error {
	nc := d.t.col
	pw := nc.pw
	out := nc.vals.Bytes()[(d.t.rowOff+d.row)*nc.ow : (d.t.rowOff+d.row+n)*nc.ow]
	direct := nn == n && nc.kind == nkFixed
	dense := out
	if !direct {
		d.dense = grow(d.dense, nn*pw)
		dense = d.dense
	}
	switch enc {
	case encPlain:
		if len(vals) < nn*pw {
			return errors.New("parquet: short data page")
		}
		if direct {
			copy(out, vals[:nn*pw])
		} else {
			dense = vals[:nn*pw]
		}
	case encPlainDict, encRLEDict:
		if d.dict == nil {
			return errors.New("parquet: dictionary page missing")
		}
		if err := d.gatherFixed(vals, nn, dense); err != nil {
			return err
		}
	default:
		return errUnsupported
	}
	if direct {
		return nil
	}
	scatterFixed(dense, out, d.pv, n, nn == n, pw, nc.ow)
	return nil
}

// dictIndices decodes nn dictionary indices from an RLE_DICTIONARY
// value section into d.idx and validates them.
func (d *chunkDecoder) dictIndices(vals []byte, nn int) ([]uint32, error) {
	if len(vals) < 1 {
		return nil, errors.New("parquet: short data page")
	}
	bw := int(vals[0])
	if bw > 32 {
		return nil, errRLE
	}
	if cap(d.idx) < nn {
		d.idx = make([]uint32, nn, nn+nn/8)
	}
	idx := d.idx[:nn]
	rd := newRLEReader(vals[1:], bw)
	if err := rd.readU32(idx); err != nil {
		return nil, err
	}
	lim := uint32(d.dictN)
	var bad uint32
	for _, v := range idx {
		if v >= lim {
			bad = 1
		}
	}
	if bad != 0 {
		return nil, errors.New("parquet: dictionary index out of range")
	}
	return idx, nil
}

func (d *chunkDecoder) gatherFixed(vals []byte, nn int, dense []byte) error {
	idx, err := d.dictIndices(vals, nn)
	if err != nil {
		return err
	}
	if d.t.col.pw == 8 {
		dict := arrow.Uint64Traits.CastFromBytes(d.dict)
		out := arrow.Uint64Traits.CastFromBytes(dense)[:nn]
		for i, v := range idx {
			out[i] = dict[v]
		}
	} else {
		dict := arrow.Uint32Traits.CastFromBytes(d.dict)
		out := arrow.Uint32Traits.CastFromBytes(dense)[:nn]
		for i, v := range idx {
			out[i] = dict[v]
		}
	}
	return nil
}

// scatterFixed spreads nn dense values over n rows following the
// validity bits, converting from width pw to ow. Null rows are zeroed.
func scatterFixed(dense, out, valid []byte, n int, allValid bool, pw, ow int) {
	switch {
	case pw == ow && pw == 8:
		src := arrow.Uint64Traits.CastFromBytes(dense)
		dst := arrow.Uint64Traits.CastFromBytes(out)[:n]
		k := 0
		for i := range dst {
			if bitutil.BitIsSet(valid, i) {
				dst[i] = src[k]
				k++
			} else {
				dst[i] = 0
			}
		}
	case pw == ow && pw == 4:
		src := arrow.Uint32Traits.CastFromBytes(dense)
		dst := arrow.Uint32Traits.CastFromBytes(out)[:n]
		k := 0
		for i := range dst {
			if bitutil.BitIsSet(valid, i) {
				dst[i] = src[k]
				k++
			} else {
				dst[i] = 0
			}
		}
	default:
		src := arrow.Uint32Traits.CastFromBytes(dense)
		k := 0
		for i := range n {
			var v uint32
			if allValid || bitutil.BitIsSet(valid, i) {
				v = src[k]
				k++
			}
			if ow == 1 {
				out[i] = byte(v)
			} else {
				out[2*i] = byte(v)
				out[2*i+1] = byte(v >> 8)
			}
		}
	}
}

func (d *chunkDecoder) boolValues(enc int32, n, nn int, vals []byte) error {
	t := d.t
	var src []byte
	switch enc {
	case encPlain:
		if len(vals) < int(bitutil.BytesForBits(int64(nn))) {
			return errors.New("parquet: short data page")
		}
		src = vals
	case encRLE:
		if len(vals) < 4 {
			return errors.New("parquet: short data page")
		}
		l := int(uint32(vals[0]) | uint32(vals[1])<<8 | uint32(vals[2])<<16 | uint32(vals[3])<<24)
		if l < 0 || 4+l > len(vals) {
			return errors.New("parquet: short data page")
		}
		d.dense = grow(d.dense, int(bitutil.BytesForBits(int64(nn))))
		rd := newRLEReader(vals[4:4+l], 1)
		if _, err := rd.readBits(d.dense, 0, nn); err != nil {
			return err
		}
		src = d.dense
	default:
		return errUnsupported
	}
	if nn == n {
		bitutil.CopyBitmap(src, 0, n, t.bits, d.row)
		return nil
	}
	k := 0
	for i := range n {
		if bitutil.BitIsSet(d.pv, i) {
			if bitutil.BitIsSet(src, k) {
				bitutil.SetBit(t.bits, d.row+i)
			}
			k++
		}
	}
	return nil
}

func (d *chunkDecoder) bytesValues(enc int32, n, nn int, vals []byte) error {
	t := d.t
	nc := t.col
	base := t.rowOff + d.row
	var o32 []int32
	var o64 []int64
	if nc.large {
		o64 = arrow.Int64Traits.CastFromBytes(nc.offs.Bytes())[base : base+n]
	} else {
		o32 = arrow.Int32Traits.CastFromBytes(nc.offs.Bytes())[base : base+n]
	}
	setOff := func(i, v int) {
		if o64 != nil {
			o64[i] = int64(v)
		} else {
			o32[i] = int32(v)
		}
	}
	allValid := nn == n
	data := t.data
	switch enc {
	case encPlain:
		need := len(data) + max(len(vals)-4*nn, 0)
		if cap(data) < need {
			data = slices.Grow(data, need-len(data))
		}
		pos := 0
		for i := range n {
			if !allValid && !bitutil.BitIsSet(d.pv, i) {
				setOff(i, len(data))
				continue
			}
			if pos+4 > len(vals) {
				return errors.New("parquet: short data page")
			}
			l := int(uint32(vals[pos]) | uint32(vals[pos+1])<<8 | uint32(vals[pos+2])<<16 | uint32(vals[pos+3])<<24)
			pos += 4
			if l < 0 || pos+l > len(vals) {
				return errors.New("parquet: short data page")
			}
			setOff(i, len(data))
			data = append(data, vals[pos:pos+l]...)
			pos += l
		}
	case encPlainDict, encRLEDict:
		if d.dictOffs == nil {
			return errors.New("parquet: dictionary page missing")
		}
		idx, err := d.dictIndices(vals, nn)
		if err != nil {
			return err
		}
		doffs, ddata := d.dictOffs, d.dictData
		need := 0
		for _, v := range idx {
			need += int(doffs[v+1] - doffs[v])
		}
		if cap(data)-len(data) < need {
			data = slices.Grow(data, need)
		}
		k := 0
		for i := range n {
			if !allValid && !bitutil.BitIsSet(d.pv, i) {
				setOff(i, len(data))
				continue
			}
			v := idx[k]
			k++
			setOff(i, len(data))
			data = append(data, ddata[doffs[v]:doffs[v+1]]...)
		}
	default:
		return errUnsupported
	}
	if !nc.large && len(data) > math.MaxInt32 {
		return errUnsupported
	}
	t.data = data
	return nil
}
