package parquet

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"runtime"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	pqschema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/klauspost/compress/s2"

	"github.com/Gaurav-Gosain/golars/internal/strhash"
)

// The native writer encodes flat columns straight from the arrow
// buffers: PLAIN or dictionary pages, RLE definition levels, one column
// chunk per task, every column of every row group in parallel. The file
// metadata goes through arrow-go's metadata builders, so the footer is
// the same as pqarrow's. Tables with a column it does not handle go
// through pqarrow.

var errNotNative = errors.New("parquet: not writable natively")

type wkind uint8

const (
	wFixed wkind = iota // values copied at their arrow width (4 or 8)
	wWiden              // 1 or 2 byte integers widened to INT32
	wBool
	wBytes
)

type skind uint8

const (
	sInt32 skind = iota
	sUint32
	sInt64
	sUint64
	sFloat32
	sFloat64
	sBytes
	sBool
)

type wcol struct {
	name     string
	data     *arrow.Chunked
	desc     *pqschema.Column
	kind     wkind
	stat     skind
	width    int // arrow value width for fixed and widened columns
	signed   bool
	nullable bool
	large    bool
	dict     bool // dictionary encoding allowed
}

// nativeColumns maps every field of sc to a native column, or returns
// errNotNative.
func nativeColumns(tbl arrow.Table, pqs *pqschema.Schema) ([]wcol, error) {
	sc := tbl.Schema()
	if pqs.NumColumns() != sc.NumFields() {
		return nil, errNotNative
	}
	cols := make([]wcol, sc.NumFields())
	for i, f := range sc.Fields() {
		desc := pqs.Column(i)
		c := wcol{name: f.Name, data: tbl.Column(i).Data(), desc: desc, nullable: f.Nullable, dict: true}
		if desc.MaxRepetitionLevel() != 0 || int(desc.MaxDefinitionLevel()) != boolInt(f.Nullable) {
			return nil, errNotNative
		}
		if !f.Nullable && c.data.NullN() > 0 {
			return nil, errNotNative
		}
		phys := desc.PhysicalType()
		lt := desc.LogicalType()
		ok := false
		switch t := f.Type.(type) {
		case *arrow.Int8Type, *arrow.Int16Type, *arrow.Uint8Type, *arrow.Uint16Type:
			c.kind, c.stat, c.width = wWiden, sInt32, t.(arrow.FixedWidthDataType).BitWidth()/8
			c.signed = t.ID() == arrow.INT8 || t.ID() == arrow.INT16
			ok = phys == parquet.Types.Int32
		case *arrow.Int32Type, *arrow.Date32Type:
			c.kind, c.stat, c.width = wFixed, sInt32, 4
			ok = phys == parquet.Types.Int32
		case *arrow.Uint32Type:
			c.kind, c.stat, c.width = wFixed, sUint32, 4
			ok = phys == parquet.Types.Int32
		case *arrow.Time32Type:
			c.kind, c.stat, c.width = wFixed, sInt32, 4
			ok = phys == parquet.Types.Int32 && unitMatches(lt, t.Unit)
		case *arrow.Int64Type:
			c.kind, c.stat, c.width = wFixed, sInt64, 8
			ok = phys == parquet.Types.Int64
		case *arrow.Uint64Type:
			c.kind, c.stat, c.width = wFixed, sUint64, 8
			ok = phys == parquet.Types.Int64
		case *arrow.TimestampType:
			c.kind, c.stat, c.width = wFixed, sInt64, 8
			ok = phys == parquet.Types.Int64 && unitMatches(lt, t.Unit)
		case *arrow.Time64Type:
			c.kind, c.stat, c.width = wFixed, sInt64, 8
			ok = phys == parquet.Types.Int64 && unitMatches(lt, t.Unit)
		case *arrow.Float32Type:
			c.kind, c.stat, c.width = wFixed, sFloat32, 4
			ok = phys == parquet.Types.Float
		case *arrow.Float64Type:
			c.kind, c.stat, c.width = wFixed, sFloat64, 8
			ok = phys == parquet.Types.Double
		case *arrow.BooleanType:
			c.kind, c.stat, c.dict = wBool, sBool, false
			ok = phys == parquet.Types.Boolean
		case *arrow.StringType, *arrow.BinaryType:
			c.kind, c.stat = wBytes, sBytes
			ok = phys == parquet.Types.ByteArray
		case *arrow.LargeStringType, *arrow.LargeBinaryType:
			c.kind, c.stat, c.large = wBytes, sBytes, true
			ok = phys == parquet.Types.ByteArray
		}
		if !ok {
			return nil, errNotNative
		}
		cols[i] = c
	}
	return cols, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func unitMatches(lt pqschema.LogicalType, u arrow.TimeUnit) bool {
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

// encTask encodes rows [off, off+rows) of one column.
type encTask struct {
	col  *wcol
	off  int64
	rows int64

	buf          []byte // encoded pages
	dictLen      int    // bytes of the dictionary page at the start of buf
	uncompressed int64
	dataPages    int32
	dataEnc      int32
	stats        metadata.EncodedStatistics
	err          error
	done         chan struct{}
}

// writeNative writes tbl with the native encoder. It returns
// errNotNative before writing anything when a column needs pqarrow.
func writeNative(ctx context.Context, tbl arrow.Table, w io.Writer, cfg config, props *parquet.WriterProperties, arrProps pqarrow.ArrowWriterProperties) error {
	sc := tbl.Schema()
	pqs, err := pqarrow.ToParquet(sc, props, arrProps)
	if err != nil {
		return errNotNative
	}
	cols, err := nativeColumns(tbl, pqs)
	if err != nil {
		return err
	}
	var enc func(dst, src []byte) []byte
	switch cfg.compression {
	case compress.Codecs.Uncompressed:
	case compress.Codecs.Snappy:
		// s2's snappy-compatible fast mode. The codec arrow-go registers
		// uses the much slower "better" mode.
		enc = s2.EncodeSnappy
	default:
		codec, err := compress.GetCodec(cfg.compression)
		if err != nil {
			return errNotNative
		}
		enc = codec.Encode
	}

	fmb := metadata.NewFileMetadataBuilder(pqs, props, arrowSchemaMeta(sc, cfg.alloc))

	chunk := min(cfg.chunkSize, props.MaxRowGroupLength())
	nrows := tbl.NumRows()
	var tasks []*encTask
	for off := int64(0); off < nrows || (off == 0 && nrows == 0); off += chunk {
		size := min(chunk, nrows-off)
		for c := range cols {
			tasks = append(tasks, &encTask{col: &cols[c], off: off, rows: size, done: make(chan struct{})})
		}
		if nrows == 0 {
			break
		}
	}

	// Workers encode tasks in file order; the caller writes them in the
	// same order as they finish. inflight bounds the memory held by
	// encoded chunks that are waiting to be written.
	workers := min(len(tasks), runtime.GOMAXPROCS(0))
	inflight := make(chan struct{}, 2*workers+len(cols))
	queue := make(chan *encTask)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			e := encoderPool.Get().(*chunkEncoder)
			defer encoderPool.Put(e)
			e.page = e.page[:0]
			for t := range queue {
				if ctx.Err() == nil {
					t.err = e.encode(t, enc, props.DataPageSize(), props.DictionaryPageSizeLimit())
				} else {
					t.err = ctx.Err()
				}
				close(t.done)
			}
		})
	}
	stop := make(chan struct{})
	go func() {
		defer close(queue)
		for _, t := range tasks {
			select {
			case inflight <- struct{}{}:
			case <-stop:
				return
			}
			select {
			case queue <- t:
			case <-stop:
				return
			}
		}
	}()

	err = writeTasks(w, tasks, cols, fmb, inflight)
	close(stop)
	wg.Wait()
	return err
}

var magic = []byte("PAR1")

// arrowSchemaMeta returns the file key-value metadata: the arrow schema
// metadata plus the serialized arrow schema. Storing the arrow schema
// lets readers restore types parquet cannot express (large strings,
// time zones, dictionary columns).
func arrowSchemaMeta(sc *arrow.Schema, mem memory.Allocator) metadata.KeyValueMetadata {
	meta := make(metadata.KeyValueMetadata, 0, sc.Metadata().Len()+1)
	for i, k := range sc.Metadata().Keys() {
		_ = meta.Append(k, sc.Metadata().Values()[i])
	}
	_ = meta.Append("ARROW:schema", base64.StdEncoding.EncodeToString(flight.SerializeSchema(sc, mem)))
	return meta
}

func writeTasks(w io.Writer, tasks []*encTask, cols []wcol, fmb *metadata.FileMetaDataBuilder, inflight chan struct{}) error {
	if _, err := w.Write(magic); err != nil {
		return err
	}
	pos := int64(len(magic))
	var rgb *metadata.RowGroupMetaDataBuilder
	ordinal := int16(0)
	for i, t := range tasks {
		<-t.done
		if t.err != nil {
			return t.err
		}
		c := i % len(cols)
		if c == 0 {
			rgb = fmb.AppendRowGroup()
			rgb.SetNumRows(int(t.rows))
		}
		cb := rgb.NextColumnChunk()
		info := metadata.ChunkMetaInfo{
			NumValues:        t.rows,
			IndexPageOffset:  -1,
			DataPageOffset:   pos + int64(t.dictLen),
			CompressedSize:   int64(len(t.buf)),
			UncompressedSize: t.uncompressed,
		}
		es := metadata.EncodingStats{DataEncodingStats: map[parquet.Encoding]int32{}}
		if t.dictLen > 0 {
			info.DictPageOffset = pos
			es.DictEncodingStats = map[parquet.Encoding]int32{parquet.Encodings.Plain: 1}
		}
		if t.dataPages > 0 {
			es.DataEncodingStats[parquet.Encoding(t.dataEnc)] = t.dataPages
		}
		if t.rows > 0 {
			cb.SetStats(t.stats)
		}
		if err := cb.Finish(info, t.dictLen > 0, false, es); err != nil {
			return err
		}
		if _, err := w.Write(t.buf); err != nil {
			return err
		}
		pos += int64(len(t.buf))
		t.buf = nil
		<-inflight
		if c == len(cols)-1 {
			if err := rgb.Finish(0, ordinal); err != nil {
				return err
			}
			ordinal++
		}
	}
	md, err := fmb.Finish()
	if err != nil {
		return err
	}
	var foot bytes.Buffer
	n, err := md.WriteTo(&foot, nil)
	if err != nil {
		return err
	}
	foot.Write(binary.LittleEndian.AppendUint32(nil, uint32(n)))
	foot.Write(magic)
	_, err = w.Write(foot.Bytes())
	return err
}

// chunkEncoder holds one worker's scratch buffers.
type chunkEncoder struct {
	page  []byte   // uncompressed page
	comp  []byte   // compressed page
	idx   []uint32 // dictionary indices of the chunk
	valid []byte   // chunk validity, realigned to bit 0
	dict  dictBuilder
}

var encoderPool = sync.Pool{New: func() any { return new(chunkEncoder) }}

// piece is a slice of one arrow chunk inside a task's row range.
type piece struct {
	arr    arrow.Array
	off, n int // element offset into arr's buffers (includes arr.Offset)
}

func pieces(data *arrow.Chunked, off, rows int64) []piece {
	var out []piece
	pos := int64(0)
	end := off + rows
	for _, ch := range data.Chunks() {
		clen := int64(ch.Len())
		lo, hi := max(off, pos), min(end, pos+clen)
		if lo < hi {
			out = append(out, piece{arr: ch, off: ch.Data().Offset() + int(lo-pos), n: int(hi - lo)})
		}
		pos += clen
		if pos >= end {
			break
		}
	}
	return out
}

func (e *chunkEncoder) encode(t *encTask, enc func(dst, src []byte) []byte, pageSize, dictLimit int64) error {
	c := t.col
	ps := pieces(c.data, t.off, t.rows)
	t.buf = make([]byte, 0, estimateChunk(c, t.rows))
	nulls := 0
	for _, p := range ps {
		nulls += nullsIn(p)
	}
	t.stats.HasNullCount = true
	t.stats.NullCount = int64(nulls)
	t.stats.Signed = c.desc.SortOrder() == pqschema.SortSIGNED

	// Chunk-wide validity at bit 0 (only when there are nulls).
	var valid []byte
	if nulls > 0 {
		e.valid = grow(e.valid, int(bitutil.BytesForBits(t.rows)))
		valid = e.valid
		row := 0
		for _, p := range ps {
			if vb := p.arr.Data().Buffers()[0]; vb != nil && p.arr.NullN() > 0 {
				bitutil.CopyBitmap(vb.Bytes(), p.off, p.n, valid, row)
			} else {
				bitutil.SetBitsTo(valid, int64(row), int64(p.n), true)
			}
			row += p.n
		}
	}

	if c.dict && int(t.rows)-nulls > 0 && dictionaryWorthwhilePieces(c, ps) {
		ok, err := e.encodeDict(t, ps, valid, nulls, enc, pageSize, dictLimit)
		if err != nil || ok {
			return err
		}
		t.buf = t.buf[:0]
		t.uncompressed = 0
		t.dataPages = 0
		t.dictLen = 0
	}
	return e.encodePlain(t, ps, valid, nulls, enc, pageSize)
}

func nullsIn(p piece) int {
	if p.arr.NullN() == 0 {
		return 0
	}
	vb := p.arr.Data().Buffers()[0]
	if vb == nil {
		return p.n
	}
	return p.n - bitutil.CountSetBits(vb.Bytes(), p.off, p.n)
}

func estimateChunk(c *wcol, rows int64) int {
	switch c.kind {
	case wFixed:
		return int(rows)*c.width/2 + 1024
	case wWiden:
		return int(rows)*2 + 1024
	}
	return int(rows) + 1024
}

// appendLevels appends the v1 definition levels of rows [row, row+n)
// as one RLE run (no nulls) or one bit-packed run of the validity bits.
func appendLevels(b []byte, valid []byte, row, n int) []byte {
	start := len(b)
	b = append(b, 0, 0, 0, 0)
	if valid == nil {
		b = binary.AppendUvarint(b, uint64(n)<<1)
		b = append(b, 1)
	} else {
		groups := (n + 7) / 8
		b = binary.AppendUvarint(b, uint64(groups)<<1|1)
		at := len(b)
		b = append(b, make([]byte, groups)...)
		bitutil.CopyBitmap(valid, row, n, b[at:], 0)
	}
	binary.LittleEndian.PutUint32(b[start:], uint32(len(b)-start-4))
	return b
}

// flushPage compresses e.page and appends it to t.buf as a data page.
func (e *chunkEncoder) flushPage(t *encTask, enc func(dst, src []byte) []byte, n int, encoding int32) {
	body := e.page
	if enc != nil {
		e.comp = enc(e.comp[:cap(e.comp)], e.page)
		body = e.comp
	}
	hs := len(t.buf)
	t.buf = appendDataPageHeader(t.buf, len(e.page), len(body), n, encoding)
	t.uncompressed += int64(len(t.buf)-hs) + int64(len(e.page))
	t.buf = append(t.buf, body...)
	t.dataPages++
	t.dataEnc = encoding
	e.page = e.page[:0]
}

// rowsPerPage returns how many rows fit a data page.
func rowsPerPage(c *wcol, pageSize int64) int {
	w := 8
	switch c.kind {
	case wFixed:
		w = c.width
	case wWiden:
		w = 4
	case wBool:
		return int(max(pageSize*8, 1))
	}
	return int(max(pageSize/int64(w), 1))
}

func (e *chunkEncoder) encodePlain(t *encTask, ps []piece, valid []byte, nulls int, enc func(dst, src []byte) []byte, pageSize int64) error {
	c := t.col
	var st statAcc
	st.init(c.stat)
	row := 0
	perPage := rowsPerPage(c, pageSize)
	for _, p := range ps {
		for lo := 0; lo < p.n; {
			n := min(perPage, p.n-lo)
			if c.kind == wBytes {
				n = bytesPageRows(p, lo, n, pageSize)
			}
			pageValid := valid
			if nulls == 0 {
				pageValid = nil
			}
			if c.nullable {
				e.page = appendLevels(e.page, pageValid, row, n)
			}
			e.page = appendPlain(e.page, c, p, lo, n, pageValid, row, &st)
			e.flushPage(t, enc, n, encPlain)
			row += n
			lo += n
		}
	}
	if len(ps) == 0 {
		// An empty column chunk still needs one (empty) data page.
		if c.nullable {
			e.page = appendLevels(e.page, nil, 0, 0)
		}
		e.flushPage(t, enc, 0, encPlain)
	}
	st.finish(&t.stats)
	return nil
}

// bytesPageRows limits n so that the page data stays near pageSize.
func bytesPageRows(p piece, lo, n int, pageSize int64) int {
	var start, end func(i int) int64
	switch a := p.arr.(type) {
	case *array.String:
		o := a.ValueOffsets()
		base := p.off - a.Data().Offset()
		start = func(i int) int64 { return int64(o[base+i]) }
		end = start
	case *array.Binary:
		o := a.ValueOffsets()
		base := p.off - a.Data().Offset()
		start = func(i int) int64 { return int64(o[base+i]) }
		end = start
	case *array.LargeString:
		o := a.ValueOffsets()
		base := p.off - a.Data().Offset()
		start = func(i int) int64 { return o[base+i] }
		end = start
	case *array.LargeBinary:
		o := a.ValueOffsets()
		base := p.off - a.Data().Offset()
		start = func(i int) int64 { return o[base+i] }
		end = start
	default:
		return n
	}
	if end(lo+n)-start(lo)+4*int64(n) <= pageSize {
		return n
	}
	// Binary search the largest count within the budget.
	a, b := 1, n
	for a < b {
		m := (a + b + 1) / 2
		if end(lo+m)-start(lo)+4*int64(m) <= pageSize {
			a = m
		} else {
			b = m - 1
		}
	}
	return a
}

// appendPlain appends the PLAIN encoding of the non-null values of
// rows [lo, lo+n) of p and folds them into the statistics. valid holds
// the chunk validity at bit row for p's row lo (nil when no nulls).
func appendPlain(b []byte, c *wcol, p piece, lo, n int, valid []byte, row int, st *statAcc) []byte {
	d := p.arr.Data()
	first := p.off + lo
	switch c.kind {
	case wFixed:
		w := c.width
		src := d.Buffers()[1].Bytes()[first*w : (first+n)*w]
		if valid == nil {
			st.fixed(src, w)
			return append(b, src...)
		}
		start := len(b)
		for i := range n {
			if bitutil.BitIsSet(valid, row+i) {
				b = append(b, src[i*w:(i+1)*w]...)
			}
		}
		st.fixed(b[start:], w)
		return b
	case wWiden:
		w := c.width
		src := d.Buffers()[1].Bytes()[first*w : (first+n)*w]
		start := len(b)
		b = growBytes(b, 4*n)
		k := start
		for i := range n {
			if valid != nil && !bitutil.BitIsSet(valid, row+i) {
				continue
			}
			var v int32
			switch {
			case w == 1 && c.signed:
				v = int32(int8(src[i]))
			case w == 1:
				v = int32(src[i])
			case c.signed:
				v = int32(int16(binary.LittleEndian.Uint16(src[2*i:])))
			default:
				v = int32(binary.LittleEndian.Uint16(src[2*i:]))
			}
			binary.LittleEndian.PutUint32(b[k:], uint32(v))
			k += 4
		}
		b = b[:k]
		st.fixed(b[start:], 4)
		return b
	case wBool:
		bits := d.Buffers()[1].Bytes()
		start := len(b)
		if valid == nil {
			b = growBytes(b, int(bitutil.BytesForBits(int64(n))))
			clear(b[start:])
			bitutil.CopyBitmap(bits, first, n, b[start:], 0)
			st.bools(bits, first, n)
			return b
		}
		k := 0
		b = growBytes(b, int(bitutil.BytesForBits(int64(n))))
		clear(b[start:])
		out := b[start:]
		for i := range n {
			if bitutil.BitIsSet(valid, row+i) {
				v := bitutil.BitIsSet(bits, first+i)
				if v {
					bitutil.SetBit(out, k)
				}
				st.bool(v)
				k++
			}
		}
		return b[:start+int(bitutil.BytesForBits(int64(k)))]
	case wBytes:
		data := d.Buffers()[2].Bytes()
		if c.large {
			offs := arrow.Int64Traits.CastFromBytes(d.Buffers()[1].Bytes())[first : first+n+1]
			b = growBytes(b, int(offs[n]-offs[0])+4*n)
			return appendByteValues(b[:len(b)-int(offs[n]-offs[0])-4*n], data, offs, valid, row, st)
		}
		offs := arrow.Int32Traits.CastFromBytes(d.Buffers()[1].Bytes())[first : first+n+1]
		b = growBytes(b, int(offs[n]-offs[0])+4*n)
		return appendByteValues(b[:len(b)-int(offs[n]-offs[0])-4*n], data, offs, valid, row, st)
	}
	return b
}

func appendByteValues[O int32 | int64](b, data []byte, offs []O, valid []byte, row int, st *statAcc) []byte {
	n := len(offs) - 1
	for i := range n {
		if valid != nil && !bitutil.BitIsSet(valid, row+i) {
			continue
		}
		v := data[offs[i]:offs[i+1]]
		b = binary.LittleEndian.AppendUint32(b, uint32(len(v)))
		b = append(b, v...)
		st.bytes(v)
	}
	return b
}

// growBytes extends b by n bytes.
func growBytes(b []byte, n int) []byte {
	if cap(b)-len(b) < n {
		nb := make([]byte, len(b), max(2*cap(b), len(b)+n))
		copy(nb, b)
		b = nb
	}
	return b[:len(b)+n]
}

// dictionaryWorthwhilePieces samples up to dictSampleSize values and
// reports whether at most a quarter of them are distinct.
func dictionaryWorthwhilePieces(c *wcol, ps []piece) bool {
	total := 0
	for _, p := range ps {
		total += p.n
	}
	step := max(total/dictSampleSize, 1)
	var d dictBuilder
	d.reset(c, 256)
	sampled := 0
	base := 0
	next := 0
	for _, p := range ps {
		for next < base+p.n {
			i := next - base
			next += step
			if p.arr.IsNull(p.off - p.arr.Data().Offset() + i) {
				continue
			}
			d.insertAt(c, p, i)
			sampled++
		}
		base += p.n
	}
	return sampled > 0 && d.n*4 <= sampled
}

// encodeDict writes a dictionary page and RLE_DICTIONARY data pages. It
// returns false (and leaves the output for the caller to reset) when
// the dictionary grows past dictLimit bytes.
func (e *chunkEncoder) encodeDict(t *encTask, ps []piece, valid []byte, nulls int, enc func(dst, src []byte) []byte, pageSize, dictLimit int64) (bool, error) {
	c := t.col
	nn := int(t.rows) - nulls
	if cap(e.idx) < nn {
		e.idx = make([]uint32, nn, nn+nn/8)
	}
	idx := e.idx[:nn]
	d := &e.dict
	d.reset(c, 1024)
	k := 0
	row := 0
	for _, p := range ps {
		for i := range p.n {
			if valid != nil && !bitutil.BitIsSet(valid, row+i) {
				continue
			}
			idx[k] = d.insertAt(c, p, i)
			k++
		}
		row += p.n
		if int64(d.size) > dictLimit || d.n > nn/2+1 {
			return false, nil
		}
	}

	// Dictionary page.
	e.page = e.page[:0]
	var st statAcc
	st.init(c.stat)
	e.page = d.appendPlain(e.page, c, &st)
	st.finish(&t.stats)
	body := e.page
	if enc != nil {
		e.comp = enc(e.comp[:cap(e.comp)], e.page)
		body = e.comp
	}
	t.buf = appendDictPageHeader(t.buf, len(e.page), len(body), d.n)
	t.uncompressed += int64(len(t.buf)) + int64(len(e.page))
	t.buf = append(t.buf, body...)
	t.dictLen = len(t.buf)
	e.page = e.page[:0]

	bw := max(bitWidth(uint64(d.n-1)), 1)
	perPage := int(max(pageSize*8/int64(bw), 8))
	row, k = 0, 0
	for row < int(t.rows) || row == 0 {
		n := min(perPage, int(t.rows)-row)
		if nulls > 0 {
			e.page = appendLevels(e.page, valid, row, n)
		} else if c.nullable {
			e.page = appendLevels(e.page, nil, row, n)
		}
		cnt := n
		if nulls > 0 {
			cnt = bitutil.CountSetBits(valid, row, n)
		}
		e.page = append(e.page, byte(bw))
		e.page = appendBitPacked(e.page, idx[k:k+cnt], bw)
		e.flushPage(t, enc, n, encRLEDict)
		k += cnt
		row += n
		if n == 0 {
			break
		}
	}
	return true, nil
}

// appendBitPacked appends vals as one bit-packed run of width bw.
func appendBitPacked(b []byte, vals []uint32, bw int) []byte {
	if len(vals) == 0 {
		return b
	}
	groups := (len(vals) + 7) / 8
	b = binary.AppendUvarint(b, uint64(groups)<<1|1)
	start := len(b)
	b = growBytes(b, groups*bw)
	out := b[start:]
	clear(out)
	var acc uint64
	nbits := 0
	o := 0
	for _, v := range vals {
		acc |= uint64(v) << nbits
		nbits += bw
		for nbits >= 32 {
			binary.LittleEndian.PutUint32(out[o:], uint32(acc))
			o += 4
			acc >>= 32
			nbits -= 32
		}
	}
	for nbits > 0 {
		out[o] = byte(acc)
		o++
		acc >>= 8
		nbits -= 8
	}
	return b
}

// dictBuilder is an insertion-ordered hash set of values.
type dictBuilder struct {
	n     int
	size  int // PLAIN size of the dictionary in bytes
	slots []int32
	mask  uint64
	fixed []uint64 // fixed-width values (as bits)
	bytes []byte   // byte values
	offs  []int32
	hash  []uint64
}

func (d *dictBuilder) reset(c *wcol, hint int) {
	d.n, d.size = 0, 0
	sz := 1
	for sz < 2*hint {
		sz <<= 1
	}
	if cap(d.slots) >= sz {
		d.slots = d.slots[:sz]
	} else {
		d.slots = make([]int32, sz)
	}
	for i := range d.slots {
		d.slots[i] = -1
	}
	d.mask = uint64(sz - 1)
	d.fixed = d.fixed[:0]
	d.bytes = d.bytes[:0]
	d.offs = append(d.offs[:0], 0)
	d.hash = d.hash[:0]
}

func mixHash(v uint64) uint64 {
	v ^= v >> 33
	v *= 0xff51afd7ed558ccd
	v ^= v >> 33
	v *= 0xc4ceb9fe1a85ec53
	v ^= v >> 33
	return v
}

// insertAt inserts value i of piece p and returns its dictionary index.
func (d *dictBuilder) insertAt(c *wcol, p piece, i int) uint32 {
	data := p.arr.Data()
	j := p.off + i
	if c.kind == wBytes {
		var v []byte
		if c.large {
			o := arrow.Int64Traits.CastFromBytes(data.Buffers()[1].Bytes())
			v = data.Buffers()[2].Bytes()[o[j]:o[j+1]]
		} else {
			o := arrow.Int32Traits.CastFromBytes(data.Buffers()[1].Bytes())
			v = data.Buffers()[2].Bytes()[o[j]:o[j+1]]
		}
		return d.insertBytes(v)
	}
	var v uint64
	src := data.Buffers()[1].Bytes()
	switch c.width {
	case 1:
		if c.signed {
			v = uint64(uint32(int32(int8(src[j]))))
		} else {
			v = uint64(src[j])
		}
	case 2:
		if c.signed {
			v = uint64(uint32(int32(int16(binary.LittleEndian.Uint16(src[2*j:])))))
		} else {
			v = uint64(binary.LittleEndian.Uint16(src[2*j:]))
		}
	case 4:
		v = uint64(binary.LittleEndian.Uint32(src[4*j:]))
	case 8:
		v = binary.LittleEndian.Uint64(src[8*j:])
	}
	return d.insertFixed(v, c)
}

func (d *dictBuilder) insertFixed(v uint64, c *wcol) uint32 {
	h := mixHash(v)
	for s := h & d.mask; ; s = (s + 1) & d.mask {
		id := d.slots[s]
		if id < 0 {
			d.slots[s] = int32(d.n)
			d.fixed = append(d.fixed, v)
			d.hash = append(d.hash, h)
			d.n++
			if c.kind == wWiden || c.width == 4 {
				d.size += 4
			} else {
				d.size += 8
			}
			if 2*d.n > len(d.slots) {
				d.rehash()
			}
			return uint32(d.n - 1)
		}
		if d.fixed[id] == v {
			return uint32(id)
		}
	}
}

func (d *dictBuilder) insertBytes(v []byte) uint32 {
	h := strhash.String(unsafeString(v))
	for s := h & d.mask; ; s = (s + 1) & d.mask {
		id := d.slots[s]
		if id < 0 {
			d.slots[s] = int32(d.n)
			d.bytes = append(d.bytes, v...)
			d.offs = append(d.offs, int32(len(d.bytes)))
			d.hash = append(d.hash, h)
			d.n++
			d.size += 4 + len(v)
			if 2*d.n > len(d.slots) {
				d.rehash()
			}
			return uint32(d.n - 1)
		}
		if d.hash[id] == h && bytes.Equal(d.bytes[d.offs[id]:d.offs[id+1]], v) {
			return uint32(id)
		}
	}
}

func (d *dictBuilder) rehash() {
	sz := len(d.slots) * 2
	d.slots = make([]int32, sz)
	for i := range d.slots {
		d.slots[i] = -1
	}
	d.mask = uint64(sz - 1)
	for id, h := range d.hash {
		s := h & d.mask
		for d.slots[s] >= 0 {
			s = (s + 1) & d.mask
		}
		d.slots[s] = int32(id)
	}
}

// appendPlain appends the PLAIN encoding of the dictionary values.
func (d *dictBuilder) appendPlain(b []byte, c *wcol, st *statAcc) []byte {
	start := len(b)
	switch {
	case c.kind == wBytes:
		for i := range d.n {
			v := d.bytes[d.offs[i]:d.offs[i+1]]
			b = binary.LittleEndian.AppendUint32(b, uint32(len(v)))
			b = append(b, v...)
			st.bytes(v)
		}
		return b
	case c.kind == wWiden || c.width == 4:
		for _, v := range d.fixed {
			b = binary.LittleEndian.AppendUint32(b, uint32(v))
		}
		st.fixed(b[start:], 4)
	default:
		for _, v := range d.fixed {
			b = binary.LittleEndian.AppendUint64(b, v)
		}
		st.fixed(b[start:], 8)
	}
	return b
}

// statAcc accumulates min and max in parquet's sort order for one
// physical type.
type statAcc struct {
	kind         skind
	has          bool
	i64min       int64
	i64max       int64
	u64min       uint64
	u64max       uint64
	fmin, fmax   float64
	bmin, bmax   []byte
	boolT, boolF bool
}

func (s *statAcc) init(k skind) { *s = statAcc{kind: k} }

// fixed folds PLAIN encoded values of width w.
func (s *statAcc) fixed(b []byte, w int) {
	n := len(b) / w
	if n == 0 {
		return
	}
	switch s.kind {
	case sInt32:
		v := arrow.Int32Traits.CastFromBytes(b)[:n]
		lo, hi := v[0], v[0]
		for _, x := range v[1:] {
			lo = min(lo, x)
			hi = max(hi, x)
		}
		s.foldInt(int64(lo), int64(hi))
	case sInt64:
		v := arrow.Int64Traits.CastFromBytes(b)[:n]
		lo, hi := v[0], v[0]
		for _, x := range v[1:] {
			lo = min(lo, x)
			hi = max(hi, x)
		}
		s.foldInt(lo, hi)
	case sUint32:
		v := arrow.Uint32Traits.CastFromBytes(b)[:n]
		lo, hi := v[0], v[0]
		for _, x := range v[1:] {
			lo = min(lo, x)
			hi = max(hi, x)
		}
		s.foldUint(uint64(lo), uint64(hi))
	case sUint64:
		v := arrow.Uint64Traits.CastFromBytes(b)[:n]
		lo, hi := v[0], v[0]
		for _, x := range v[1:] {
			lo = min(lo, x)
			hi = max(hi, x)
		}
		s.foldUint(lo, hi)
	case sFloat32:
		for _, x := range arrow.Float32Traits.CastFromBytes(b)[:n] {
			s.foldFloat(float64(x))
		}
	case sFloat64:
		v := arrow.Float64Traits.CastFromBytes(b)[:n]
		lo, hi := math.Inf(1), math.Inf(-1)
		seen := false
		for _, x := range v {
			if x != x {
				continue
			}
			seen = true
			if x < lo {
				lo = x
			}
			if x > hi {
				hi = x
			}
		}
		if seen {
			s.foldFloat(lo)
			s.foldFloat(hi)
		}
	}
}

func (s *statAcc) foldInt(lo, hi int64) {
	if !s.has {
		s.i64min, s.i64max, s.has = lo, hi, true
		return
	}
	s.i64min = min(s.i64min, lo)
	s.i64max = max(s.i64max, hi)
}

func (s *statAcc) foldUint(lo, hi uint64) {
	if !s.has {
		s.u64min, s.u64max, s.has = lo, hi, true
		return
	}
	s.u64min = min(s.u64min, lo)
	s.u64max = max(s.u64max, hi)
}

func (s *statAcc) foldFloat(x float64) {
	if x != x {
		return
	}
	if !s.has {
		s.fmin, s.fmax, s.has = x, x, true
		return
	}
	if x < s.fmin {
		s.fmin = x
	}
	if x > s.fmax {
		s.fmax = x
	}
}

func (s *statAcc) bytes(v []byte) {
	if !s.has {
		s.bmin = append(s.bmin[:0], v...)
		s.bmax = append(s.bmax[:0], v...)
		s.has = true
		return
	}
	if bytes.Compare(v, s.bmin) < 0 {
		s.bmin = append(s.bmin[:0], v...)
	} else if bytes.Compare(v, s.bmax) > 0 {
		s.bmax = append(s.bmax[:0], v...)
	}
}

func (s *statAcc) bool(v bool) {
	s.has = true
	if v {
		s.boolT = true
	} else {
		s.boolF = true
	}
}

func (s *statAcc) bools(bits []byte, off, n int) {
	if n == 0 {
		return
	}
	ones := bitutil.CountSetBits(bits, off, n)
	if ones > 0 {
		s.bool(true)
	}
	if ones < n {
		s.bool(false)
	}
}

// finish stores the accumulated min and max as PLAIN bytes.
func (s *statAcc) finish(e *metadata.EncodedStatistics) {
	if !s.has {
		return
	}
	var lo, hi []byte
	switch s.kind {
	case sInt32:
		lo = binary.LittleEndian.AppendUint32(nil, uint32(int32(s.i64min)))
		hi = binary.LittleEndian.AppendUint32(nil, uint32(int32(s.i64max)))
	case sInt64:
		lo = binary.LittleEndian.AppendUint64(nil, uint64(s.i64min))
		hi = binary.LittleEndian.AppendUint64(nil, uint64(s.i64max))
	case sUint32:
		lo = binary.LittleEndian.AppendUint32(nil, uint32(s.u64min))
		hi = binary.LittleEndian.AppendUint32(nil, uint32(s.u64max))
	case sUint64:
		lo = binary.LittleEndian.AppendUint64(nil, s.u64min)
		hi = binary.LittleEndian.AppendUint64(nil, s.u64max)
	case sFloat32, sFloat64:
		// Parquet writers store -0.0 as the minimum and +0.0 as the
		// maximum when a bound is zero.
		fmin, fmax := s.fmin, s.fmax
		if fmin == 0 {
			fmin = math.Copysign(0, -1)
		}
		if fmax == 0 {
			fmax = 0
		}
		if s.kind == sFloat32 {
			lo = binary.LittleEndian.AppendUint32(nil, math.Float32bits(float32(fmin)))
			hi = binary.LittleEndian.AppendUint32(nil, math.Float32bits(float32(fmax)))
		} else {
			lo = binary.LittleEndian.AppendUint64(nil, math.Float64bits(fmin))
			hi = binary.LittleEndian.AppendUint64(nil, math.Float64bits(fmax))
		}
	case sBytes:
		if len(s.bmin) > maxStatBytes || len(s.bmax) > maxStatBytes {
			return
		}
		lo = append(make([]byte, 0, len(s.bmin)), s.bmin...)
		hi = append(make([]byte, 0, len(s.bmax)), s.bmax...)
	case sBool:
		lo = []byte{boolByte(!s.boolF)}
		hi = []byte{boolByte(s.boolT)}
	}
	e.SetMin(lo)
	e.SetMax(hi)
}

// maxStatBytes matches arrow-go's default statistics size limit.
const maxStatBytes = 4096

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func unsafeString(b []byte) string { return unsafe.String(unsafe.SliceData(b), len(b)) }
