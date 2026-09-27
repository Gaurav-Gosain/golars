package analysis

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Gaurav-Gosain/golars/dataframe"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
	ioipc "github.com/Gaurav-Gosain/golars/io/ipc"
	iojson "github.com/Gaurav-Gosain/golars/io/json"
	ioparquet "github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/schema"
)

// fileInfo is what the analyser knows about a data file.
type fileInfo struct {
	mtime int64
	size  int64
	sch   *schema.Schema // nil when the file could not be read
	rows  int            // -1 when unknown
	err   error
}

var fileCache sync.Map // abs path -> fileInfo

// sampleBytes bounds how much of a text file is read to infer its
// schema; countBytes bounds the files whose rows are counted.
const (
	sampleBytes = 256 << 10
	countBytes  = 8 << 20
	// wholeBytes bounds binary and JSON files read in full to learn
	// their schema.
	wholeBytes = 16 << 20
)

// statFile returns the schema and row count of the file at abs,
// reading it at most once per modification.
func statFile(abs string) fileInfo {
	st, err := os.Stat(abs)
	if err != nil {
		return fileInfo{rows: -1, err: err}
	}
	if !st.Mode().IsRegular() {
		return fileInfo{rows: -1, err: os.ErrInvalid}
	}
	if v, found := fileCache.Load(abs); found {
		fi := v.(fileInfo)
		if fi.mtime == st.ModTime().UnixNano() && fi.size == st.Size() {
			return fi
		}
	}
	fi := fileInfo{mtime: st.ModTime().UnixNano(), size: st.Size(), rows: -1}
	fi.sch, fi.rows, fi.err = readSchema(abs, st.Size())
	fileCache.Store(abs, fi)
	return fi
}

func readSchema(abs string, size int64) (*schema.Schema, int, error) {
	ctx := context.Background()
	ext := strings.ToLower(filepath.Ext(abs))
	var df *dataframe.DataFrame
	var err error
	rows := -1
	switch ext {
	case ".csv", ".tsv":
		head, err := readHead(abs, sampleBytes)
		if err != nil {
			return nil, -1, err
		}
		opts := []iocsv.Option{iocsv.WithNullValues("")}
		if ext == ".tsv" {
			opts = append(opts, iocsv.WithDelimiter('\t'))
		}
		df, err = iocsv.Read(ctx, bytes.NewReader(head), opts...)
		if err != nil {
			return nil, -1, err
		}
		if size <= int64(len(head)) {
			rows = df.Height()
		} else if size <= countBytes {
			rows = countRecords(abs, ext == ".tsv")
		}
	case ".ndjson", ".jsonl":
		head, err := readHead(abs, sampleBytes)
		if err != nil {
			return nil, -1, err
		}
		df, err = iojson.ReadNDJSON(ctx, bytes.NewReader(head))
		if err != nil {
			return nil, -1, err
		}
		if size <= int64(len(head)) {
			rows = df.Height()
		}
	case ".parquet", ".pq":
		sch, err := ioparquet.ReadSchema(abs)
		return sch, -1, err
	case ".arrow", ".ipc", ".json":
		if size > wholeBytes {
			return nil, -1, nil
		}
		if ext == ".json" {
			df, err = iojson.ReadFile(ctx, abs)
		} else {
			df, err = ioipc.ReadFile(ctx, abs)
		}
		if err != nil {
			return nil, -1, err
		}
		rows = df.Height()
	default:
		return nil, -1, nil
	}
	defer df.Release()
	return df.Schema(), rows, nil
}

// readHead reads up to n bytes of path, cut after the last complete
// line when the file is longer.
func readHead(path string, n int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	k, err := io.ReadFull(f, buf)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, err
	}
	buf = buf[:k]
	if k == n {
		if i := bytes.LastIndexByte(buf, '\n'); i >= 0 {
			buf = buf[:i+1]
		}
	}
	return buf, nil
}

// countRecords counts the data rows of a delimited file.
func countRecords(path string, tsv bool) int {
	f, err := os.Open(path)
	if err != nil {
		return -1
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	r.ReuseRecord = true
	r.FieldsPerRecord = -1
	if tsv {
		r.Comma = '\t'
	}
	n := 0
	for {
		_, err := r.Read()
		if err == io.EOF {
			return max(n-1, 0)
		}
		if err != nil {
			return -1
		}
		n++
	}
}

// Resolve turns a path written in a script into an absolute path.
// Scripts run from the working directory, but an editor has no
// reliable one, so relative paths are tried against dir and each of
// its ancestors, then the process working directory. The first that
// exists wins; otherwise the dir-relative candidate is returned.
func Resolve(path, dir string) string {
	if filepath.IsAbs(path) {
		return path
	}
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[2:])
		}
	}
	var candidates []string
	if dir != "" {
		d := dir
		for {
			candidates = append(candidates, filepath.Join(d, path))
			parent := filepath.Dir(d)
			if parent == d {
				break
			}
			d = parent
		}
	}
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, path))
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	if len(candidates) > 0 {
		return candidates[0]
	}
	abs, _ := filepath.Abs(path)
	return abs
}
