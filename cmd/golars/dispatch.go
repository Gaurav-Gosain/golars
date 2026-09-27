package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Gaurav-Gosain/golars/script"
)

// call is one parsed statement handed to a command handler.
type call struct {
	spec *script.CommandSpec
	// args are the whitespace-separated arguments after the command.
	args []string
	// rest is the raw text after the command, for handlers with their
	// own grammar (filter, with).
	rest string
}

// usage returns the standard error for a malformed statement. The
// text comes from the command's spec signature.
func (c *call) usage() error {
	return &usageError{sig: c.spec.Signature}
}

type usageError struct{ sig string }

func (e *usageError) Error() string { return "usage: " + e.sig }

type handlerFunc func(s *state, c *call) error

// handlers maps each canonical command name in script.Commands to its
// implementation. Aliases resolve through script.FindCommand, so they
// never appear here. TestEveryCommandHasHandler keeps the two in sync.
var handlers map[string]handlerFunc

func init() {
	handlers = map[string]handlerFunc{
		// io
		"load":         cmdLoad,
		"save":         cmdSave,
		"scan_csv":     scanCmd("csv"),
		"scan_parquet": scanCmd("parquet"),
		"scan_ipc":     scanCmd("ipc"),
		"scan_json":    scanCmd("json"),
		"scan_ndjson":  scanCmd("ndjson"),
		"scan_auto":    scanCmd(""),

		// frames
		"use":        cmdUse,
		"stash":      cmdStash,
		"frames":     cmdFrames,
		"drop_frame": cmdDropFrame,

		// pipeline
		"select":           cmdSelect,
		"drop":             cmdDrop,
		"filter":           cmdFilter,
		"sort":             cmdSort,
		"limit":            cmdLimit,
		"groupby":          cmdGroupBy,
		"group_by_dynamic": cmdGroupByDynamic,
		"join":             cmdJoin,
		"join_asof":        cmdJoinAsof,
		"with":             cmdWith,
		"collect":          cmdCollect,
		"reset":            cmdReset,
		"reverse":          cmdReverse,
		"unique":           cmdUnique,
		"cast":             cmdCast,
		"fill_null":        cmdFillNull,
		"drop_null":        cmdDropNull,
		"fill_nan":         cmdFillNan,
		"forward_fill":     fillCmd(true),
		"backward_fill":    fillCmd(false),
		"rename":           cmdRename,
		"with_row_index":   cmdWithRowIndex,
		"sum_horizontal":   horizontalCmd,
		"mean_horizontal":  horizontalCmd,
		"min_horizontal":   horizontalCmd,
		"max_horizontal":   horizontalCmd,
		"all_horizontal":   horizontalCmd,
		"any_horizontal":   horizontalCmd,

		// reshape
		"sample":     cmdSample,
		"shuffle":    cmdShuffle,
		"top_k":      topKCmd(true),
		"bottom_k":   topKCmd(false),
		"transpose":  cmdTranspose,
		"unpivot":    cmdUnpivot,
		"pivot":      cmdPivot,
		"unnest":     cmdUnnest,
		"explode":    cmdExplode,
		"to_dummies": cmdToDummies,
		"upsample":   cmdUpsample,

		// inspect
		"show":         cmdHead,
		"head":         cmdHead,
		"tail":         cmdTail,
		"schema":       cmdSchema,
		"describe":     cmdDescribe,
		"glimpse":      cmdGlimpse,
		"size":         cmdSize,
		"null_count":   cmdNullCount,
		"ishow":        cmdInteractiveShow,
		"partition_by": cmdPartitionBy,

		// aggregate
		"sum":             scalarAggCmd,
		"mean":            scalarAggCmd,
		"min":             scalarAggCmd,
		"max":             scalarAggCmd,
		"median":          scalarAggCmd,
		"std":             scalarAggCmd,
		"skew":            scalarAggCmd,
		"kurtosis":        scalarAggCmd,
		"approx_n_unique": scalarAggCmd,
		"corr":            pairStatCmd,
		"cov":             pairStatCmd,
		"sum_all":         frameAggCmd,
		"mean_all":        frameAggCmd,
		"min_all":         frameAggCmd,
		"max_all":         frameAggCmd,
		"std_all":         frameAggCmd,
		"var_all":         frameAggCmd,
		"median_all":      frameAggCmd,
		"count_all":       frameAggCmd,
		"null_count_all":  frameAggCmd,

		// plan
		"explain":      cmdExplain,
		"explain_tree": cmdExplainTree,
		"graph":        cmdGraph,
		"mermaid":      cmdMermaid,

		// session
		"source": cmdSource,
		"help":   cmdHelp,
		"exit":   cmdExit,
		"timing": cmdTiming,
		"info":   cmdInfo,
		"clear":  cmdClear,
		"pwd":    cmdPwd,
		"ls":     cmdLs,
		"cd":     cmdCd,
	}
}

// handle runs one statement. It accepts both the REPL form (".cmd
// args") and the script form ("cmd args"), and strips comments so
// pasted script lines work at the prompt.
func (s *state) handle(line string) error {
	line = script.Normalize(line)
	if line == "" {
		return nil
	}
	s.evalCount++
	fields := strings.Fields(line)
	verb := strings.TrimPrefix(fields[0], ".")
	spec := script.FindCommand(verb)
	if spec == nil {
		return script.UnknownCommand(verb)
	}
	h, found := handlers[spec.Name]
	if !found {
		return fmt.Errorf("%s: no handler registered", spec.Name)
	}
	c := &call{
		spec: spec,
		args: fields[1:],
		rest: strings.TrimSpace(strings.TrimPrefix(line, fields[0])),
	}
	err := h(s, c)
	var ue *usageError
	if err == nil || errors.As(err, &ue) || errors.Is(err, errExit) || errors.Is(err, errNoFrame) {
		return err
	}
	return fmt.Errorf("%s: %w", spec.Name, err)
}

// parseNat parses a non-negative decimal integer. Signs, spaces and
// values that overflow int are rejected.
func parseNat(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(s)
	return n, err == nil
}

// optCount returns args[i] parsed as a row count, or def when the
// argument is absent.
func optCount(args []string, i, def int, what string) (int, error) {
	if len(args) <= i {
		return def, nil
	}
	n, isNat := parseNat(args[i])
	if !isNat {
		return 0, fmt.Errorf("invalid %s %q: want a non-negative integer", what, args[i])
	}
	return n, nil
}

// optSeed returns args[i] parsed as a PRNG seed, or 42 when absent.
func optSeed(args []string, i int) (uint64, error) {
	if len(args) <= i {
		return 42, nil
	}
	v, err := strconv.ParseUint(args[i], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid seed %q", args[i])
	}
	return v, nil
}

// columnList flattens arguments that may be comma- or space-separated
// ("a,b c" and "a, b" both give [a b c] / [a b]).
func columnList(args []string) []string {
	var out []string
	for _, a := range args {
		for p := range strings.SplitSeq(a, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

// parseScalar converts a CLI argument to a Go value: true/false,
// int64, float64, or a string with surrounding double quotes removed.
func parseScalar(raw string) any {
	raw = strings.TrimSpace(raw)
	switch raw {
	case "true":
		return true
	case "false":
		return false
	}
	if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return v
	}
	if v, err := strconv.ParseFloat(raw, 64); err == nil {
		return v
	}
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		return raw[1 : len(raw)-1]
	}
	return raw
}
