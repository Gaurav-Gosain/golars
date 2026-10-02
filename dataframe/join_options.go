package dataframe

import (
	"fmt"
	"strings"
)

// JoinValidation checks the uniqueness of the join keys before joining,
// like the validate argument of polars' DataFrame.join.
type JoinValidation uint8

const (
	// ValidateManyToMany performs no check (the default).
	ValidateManyToMany JoinValidation = iota
	// ValidateOneToOne requires unique keys on both sides.
	ValidateOneToOne
	// ValidateOneToMany requires unique keys on the left side.
	ValidateOneToMany
	// ValidateManyToOne requires unique keys on the right side.
	ValidateManyToOne
)

func (v JoinValidation) String() string {
	switch v {
	case ValidateOneToOne:
		return "1:1"
	case ValidateOneToMany:
		return "1:m"
	case ValidateManyToOne:
		return "m:1"
	}
	return "m:m"
}

// ParseJoinValidation parses "m:m", "1:1", "1:m" or "m:1".
func ParseJoinValidation(s string) (JoinValidation, error) {
	switch strings.ToLower(s) {
	case "m:m", "":
		return ValidateManyToMany, nil
	case "1:1":
		return ValidateOneToOne, nil
	case "1:m":
		return ValidateOneToMany, nil
	case "m:1":
		return ValidateManyToOne, nil
	}
	return 0, fmt.Errorf("dataframe.Join: unknown validation %q: want m:m, 1:1, 1:m or m:1", s)
}

// JoinOrder is the row order a join guarantees, like the maintain_order
// argument of polars' DataFrame.join.
type JoinOrder uint8

const (
	// JoinOrderNone guarantees no order. golars keeps the left order
	// for inner, left, full, semi and anti joins and the right order for
	// right joins, but callers must not rely on it.
	JoinOrderNone JoinOrder = iota
	// JoinOrderLeft keeps the order of the left frame.
	JoinOrderLeft
	// JoinOrderRight keeps the order of the right frame.
	JoinOrderRight
	// JoinOrderLeftRight keeps the left order, then the right order
	// among rows with the same left row.
	JoinOrderLeftRight
	// JoinOrderRightLeft keeps the right order, then the left order
	// among rows with the same right row.
	JoinOrderRightLeft
)

func (o JoinOrder) String() string {
	switch o {
	case JoinOrderLeft:
		return "left"
	case JoinOrderRight:
		return "right"
	case JoinOrderLeftRight:
		return "left_right"
	case JoinOrderRightLeft:
		return "right_left"
	}
	return "none"
}

// ParseJoinOrder parses "none", "left", "right", "left_right" or
// "right_left".
func ParseJoinOrder(s string) (JoinOrder, error) {
	switch strings.ToLower(s) {
	case "none", "":
		return JoinOrderNone, nil
	case "left":
		return JoinOrderLeft, nil
	case "right":
		return JoinOrderRight, nil
	case "left_right":
		return JoinOrderLeftRight, nil
	case "right_left":
		return JoinOrderRightLeft, nil
	}
	return 0, fmt.Errorf("dataframe.Join: unknown maintain_order %q: want none, left, right, left_right or right_left", s)
}

// ParseJoinType parses a polars join strategy name: inner, left, right,
// full (or outer), semi, anti or cross.
func ParseJoinType(s string) (JoinType, error) {
	switch strings.ToLower(s) {
	case "inner":
		return InnerJoin, nil
	case "left":
		return LeftJoin, nil
	case "right":
		return RightJoin, nil
	case "full", "outer":
		return FullJoin, nil
	case "semi":
		return SemiJoin, nil
	case "anti":
		return AntiJoin, nil
	case "cross":
		return CrossJoin, nil
	}
	return 0, fmt.Errorf("unknown join type %q: want inner, left, right, full, semi, anti or cross", s)
}

// WithJoinKeys joins left column leftOn[i] with right column rightOn[i]
// (polars' left_on and right_on). The on argument of Join is ignored
// when this option is set.
func WithJoinKeys(leftOn, rightOn []string) JoinOption {
	return func(c *joinConfig) {
		c.leftOn = leftOn
		c.rightOn = rightOn
	}
}

// WithJoinCoalesce sets whether the key columns of both sides are merged
// into one. The default (like polars) coalesces every join type except
// full joins.
func WithJoinCoalesce(coalesce bool) JoinOption {
	return func(c *joinConfig) {
		if coalesce {
			c.coalesce = 1
		} else {
			c.coalesce = 0
		}
	}
}

// WithJoinNullsEqual makes null keys match each other (polars'
// nulls_equal). By default a null key never matches.
func WithJoinNullsEqual(equal bool) JoinOption {
	return func(c *joinConfig) { c.nullsEqual = equal }
}

// WithJoinValidate checks key uniqueness before joining.
func WithJoinValidate(v JoinValidation) JoinOption {
	return func(c *joinConfig) { c.validate = v }
}

// WithJoinMaintainOrder sets the row order the join must keep.
func WithJoinMaintainOrder(o JoinOrder) JoinOption {
	return func(c *joinConfig) { c.order = o }
}

// JoinSpec is the resolved form of a join's arguments and options. The
// lazy planner uses it to derive the output schema and to decide which
// filters can move below a join.
type JoinSpec struct {
	How     JoinType
	LeftOn  []string
	RightOn []string
	Suffix  string
	// Coalesce is the effective setting after applying the default.
	Coalesce   bool
	NullsEqual bool
	Validate   JoinValidation
	Order      JoinOrder
}

// ResolveJoinSpec applies opts to the Join arguments on and how.
func ResolveJoinSpec(on []string, how JoinType, opts ...JoinOption) JoinSpec {
	cfg := resolveJoin(opts)
	return cfg.spec(on, how)
}

func (c joinConfig) spec(on []string, how JoinType) JoinSpec {
	s := JoinSpec{
		How:        how,
		LeftOn:     on,
		RightOn:    on,
		Suffix:     c.suffix,
		NullsEqual: c.nullsEqual,
		Validate:   c.validate,
		Order:      c.order,
	}
	if c.leftOn != nil || c.rightOn != nil {
		s.LeftOn, s.RightOn = c.leftOn, c.rightOn
	}
	switch c.coalesce {
	case 0:
		s.Coalesce = false
	case 1:
		s.Coalesce = true
	default:
		s.Coalesce = how != FullJoin
	}
	if how == CrossJoin {
		s.LeftOn, s.RightOn = nil, nil
	}
	return s
}
