package difftest

import (
	"fmt"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/lazy"
)

// golarsJoin runs a join op through the lazy API.
func golarsJoin(lf, rl lazy.LazyFrame, op Op) (lazy.LazyFrame, error) {
	how, err := dataframe.ParseJoinType(op.How)
	if err != nil {
		return lf, fmt.Errorf("%w: join how=%s", ErrUnsupported, op.How)
	}
	var opts []dataframe.JoinOption
	if len(op.RightOn) > 0 {
		opts = append(opts, dataframe.WithJoinKeys(op.Keys, op.RightOn))
	}
	if op.Suffix != "" {
		opts = append(opts, dataframe.WithJoinSuffix(op.Suffix))
	}
	switch op.Coalesce {
	case "true":
		opts = append(opts, dataframe.WithJoinCoalesce(true))
	case "false":
		opts = append(opts, dataframe.WithJoinCoalesce(false))
	}
	if op.NullsEqual {
		opts = append(opts, dataframe.WithJoinNullsEqual(true))
	}
	if op.Validate != "" {
		v, err := dataframe.ParseJoinValidation(op.Validate)
		if err != nil {
			return lf, err
		}
		opts = append(opts, dataframe.WithJoinValidate(v))
	}
	if op.JoinOrder != "" {
		o, err := dataframe.ParseJoinOrder(op.JoinOrder)
		if err != nil {
			return lf, err
		}
		opts = append(opts, dataframe.WithJoinMaintainOrder(o))
	}
	keys := op.Keys
	if how == dataframe.CrossJoin {
		keys = nil
	}
	return lf.Join(rl, keys, how, opts...), nil
}
