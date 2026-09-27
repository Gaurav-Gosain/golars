package difftest

import "slices"

// OutputName is the column name polars gives a top-level expression:
// the alias if any, otherwise the leftmost column in the tree.
func OutputName(e *Expr) string {
	if e.K == "call" && e.Name == "alias" && len(e.Args) == 2 {
		if s, ok := e.Args[1].Val.(string); ok {
			return s
		}
	}
	name := ""
	e.Walk(func(x *Expr) {
		if name == "" && x.K == "col" {
			name = x.Name
		}
	})
	if name == "" {
		return "literal"
	}
	return name
}

// DeriveOrder computes the order contract of a plan's result from its
// ops alone. The generator and the shrinker both use it, so removing
// an op during shrinking never leaves a stale contract.
func DeriveOrder(ops []Op) Order {
	o := Order{Mode: OrderExact}
	degrade := func() {
		switch o.Mode {
		case OrderSorted:
			o = Order{Mode: OrderUnordered}
		case OrderKeysOnly:
			o = Order{Mode: OrderCountOnly}
		}
	}
	touches := func(names []string) {
		for _, k := range o.Keys {
			if slices.Contains(names, k) {
				degrade()
				return
			}
		}
	}
	for _, op := range ops {
		switch op.Op {
		case "with_columns":
			var names []string
			for _, e := range op.Exprs {
				names = append(names, OutputName(e))
			}
			touches(names)
		case "select":
			if op.ResetsOrder {
				o = Order{Mode: OrderExact}
				continue
			}
			var names []string
			for _, e := range op.Exprs {
				names = append(names, OutputName(e))
			}
			for _, k := range o.Keys {
				if !slices.Contains(names, k) {
					degrade()
					break
				}
			}
		case "drop", "explode", "rename":
			touches(op.Keys)
		case "group_by", "unique":
			if !(op.Maintain && o.Mode == OrderExact) {
				o = Order{Mode: OrderUnordered}
			}
		case "sort":
			switch o.Mode {
			case OrderCountOnly, OrderKeysOnly:
				// The rows were an arbitrary subset; sorting them
				// does not make their values comparable.
				o = Order{Mode: OrderCountOnly}
			case OrderUnordered, OrderSorted:
				o = Order{Mode: OrderSorted, Keys: slices.Clone(op.Keys)}
			}
		case "join":
			if o.Mode != OrderCountOnly && !(o.Mode == OrderExact && JoinKeepsOrder(op)) {
				o = Order{Mode: OrderUnordered}
			}
		case "slice", "head", "tail":
			switch o.Mode {
			case OrderSorted:
				o.Mode = OrderKeysOnly
			case OrderUnordered:
				o.Mode = OrderCountOnly
			}
		case "unpivot":
			if o.Mode != OrderExact && o.Mode != OrderCountOnly {
				o = Order{Mode: OrderUnordered}
			}
		}
	}
	return o
}
