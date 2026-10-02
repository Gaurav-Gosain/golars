// As-of join: attach the most recent quote to every trade, per symbol,
// with a staleness tolerance. Also shows the forward and nearest
// strategies and the lazy form.
// Run: go run ./examples/join_asof
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func at(hh, mm, ss int) time.Time {
	return time.Date(2024, 5, 1, hh, mm, ss, 0, time.UTC)
}

func main() {
	ctx := context.Background()

	// Both sides must be sorted by the "on" key (within each "by" group
	// is enough when By is set).
	tTime, _ := series.FromTimes("time", []time.Time{
		at(9, 30, 5), at(9, 31, 0), at(9, 33, 30), at(9, 36, 0), at(9, 37, 10),
	}, nil, dtype.Millisecond, "")
	tSym, _ := series.FromString("symbol", []string{"AAPL", "MSFT", "AAPL", "AAPL", "MSFT"}, nil)
	tQty, _ := series.FromInt64("qty", []int64{100, 50, 200, 75, 30}, nil)
	trades, _ := dataframe.New(tTime, tSym, tQty)
	defer trades.Release()

	qTime, _ := series.FromTimes("time", []time.Time{
		at(9, 30, 0), at(9, 30, 0), at(9, 32, 0), at(9, 33, 0), at(9, 36, 30),
	}, nil, dtype.Millisecond, "")
	qSym, _ := series.FromString("symbol", []string{"AAPL", "MSFT", "MSFT", "AAPL", "MSFT"}, nil)
	qBid, _ := series.FromFloat64("bid", []float64{189.10, 410.00, 410.25, 189.40, 411.10}, nil)
	quotes, _ := dataframe.New(qTime, qSym, qBid)
	defer quotes.Release()

	fmt.Println("trades:")
	fmt.Println(trades)
	fmt.Println("quotes:")
	fmt.Println(quotes)

	// Backward (the default): the last quote at or before each trade,
	// matched only within the same symbol, and at most 2 minutes old.
	backward, err := trades.JoinAsof(ctx, quotes, dataframe.AsofOptions{
		On:        "time",
		By:        []string{"symbol"},
		Tolerance: "2m",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer backward.Release()
	fmt.Println("backward, by=symbol, tolerance=2m:")
	fmt.Println(backward)

	// Forward: the first quote at or after each trade.
	forward, err := trades.JoinAsof(ctx, quotes, dataframe.AsofOptions{
		On:       "time",
		By:       []string{"symbol"},
		Strategy: dataframe.AsofForward,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer forward.Release()
	fmt.Println("forward, by=symbol:")
	fmt.Println(forward)

	// Nearest through the lazy API, keeping the matched quote time.
	nearest, err := lazy.FromDataFrame(trades).
		JoinAsof(lazy.FromDataFrame(quotes), dataframe.AsofOptions{
			On:         "time",
			By:         []string{"symbol"},
			Strategy:   dataframe.AsofNearest,
			NoCoalesce: true,
		}).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer nearest.Release()
	fmt.Println("nearest, by=symbol, lazy, right key kept:")
	fmt.Println(nearest)
}
