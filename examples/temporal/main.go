// Temporal toolkit: parse strings into datetimes, extract calendar
// fields, truncate and format, convert time zones, bucket rows into
// time windows with GroupByDynamic, and compute a time-based rolling
// mean.
// Run: go run ./examples/temporal
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Gaurav-Gosain/golars"
	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	ctx := context.Background()

	// Sensor readings logged as text, the way they often arrive in a CSV.
	ts, _ := series.FromString("ts", []string{
		"2024-03-01 08:05:00",
		"2024-03-01 08:40:00",
		"2024-03-01 09:10:00",
		"2024-03-01 09:55:00",
		"2024-03-01 10:20:00",
		"2024-03-02 08:15:00",
		"2024-03-02 09:30:00",
	}, nil)
	temp, _ := series.FromFloat64("temp", []float64{18.5, 19.0, 20.5, 21.0, 22.5, 17.5, 19.5}, nil)
	raw, _ := dataframe.New(ts, temp)
	defer raw.Release()

	// 1. Parse with a strftime-style format.
	df, err := lazy.FromDataFrame(raw).
		WithColumns(expr.Col("ts").Str().ToDatetime("%Y-%m-%d %H:%M:%S")).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer df.Release()
	fmt.Println("parsed:")
	fmt.Println(df)

	// 2. Calendar fields, truncation, formatting and time zones.
	ts0 := expr.Col("ts")
	fields, err := lazy.FromDataFrame(df).
		Select(
			ts0,
			ts0.Dt().Year().Alias("year"),
			ts0.Dt().Month().Alias("month"),
			ts0.Dt().Weekday().Alias("weekday"),
			ts0.Dt().Hour().Alias("hour"),
			ts0.Dt().Truncate("1h").Alias("hour_start"),
			ts0.Dt().Strftime("%a %d %b %H:%M").Alias("label"),
			ts0.Dt().ReplaceTimeZone("UTC").Dt().ConvertTimeZone("America/New_York").Alias("new_york"),
		).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer fields.Release()
	fmt.Println("calendar fields:")
	fmt.Println(fields)

	// 3. Hourly buckets. The index column must be sorted ascending.
	hourly, err := lazy.FromDataFrame(df).
		GroupByDynamic("ts", dataframe.DynamicGroupOptions{Every: "1h"}).
		Agg(
			expr.Col("temp").Mean().Alias("avg_temp"),
			expr.Col("temp").Count().Alias("n"),
		).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer hourly.Release()
	fmt.Println("group_by_dynamic every=1h:")
	fmt.Println(hourly)

	// 4. Rolling mean over the trailing two hours of each row.
	rolled, err := lazy.FromDataFrame(df).
		WithColumns(expr.Col("temp").RollingMeanBy(expr.Col("ts"), "2h").Alias("temp_2h")).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer rolled.Release()
	fmt.Println("rolling_mean_by window=2h:")
	fmt.Println(rolled)

	// 5. A calendar of dates, one per week.
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	days, err := golars.DateRangeSeries("week", start, end, "1w", "both")
	if err != nil {
		log.Fatal(err)
	}
	cal, _ := dataframe.New(days)
	defer cal.Release()
	withEnd, err := lazy.FromDataFrame(cal).
		WithColumns(
			expr.Col("week").Dt().MonthEnd().Alias("month_end"),
			expr.Col("week").Dt().OffsetBy("3d").Alias("plus_3d"),
		).
		Collect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer withEnd.Release()
	fmt.Println("date_range interval=1w:")
	fmt.Println(withEnd)
}
