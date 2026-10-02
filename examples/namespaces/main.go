// Expression namespaces: list, struct, name, str and cat. Each
// namespace hangs off an Expr (e.List(), e.Struct(), e.Name(), e.Str(),
// e.Cat()) and mirrors the polars namespace of the same name.
// Run: go run ./examples/namespaces
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/expr"
	"github.com/Gaurav-Gosain/golars/lazy"
	"github.com/Gaurav-Gosain/golars/series"
)

func run(title string, df *dataframe.DataFrame, exprs ...expr.Expr) {
	out, err := lazy.FromDataFrame(df).Select(exprs...).Collect(context.Background())
	if err != nil {
		log.Fatalf("%s: %v", title, err)
	}
	defer out.Release()
	fmt.Println(title + ":")
	fmt.Println(out)
}

func main() {
	user, _ := series.FromString("user", []string{"ana", "bo", "cy", "di"}, nil)
	tags, _ := series.FromString("tags", []string{"go,db,go", "rust", "", "py,go,ml"}, nil)
	scores, _ := series.FromString("scores", []string{"3 9 4", "7", "5 5", "10 2 8 1"}, nil)
	email, _ := series.FromString("email", []string{"Ana@Example.com", "bo@test.org", "cy@example.com", "DI@Test.org"}, nil)
	x, _ := series.FromInt64("x", []int64{1, 2, 3, 4}, nil)
	y, _ := series.FromFloat64("y", []float64{0.5, 1.5, 2.5, 3.5}, nil)
	df, _ := dataframe.New(user, tags, scores, email, x, y)
	defer df.Release()

	// list: split strings into List<str>, then reduce or transform each list.
	tagList := expr.Col("tags").Str().Split(",")
	nums := expr.Col("scores").Str().Split(" ").List().Eval(expr.Element().Cast(dtype.Int64()))
	run("list", df,
		expr.Col("user"),
		tagList.List().Len().Alias("n_tags"),
		tagList.List().Contains("go").Alias("has_go"),
		tagList.List().Unique(true).List().JoinWith("|", true).Alias("unique_tags"),
		nums.List().Sum().Alias("total"),
		nums.List().Get(0).Alias("first"),
		nums.List().Sort(true, false).Alias("desc"),
		nums.List().Eval(expr.Element().Mul(expr.Lit(int64(10)))).Alias("times10"),
	)

	// struct: pack columns into one struct, then read, rename and unnest.
	point := expr.Struct(expr.Col("x"), expr.Col("y")).Alias("point")
	packed, err := lazy.FromDataFrame(df).Select(expr.Col("user"), point).Collect(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer packed.Release()
	fmt.Println("struct:")
	fmt.Println(packed)
	pt := expr.Col("point")
	run("struct field / rename + unnest", packed,
		expr.Col("user"),
		pt.Struct().Field("y").Alias("point_y"),
		pt.Struct().RenameFields("px", "py").Struct().Unnest(),
	)

	// name: rename outputs without touching values.
	run("name", df,
		expr.Col("x").Name().Prefix("raw_"),
		expr.Col("y").Mul(expr.Lit(2.0)).Name().Suffix("_x2"),
		expr.Col("user").Name().Map(strings.ToUpper),
	)

	// str: a few of the polars string helpers.
	run("str", df,
		expr.Col("email").Str().ToLower().Alias("lower"),
		expr.Col("email").Str().Extract(`@(.+)$`, 1).Str().ToLower().Alias("domain"),
		expr.Col("email").Str().SplitN("@", 2).Struct().Field("field_0").Alias("local"),
		expr.Col("user").Str().ToTitlecase().Alias("title"),
		expr.Col("user").Str().PadStart(5, '.').Alias("padded"),
	)

	// cat: cast to Categorical, then query the dictionary.
	domain := expr.Col("email").Str().Extract(`@(.+)$`, 1).Str().ToLower().Cast(dtype.Categorical()).Alias("domain")
	withCat, err := lazy.FromDataFrame(df).Select(domain).Collect(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer withCat.Release()
	fmt.Println("cat:")
	fmt.Println(withCat)
	run("cat.get_categories", withCat, expr.Col("domain").Cat().GetCategories())
	run("cat.starts_with", withCat,
		expr.Col("domain"),
		expr.Col("domain").Cat().StartsWith("test").Alias("is_test"),
	)
}
