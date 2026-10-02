package lazy_test

// lazyExpected holds polars 1.39.3 output for the lazy parity tests,
// rendered with internal/framerepr.
var lazyExpected = map[string]string{
	"explode l": `k:str|a:i64|f:f64|l:i64
"x"|1|1.5|1
"x"|1|1.5|2
"y"|2|null|3
"x"|null|2.5|null
"z"|4|nan|null
"y"|5|0.5|4
"y"|5|0.5|5
"x"|6|3.0|6`,
	"explode l l2": `k:str|a:i64|f:f64|l:i64|l2:str
"x"|1|1.5|1|"a"
"x"|1|1.5|2|"b"
"y"|2|null|3|"c"
"x"|null|2.5|null|null
"z"|4|nan|null|null
"y"|5|0.5|4|"d"
"y"|5|0.5|5|"e"
"x"|6|3.0|6|"f"`,
	"explode filter": `k:str|a:i64|f:f64|l:i64
"y"|2|null|3
"y"|5|0.5|4
"y"|5|0.5|5
"x"|6|3.0|6`,
	"unpivot": `k:str|variable:str|value:f64
"x"|"a"|1.0
"y"|"a"|2.0
"x"|"a"|null
"z"|"a"|4.0
"y"|"a"|5.0
"x"|"a"|6.0
"x"|"f"|1.5
"y"|"f"|null
"x"|"f"|2.5
"z"|"f"|nan
"y"|"f"|0.5
"x"|"f"|3.0`,
	"unpivot names": `k:str|var:str|val:i64
"x"|"a"|1
"y"|"a"|2
"x"|"a"|null
"z"|"a"|4
"y"|"a"|5
"x"|"a"|6`,
	"top_k": `k:str|a:i64
"x"|6
"y"|5`,
	"bottom_k": `k:str|a:i64
"x"|1
"y"|2`,
	"shift": `k:str|a:i64
null|null
"x"|1
"y"|2
"x"|null
"z"|4
"y"|5`,
	"shift fill": `a:i64
null
4
5
6
0
0`,
	"gather_every": `k:str
"y"
"z"
"x"`,
	"first": `k:str|a:i64
"x"|1`,
	"last": `k:str|a:i64
"x"|6`,
	"agg sum": `a:i64|f:f64
18|nan`,
	"agg mean": `a:f64|f:f64
3.6|nan`,
	"agg median": `a:f64|f:f64
4.0|2.5`,
	"agg min": `a:i64|f:f64
1|0.5`,
	"agg max": `a:i64|f:f64
6|3.0`,
	"agg std": `a:f64|f:f64
2.073644135332772|nan`,
	"agg var": `a:f64|f:f64
4.3|nan`,
	"agg count": `a:u32|f:u32
5|5`,
	"agg null_count": `a:u32|f:u32
1|1`,
	"agg quantile": `a:f64|f:f64
4.0|2.5`,
	"drop_nans": `k:str|f:f64
"x"|1.5
"y"|null
"x"|2.5
"y"|0.5
"x"|3.0`,
	"fill_null fwd": `a:i64
1
2
2
4
5
6`,
	"fill_null zero": `a:i64|f:f64
1|1.5
2|0.0
0|2.5
4|nan
5|0.5
6|3.0`,
	"clear": `k:str|a:i64
null|null
null|null`,
	"cast": `a:f64|f:f64
1.0|1.5
2.0|null
null|2.5
4.0|nan
5.0|0.5
6.0|3.0`,
	"with_row_index": `index:u32|k:str
0|"x"
1|"y"
2|"x"
3|"z"
4|"y"
5|"x"`,
	"unnest": `id:i64|p:i64|q:str
1|1|"u"
2|2|"v"`,
	"pivot sum": `k:str|y:i64|x:i64|w:i64
"a"|2|1|0
"b"|0|3|0
"c"|5|0|0`,
	"pivot first": `k:str|x:i64|z:i64
"a"|1|null
"b"|3|4
"c"|null|null`,
	"gb len": `k:str|len:u32
"x"|3
"y"|2
"z"|1`,
	"gb sum": `k:str|a:i64|f:f64
"x"|7|7.0
"y"|7|0.5
"z"|4|nan`,
	"gb mean": `k:str|a:f64|f:f64
"x"|3.5|2.3333333333333335
"y"|3.5|0.5
"z"|4.0|nan`,
	"gb median": `k:str|a:f64|f:f64
"x"|3.5|2.5
"y"|3.5|0.5
"z"|4.0|nan`,
	"gb n_unique": `k:str|a:u32|f:u32
"x"|3|3
"y"|2|2
"z"|1|1`,
	"gb first": `k:str|a:i64|f:f64
"x"|1|1.5
"y"|2|null
"z"|4|nan`,
	"gb last": `k:str|a:i64|f:f64
"x"|6|3.0
"y"|5|0.5
"z"|4|nan`,
	"gb head": `k:str|a:i64|f:f64
"x"|1|1.5
"y"|2|null
"z"|4|nan`,
	"gb tail": `k:str|a:i64|f:f64
"x"|6|3.0
"y"|5|0.5
"z"|4|nan`,
	"gb all": `k:str|a:list[i64]|f:list[f64]
"x"|[1,null,6]|[1.5,2.5,3.0]
"y"|[2,5]|[null,0.5]
"z"|[4]|[nan]`,
	"gb quantile": `k:str|a:f64|f:f64
"x"|6.0|2.5
"y"|5.0|0.5
"z"|4.0|nan`,
	"gb agg maintain": `k:str|s:i64|f:f64
"x"|7|3.0
"y"|7|0.5
"z"|4|nan`,
	"gb expr agg": `a:bool|f:f64
false|1.5
null|2.5
true|nan`,
	"gb expr alias agg": `p:bool|a:i64
false|3
null|0
true|15`,
	"gb expr sum": `a:bool|f:f64
false|1.5
null|2.5
true|nan`,
	"gb mixed keys": `k:str|a:bool|f:f64
"x"|false|1.5
"y"|false|0.0
"x"|null|2.5
"z"|true|nan
"y"|true|0.5
"x"|true|3.0`,
	"join_asof": `t:i64|l:str|r:i64
1|"a"|null
5|"b"|40
10|"c"|90`,
	"join_asof filter": `t:i64|l:str|r:i64
5|"b"|40
10|"c"|90`,
	"join_where": `t:i64|l:str|t2:i64|r:i64
5|"b"|2|20
5|"b"|4|40
10|"c"|2|20
10|"c"|4|40
10|"c"|9|90`,
	"merge_sorted": `k:i64|v:str
1|"a"
2|"x"
3|"b"
3|"y"
4|"z"
5|"c"`,
	"update": `A:i64|B:i64
1|-99
2|500
3|600
4|700
5|-66`,
}
