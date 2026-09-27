package dataframe_test

// Code below is pasted from polars 1.39.3 output (prepr.rep).

var gbExpected = map[string]string{
	"Len": `k:str|len:u32
"a"|3
"b"|2
null|1`,
	"LenNamed": `k:str|cnt:u32
"a"|3
"b"|2
null|1`,
	"Count": `k:str|count:u32
"a"|3
"b"|2
null|1`,
	"First": `k:str|i8:i8|i32:i32|i64:i64|u8:u8|u64:u64|f32:f32|f64:f64|b:bool|d:date|dt:datetime[us]|n:null|s:str
"a"|1|1|10|1|1|1.5|1.0|true|2020-01-01|2020-01-01T00:00:00.000000|null|"x"
"b"|2|2|null|2|2|2.5|nan|false|2020-01-02|null|null|"y"
null|4|4|40|4|4|4.0|null|true|2020-01-04|2020-01-04T00:00:00.000000|null|"z"`,
	"Last": `k:str|i8:i8|i32:i32|i64:i64|u8:u8|u64:u64|f32:f32|f64:f64|b:bool|d:date|dt:datetime[us]|n:null|s:str
"a"|6|null|60|6|6|6.0|-1.0|false|2020-01-06|2020-01-06T12:00:00.000000|null|"w"
"b"|5|5|50|5|5|5.0|2.0|true|2020-01-05|2020-01-05T00:00:00.000000|null|"y"
null|4|4|40|4|4|4.0|null|true|2020-01-04|2020-01-04T00:00:00.000000|null|"z"`,
	"Sum": `k:str|i8:i64|i32:i32|i64:i64|u8:i64|u64:u64|f32:f32|f64:f64|b:u32|d:date|dt:datetime[us]|n:null
"a"|7|4|100|10|10|7.5|3.0|1|null|null|null
"b"|7|7|50|7|7|7.5|nan|1|null|null|null
null|4|4|40|4|4|4.0|0.0|1|null|null|null`,
	"Mean": `k:str|i8:f64|i32:f64|i64:f64|u8:f64|u64:f64|f32:f32|f64:f64|b:f64|d:datetime[us]|dt:datetime[us]|n:null|s:str
"a"|3.5|2.0|33.333333333333336|3.3333333333333335|3.3333333333333335|3.75|1.0|0.5|2020-01-03T12:00:00.000000|2020-01-03T12:00:00.000000|null|null
"b"|3.5|3.5|50.0|3.5|3.5|3.75|nan|0.5|2020-01-03T12:00:00.000000|2020-01-05T00:00:00.000000|null|null
null|4.0|4.0|40.0|4.0|4.0|4.0|null|1.0|2020-01-04T00:00:00.000000|2020-01-04T00:00:00.000000|null|null`,
	"Median": `k:str|i8:f64|i32:f64|i64:f64|u8:f64|u64:f64|f32:f32|f64:f64|b:f64|d:datetime[us]|dt:datetime[us]|n:null|s:str
"a"|3.5|2.0|30.0|3.0|3.0|3.75|1.0|0.5|2020-01-03T12:00:00.000000|2020-01-03T00:00:00.000000|null|null
"b"|3.5|3.5|50.0|3.5|3.5|3.75|nan|0.5|2020-01-03T12:00:00.000000|2020-01-05T00:00:00.000000|null|null
null|4.0|4.0|40.0|4.0|4.0|4.0|null|1.0|2020-01-04T00:00:00.000000|2020-01-04T00:00:00.000000|null|null`,
	"Min": `k:str|i8:i8|i32:i32|i64:i64|u8:u8|u64:u64|f32:f32|f64:f64|b:bool|d:date|dt:datetime[us]|n:null|s:str
"a"|1|1|10|1|1|1.5|-1.0|false|2020-01-01|2020-01-01T00:00:00.000000|null|"w"
"b"|2|2|50|2|2|2.5|2.0|false|2020-01-02|2020-01-05T00:00:00.000000|null|"y"
null|4|4|40|4|4|4.0|null|true|2020-01-04|2020-01-04T00:00:00.000000|null|"z"`,
	"Max": `k:str|i8:i8|i32:i32|i64:i64|u8:u8|u64:u64|f32:f32|f64:f64|b:bool|d:date|dt:datetime[us]|n:null|s:str
"a"|6|3|60|6|6|6.0|3.0|true|2020-01-06|2020-01-06T12:00:00.000000|null|"x"
"b"|5|5|50|5|5|5.0|2.0|true|2020-01-05|2020-01-05T00:00:00.000000|null|"y"
null|4|4|40|4|4|4.0|null|true|2020-01-04|2020-01-04T00:00:00.000000|null|"z"`,
	"NUnique": `k:str|i8:u32|i32:u32|i64:u32|u8:u32|u64:u32|f32:u32|f64:u32|b:u32|d:u32|dt:u32|n:u32|s:u32
"a"|3|3|3|3|3|3|3|3|3|3|1|3
"b"|2|2|2|2|2|2|2|2|2|2|1|1
null|1|1|1|1|1|1|1|1|1|1|1|1`,
	"QuantileNearest": `k:str|i8:f64|i32:f64|i64:f64|u8:f64|u64:f64|f32:f32|f64:f64|b:bool|d:datetime[us]|dt:datetime[us]|n:null|s:str
"a"|6.0|3.0|30.0|3.0|3.0|6.0|1.0|null|2020-01-06T00:00:00.000000|2020-01-03T00:00:00.000000|null|null
"b"|5.0|5.0|50.0|5.0|5.0|5.0|nan|null|2020-01-05T00:00:00.000000|2020-01-05T00:00:00.000000|null|null
null|4.0|4.0|40.0|4.0|4.0|4.0|null|null|2020-01-04T00:00:00.000000|2020-01-04T00:00:00.000000|null|null`,
	"QuantileLinear": `k:str|i8:f64|i32:f64|i64:f64|u8:f64|u64:f64|f32:f32|f64:f64|b:bool|d:datetime[us]|dt:datetime[us]|n:null|s:str
"a"|2.5|1.6|22.0|2.2|2.2|2.8499999046325684|0.19999999999999996|null|2020-01-02T12:00:00.000000|2020-01-02T04:48:00.000000|null|null
"b"|2.9|2.9|50.0|2.9|2.9|3.25|nan|null|2020-01-02T21:36:00.000000|2020-01-05T00:00:00.000000|null|null
null|4.0|4.0|40.0|4.0|4.0|4.0|null|null|2020-01-04T00:00:00.000000|2020-01-04T00:00:00.000000|null|null`,
	"QuantileHigher": `k:str|i8:f64|i32:f64|i64:f64|u8:f64|u64:f64|f32:f32|f64:f64|b:bool|d:datetime[us]|dt:datetime[us]|n:null
"a"|6.0|3.0|30.0|3.0|3.0|6.0|1.0|null|2020-01-06T00:00:00.000000|2020-01-03T00:00:00.000000|null
"b"|5.0|5.0|50.0|5.0|5.0|5.0|nan|null|2020-01-05T00:00:00.000000|2020-01-05T00:00:00.000000|null
null|4.0|4.0|40.0|4.0|4.0|4.0|null|null|2020-01-04T00:00:00.000000|2020-01-04T00:00:00.000000|null`,
	"QuantileLower": `k:str|i8:f64|i32:f64|i64:f64|u8:f64|u64:f64|f32:f32|f64:f64|b:bool|d:datetime[us]|dt:datetime[us]|n:null
"a"|1.0|1.0|30.0|3.0|3.0|1.5|1.0|null|2020-01-01T00:00:00.000000|2020-01-03T00:00:00.000000|null
"b"|2.0|2.0|50.0|2.0|2.0|2.5|2.0|null|2020-01-02T00:00:00.000000|2020-01-05T00:00:00.000000|null
null|4.0|4.0|40.0|4.0|4.0|4.0|null|null|2020-01-04T00:00:00.000000|2020-01-04T00:00:00.000000|null`,
	"QuantileMidpoint": `k:str|i8:f64|i32:f64|i64:f64|u8:f64|u64:f64|f32:f32|f64:f64|b:bool|d:datetime[us]|dt:datetime[us]|n:null
"a"|3.5|2.0|20.0|2.0|2.0|3.75|0.0|null|2020-01-03T12:00:00.000000|2020-01-02T00:00:00.000000|null
"b"|3.5|3.5|50.0|3.5|3.5|3.75|nan|null|2020-01-03T12:00:00.000000|2020-01-05T00:00:00.000000|null
null|4.0|4.0|40.0|4.0|4.0|4.0|null|null|2020-01-04T00:00:00.000000|2020-01-04T00:00:00.000000|null`,
	"QuantileEquiprobable": `k:str|i8:f64|i32:f64|i64:f64|u8:f64|u64:f64|f32:f32|f64:f64|b:bool|d:datetime[us]|dt:datetime[us]|n:null
"a"|1.0|1.0|10.0|1.0|1.0|1.5|-1.0|null|2020-01-01T00:00:00.000000|2020-01-01T00:00:00.000000|null
"b"|2.0|2.0|50.0|2.0|2.0|2.5|2.0|null|2020-01-02T00:00:00.000000|2020-01-05T00:00:00.000000|null
null|4.0|4.0|40.0|4.0|4.0|4.0|null|null|2020-01-04T00:00:00.000000|2020-01-04T00:00:00.000000|null`,
	"All": `k:str|i8:list[i8]|i32:list[i32]|i64:list[i64]|u8:list[u8]|u64:list[u64]|f32:list[f32]|f64:list[f64]|b:list[bool]|d:list[date]|dt:list[datetime[us]]|n:list[null]|s:list[str]
"a"|[1,null,6]|[1,3,null]|[10,30,60]|[1,3,6]|[1,3,6]|[1.5,null,6.0]|[1.0,3.0,-1.0]|[true,null,false]|[2020-01-01,null,2020-01-06]|[2020-01-01T00:00:00.000000,2020-01-03T00:00:00.000000,2020-01-06T12:00:00.000000]|[null,null,null]|["x",null,"w"]
"b"|[2,5]|[2,5]|[null,50]|[2,5]|[2,5]|[2.5,5.0]|[nan,2.0]|[false,true]|[2020-01-02,2020-01-05]|[null,2020-01-05T00:00:00.000000]|[null,null]|["y","y"]
null|[4]|[4]|[40]|[4]|[4]|[4.0]|[null]|[true]|[2020-01-04]|[2020-01-04T00:00:00.000000]|[null]|["z"]`,
	"Head": `k:str|i8:i8|i32:i32|i64:i64|u8:u8|u64:u64|f32:f32|f64:f64|b:bool|d:date|dt:datetime[us]|n:null|s:str
"a"|1|1|10|1|1|1.5|1.0|true|2020-01-01|2020-01-01T00:00:00.000000|null|"x"
"a"|null|3|30|3|3|null|3.0|null|null|2020-01-03T00:00:00.000000|null|null
"b"|2|2|null|2|2|2.5|nan|false|2020-01-02|null|null|"y"
"b"|5|5|50|5|5|5.0|2.0|true|2020-01-05|2020-01-05T00:00:00.000000|null|"y"
null|4|4|40|4|4|4.0|null|true|2020-01-04|2020-01-04T00:00:00.000000|null|"z"`,
	"Tail": `k:str|i8:i8|i32:i32|i64:i64|u8:u8|u64:u64|f32:f32|f64:f64|b:bool|d:date|dt:datetime[us]|n:null|s:str
"a"|null|3|30|3|3|null|3.0|null|null|2020-01-03T00:00:00.000000|null|null
"a"|6|null|60|6|6|6.0|-1.0|false|2020-01-06|2020-01-06T12:00:00.000000|null|"w"
"b"|2|2|null|2|2|2.5|nan|false|2020-01-02|null|null|"y"
"b"|5|5|50|5|5|5.0|2.0|true|2020-01-05|2020-01-05T00:00:00.000000|null|"y"
null|4|4|40|4|4|4.0|null|true|2020-01-04|2020-01-04T00:00:00.000000|null|"z"`,
	"Head0": `a:i64|b:str|v:f64|w:f64
1|null|null|null
null|null|null|null
2|null|null|null`,
	"MultiLen": `a:i64|b:str|len:u32
1|"x"|2
1|"y"|1
null|"x"|2
2|null|1`,
	"MultiSum": `a:i64|b:str|v:f64|w:f64
1|"x"|6.0|nan
1|"y"|2.0|nan
null|"x"|7.0|nan
2|null|6.0|3.0`,
	"MultiMin": `a:i64|b:str|v:f64|w:f64
1|"x"|1.0|nan
1|"y"|2.0|nan
null|"x"|3.0|1.0
2|null|6.0|3.0`,
	"MultiMax": `a:i64|b:str|v:f64|w:f64
1|"x"|5.0|nan
1|"y"|2.0|nan
null|"x"|4.0|1.0
2|null|6.0|3.0`,
	"MultiMedian": `a:i64|b:str|v:f64|w:f64
1|"x"|3.0|nan
1|"y"|2.0|nan
null|"x"|3.5|nan
2|null|6.0|3.0`,
	"MultiNUnique": `b:str|a:u32|v:u32|w:u32
"x"|2|4|3
"y"|1|1|1
null|1|1|1`,
	"MultiTail": `a:i64|b:str|v:f64|w:f64
1|"x"|1.0|nan
1|"y"|2.0|nan
1|"x"|5.0|null
null|"x"|3.0|1.0
null|"x"|4.0|nan
2|null|6.0|3.0`,
	"MapGroups": `v:f64
8.0
7.0
6.0`,
	"AggMaintainSingle": `b:str|v:f64|wm:f64
"x"|13.0|5.0
"y"|2.0|2.0
null|6.0|6.0`,
	"AggMaintainMulti": `b:str|a:i64|v:f64
"x"|1|6.0
"y"|1|2.0
"x"|null|7.0
null|2|6.0`,
	"FloatKey": `k:f64|v:i64
nan|5
0.0|5
null|5
2.5|6`,
	"DateKey": `k:date|u:i64
2020-01-02|4
2020-01-01|2
null|4`,
	"BoolKey": `k:bool|v:f64
true|2.5
null|2.0
false|3.0`,
	"EmptyLen":     `k:i64|len:u32`,
	"EmptyMean":    `k:i64|v:f64|s:str`,
	"EmptyNUnique": `k:i64|v:u32|s:u32`,
	"EmptyAll":     `k:i64|v:list[f64]|s:list[str]`,
	"EmptyHead":    `k:i64|v:f64|s:str`,
	"UnorderedSum": `a:i64|b:str|v:f64|w:f64
null|"x"|7.0|nan
2|null|6.0|3.0
1|"x"|6.0|nan
1|"y"|2.0|nan`,
	"UnorderedLen": `k:str|len:u32
"b"|2
"a"|3
null|1`,
}
