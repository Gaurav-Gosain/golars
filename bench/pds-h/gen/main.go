// Command gen writes the eight TPC-H tables as parquet files for local
// PDS-H runs.
//
// It follows the dbgen specification closely enough for the queries to
// return meaningful answers: key ranges and sparse order keys, foreign
// keys that resolve (every lineitem part/supplier pair exists in
// partsupp, a third of customers place no orders), the date
// arithmetic between order, ship, commit and receipt dates, the
// returnflag and linestatus rules, the retail price formula and the
// fixed value lists for segments, priorities, ship modes, brands,
// types and containers. Free text (comments, addresses, part names) is
// drawn from small word lists rather than dbgen's grammar, so string
// lengths are similar but the text differs. The output is
// deterministic for a given -sf and -seed.
//
// Types match the parquet files polars-benchmark generates: Int64 keys
// and counts, Float64 for decimals, Date for dates, String for text.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/io/parquet"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	var (
		sf   = flag.Float64("sf", 0.1, "scale factor (1 = 6M lineitem rows)")
		out  = flag.String("out", "/tmp/pdsh", "output directory")
		seed = flag.Uint64("seed", 1, "PCG seed")
	)
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail("mkdir: %v", err)
	}
	g := newGen(*sf, *seed)
	start := time.Now()
	write(*out, "region", g.region())
	write(*out, "nation", g.nation())
	write(*out, "supplier", g.supplier())
	write(*out, "customer", g.customer())
	write(*out, "part", g.part())
	write(*out, "partsupp", g.partsupp())
	orders, lineitem := g.ordersAndLineitem()
	write(*out, "orders", orders)
	write(*out, "lineitem", lineitem)
	fmt.Printf("sf=%g done in %s\n", *sf, time.Since(start).Truncate(time.Millisecond))
}

type gen struct {
	r         *rand.Rand
	nSupp     int64
	nCust     int64
	nPart     int64
	nOrders   int64
	retail    []float64 // indexed by partkey
	startDays int32
	endDays   int32
	current   int32
}

func newGen(sf float64, seed uint64) *gen {
	scale := func(base float64) int64 { return max(int64(base*sf), 1) }
	g := &gen{
		r:         rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15)),
		nSupp:     scale(10_000),
		nCust:     scale(150_000),
		nPart:     scale(200_000),
		nOrders:   scale(1_500_000),
		startDays: days(1992, 1, 1),
		endDays:   days(1998, 12, 31),
		current:   days(1995, 6, 17),
	}
	g.retail = make([]float64, g.nPart+1)
	for k := int64(1); k <= g.nPart; k++ {
		g.retail[k] = float64(90000+((k/10)%20001)+100*(k%1000)) / 100
	}
	return g
}

func days(y int, m time.Month, d int) int32 {
	return int32(time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Unix() / 86400)
}

func (g *gen) intn(lo, hi int64) int64 { return lo + g.r.Int64N(hi-lo+1) }

// money returns a value in [lo, hi] with two decimals.
func (g *gen) money(lo, hi float64) float64 {
	c := g.intn(int64(lo*100), int64(hi*100))
	return float64(c) / 100
}

func (g *gen) pick(xs []string) string { return xs[g.r.IntN(len(xs))] }

var textWords = strings.Fields(`furiously sly carefully blithely quickly fluffily slyly
	ironic final regular express special pending bold even silent unusual
	deposits requests packages accounts instructions theodolites foxes
	pinto beans ideas dependencies excuses platelets asymptotes courts
	dolphins multipliers sauternes warthogs frets dinos attainments
	somas Tiresias patterns forges braids hockey players frays warhorses
	dugouts notornis epitaphs pearls tithes waters orbits gifts sheaves
	depths sentiments decoys realms pains grouches escapades haggle nag
	sleep wake are cajole detect integrate engage promise boost affix
	about above according across after against along alongside among
	Customer Complaints Recommends`)

// text returns space-separated words with a total length in [lo, hi].
func (g *gen) text(lo, hi int) string {
	target := lo + g.r.IntN(hi-lo+1)
	var b strings.Builder
	for b.Len() < target {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(g.pick(textWords))
	}
	s := b.String()
	if len(s) > hi {
		s = strings.TrimSpace(s[:hi])
	}
	return s
}

const alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,"

func (g *gen) address() string {
	n := 10 + g.r.IntN(31)
	b := make([]byte, n)
	for i := range b {
		b[i] = alnum[g.r.IntN(len(alnum))]
	}
	return string(b)
}

func (g *gen) phone(nation int64) string {
	return fmt.Sprintf("%02d-%03d-%03d-%04d", nation+10, g.intn(100, 999), g.intn(100, 999), g.intn(1000, 9999))
}

var regions = []string{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}

var nations = []struct {
	name   string
	region int64
}{
	{"ALGERIA", 0}, {"ARGENTINA", 1}, {"BRAZIL", 1}, {"CANADA", 1}, {"EGYPT", 4},
	{"ETHIOPIA", 0}, {"FRANCE", 3}, {"GERMANY", 3}, {"INDIA", 2}, {"INDONESIA", 2},
	{"IRAN", 4}, {"IRAQ", 4}, {"JAPAN", 2}, {"JORDAN", 4}, {"KENYA", 0},
	{"MOROCCO", 0}, {"MOZAMBIQUE", 0}, {"PERU", 1}, {"CHINA", 2}, {"ROMANIA", 3},
	{"SAUDI ARABIA", 4}, {"VIETNAM", 2}, {"RUSSIA", 3}, {"UNITED KINGDOM", 3}, {"UNITED STATES", 1},
}

func (g *gen) region() *dataframe.DataFrame {
	keys := make([]int64, len(regions))
	comments := make([]string, len(regions))
	for i := range regions {
		keys[i] = int64(i)
		comments[i] = g.text(31, 115)
	}
	return frame(i64("r_regionkey", keys), str("r_name", regions), str("r_comment", comments))
}

func (g *gen) nation() *dataframe.DataFrame {
	n := len(nations)
	keys, regs := make([]int64, n), make([]int64, n)
	names, comments := make([]string, n), make([]string, n)
	for i, nt := range nations {
		keys[i], names[i], regs[i] = int64(i), nt.name, nt.region
		comments[i] = g.text(31, 114)
	}
	return frame(i64("n_nationkey", keys), str("n_name", names), i64("n_regionkey", regs), str("n_comment", comments))
}

func (g *gen) supplier() *dataframe.DataFrame {
	n := int(g.nSupp)
	keys, nat := make([]int64, n), make([]int64, n)
	names, addr, phone, comment := make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	bal := make([]float64, n)
	for i := range n {
		k := int64(i + 1)
		keys[i] = k
		names[i] = fmt.Sprintf("Supplier#%09d", k)
		addr[i] = g.address()
		nat[i] = g.intn(0, 24)
		phone[i] = g.phone(nat[i])
		bal[i] = g.money(-999.99, 9999.99)
		comment[i] = g.text(25, 100)
	}
	return frame(i64("s_suppkey", keys), str("s_name", names), str("s_address", addr),
		i64("s_nationkey", nat), str("s_phone", phone), f64("s_acctbal", bal), str("s_comment", comment))
}

var segments = []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"}

func (g *gen) customer() *dataframe.DataFrame {
	n := int(g.nCust)
	keys, nat := make([]int64, n), make([]int64, n)
	names, addr, phone, seg, comment := make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	bal := make([]float64, n)
	for i := range n {
		k := int64(i + 1)
		keys[i] = k
		names[i] = fmt.Sprintf("Customer#%09d", k)
		addr[i] = g.address()
		nat[i] = g.intn(0, 24)
		phone[i] = g.phone(nat[i])
		bal[i] = g.money(-999.99, 9999.99)
		seg[i] = g.pick(segments)
		comment[i] = g.text(29, 116)
	}
	return frame(i64("c_custkey", keys), str("c_name", names), str("c_address", addr),
		i64("c_nationkey", nat), str("c_phone", phone), f64("c_acctbal", bal),
		str("c_mktsegment", seg), str("c_comment", comment))
}

var colors = strings.Fields(`almond antique aquamarine azure beige bisque black blanched
	blue blush brown burlywood burnished chartreuse chiffon chocolate coral
	cornflower cornsilk cream cyan dark deep dim dodger drab firebrick floral
	forest frosted gainsboro ghost goldenrod green grey honeydew hot indian
	ivory khaki lace lavender lawn lemon light lime linen magenta maroon
	medium metallic midnight mint misty moccasin navajo navy olive orange
	orchid pale papaya peach peru pink plum powder puff purple red rose rosy
	royal saddle salmon sandy seashell sienna sky slate smoke snow spring
	steel tan thistle tomato turquoise violet wheat white yellow`)

var (
	typeSyl1 = []string{"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"}
	typeSyl2 = []string{"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"}
	typeSyl3 = []string{"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"}
	contSyl1 = []string{"SM", "LG", "MED", "JUMBO", "WRAP"}
	contSyl2 = []string{"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"}
)

func (g *gen) part() *dataframe.DataFrame {
	n := int(g.nPart)
	keys, size := make([]int64, n), make([]int64, n)
	name, mfgr, brand, typ, cont, comment := make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	price := make([]float64, n)
	var words [5]string
	for i := range n {
		k := int64(i + 1)
		keys[i] = k
		// Five distinct colors.
		for j := 0; j < 5; {
			c := g.pick(colors)
			dup := false
			for _, w := range words[:j] {
				dup = dup || w == c
			}
			if !dup {
				words[j] = c
				j++
			}
		}
		name[i] = strings.Join(words[:], " ")
		m := g.intn(1, 5)
		mfgr[i] = fmt.Sprintf("Manufacturer#%d", m)
		brand[i] = fmt.Sprintf("Brand#%d%d", m, g.intn(1, 5))
		typ[i] = g.pick(typeSyl1) + " " + g.pick(typeSyl2) + " " + g.pick(typeSyl3)
		size[i] = g.intn(1, 50)
		cont[i] = g.pick(contSyl1) + " " + g.pick(contSyl2)
		price[i] = g.retail[k]
		comment[i] = g.text(5, 22)
	}
	return frame(i64("p_partkey", keys), str("p_name", name), str("p_mfgr", mfgr), str("p_brand", brand),
		str("p_type", typ), i64("p_size", size), str("p_container", cont), f64("p_retailprice", price),
		str("p_comment", comment))
}

// partSupp returns the i-th (0..3) supplier of a part, as dbgen does.
func (g *gen) partSupp(partkey, i int64) int64 {
	s := g.nSupp
	return (partkey+i*(s/4+(partkey-1)/s))%s + 1
}

func (g *gen) partsupp() *dataframe.DataFrame {
	n := int(g.nPart * 4)
	pk, sk, qty := make([]int64, n), make([]int64, n), make([]int64, n)
	cost := make([]float64, n)
	comment := make([]string, n)
	for i := range n {
		p := int64(i/4) + 1
		pk[i] = p
		sk[i] = g.partSupp(p, int64(i%4))
		qty[i] = g.intn(1, 9999)
		cost[i] = g.money(1, 1000)
		comment[i] = g.text(49, 198)
	}
	return frame(i64("ps_partkey", pk), i64("ps_suppkey", sk), i64("ps_availqty", qty),
		f64("ps_supplycost", cost), str("ps_comment", comment))
}

var (
	priorities    = []string{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}
	shipInstructs = []string{"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"}
	shipModes     = []string{"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"}
)

func (g *gen) ordersAndLineitem() (*dataframe.DataFrame, *dataframe.DataFrame) {
	no := int(g.nOrders)
	oKey, oCust, oShipPri := make([]int64, no), make([]int64, no), make([]int64, no)
	oStatus, oPri, oClerk, oComment := make([]string, no), make([]string, no), make([]string, no), make([]string, no)
	oPrice := make([]float64, no)
	oDate := make([]int32, no)

	est := no * 4
	var (
		lOrder, lPart, lSupp, lLine          = make([]int64, 0, est), make([]int64, 0, est), make([]int64, 0, est), make([]int64, 0, est)
		lQty, lPrice, lDisc, lTax            = make([]float64, 0, est), make([]float64, 0, est), make([]float64, 0, est), make([]float64, 0, est)
		lRet, lStat, lInstr, lMode, lComment = make([]string, 0, est), make([]string, 0, est), make([]string, 0, est), make([]string, 0, est), make([]string, 0, est)
		lShip, lCommit, lReceipt             = make([]int32, 0, est), make([]int32, 0, est), make([]int32, 0, est)
	)
	nClerks := max(g.nOrders/1500, 1)
	for i := range no {
		// dbgen uses the first 8 of every 32 keys.
		key := int64(i/8)*32 + int64(i%8) + 1
		oKey[i] = key
		// Customers whose key is a multiple of 3 place no orders.
		c := g.intn(1, g.nCust)
		for c%3 == 0 && g.nCust > 2 {
			c = g.intn(1, g.nCust)
		}
		oCust[i] = c
		od := g.startDays + int32(g.r.IntN(int(g.endDays-151-g.startDays+1)))
		oDate[i] = od
		oPri[i] = g.pick(priorities)
		oClerk[i] = fmt.Sprintf("Clerk#%09d", g.intn(1, nClerks))
		oComment[i] = g.text(19, 78)

		lines := 1 + g.r.IntN(7)
		total := 0.0
		nF, nO := 0, 0
		for ln := 1; ln <= lines; ln++ {
			p := g.intn(1, g.nPart)
			q := float64(g.intn(1, 50))
			disc := float64(g.intn(0, 10)) / 100
			tax := float64(g.intn(0, 8)) / 100
			price := q * g.retail[p]
			ship := od + int32(g.intn(1, 121))
			commit := od + int32(g.intn(30, 90))
			receipt := ship + int32(g.intn(1, 30))
			ret := "N"
			if receipt <= g.current {
				ret = []string{"R", "A"}[g.r.IntN(2)]
			}
			stat := "F"
			if ship > g.current {
				stat = "O"
				nO++
			} else {
				nF++
			}
			total += price * (1 + tax) * (1 - disc)

			lOrder = append(lOrder, key)
			lPart = append(lPart, p)
			lSupp = append(lSupp, g.partSupp(p, g.intn(0, 3)))
			lLine = append(lLine, int64(ln))
			lQty = append(lQty, q)
			lPrice = append(lPrice, price)
			lDisc = append(lDisc, disc)
			lTax = append(lTax, tax)
			lRet = append(lRet, ret)
			lStat = append(lStat, stat)
			lShip = append(lShip, ship)
			lCommit = append(lCommit, commit)
			lReceipt = append(lReceipt, receipt)
			lInstr = append(lInstr, g.pick(shipInstructs))
			lMode = append(lMode, g.pick(shipModes))
			lComment = append(lComment, g.text(10, 43))
		}
		switch {
		case nO == 0:
			oStatus[i] = "F"
		case nF == 0:
			oStatus[i] = "O"
		default:
			oStatus[i] = "P"
		}
		oPrice[i] = float64(int64(total*100+0.5)) / 100
	}

	orders := frame(i64("o_orderkey", oKey), i64("o_custkey", oCust), str("o_orderstatus", oStatus),
		f64("o_totalprice", oPrice), date("o_orderdate", oDate), str("o_orderpriority", oPri),
		str("o_clerk", oClerk), i64("o_shippriority", oShipPri), str("o_comment", oComment))
	lineitem := frame(i64("l_orderkey", lOrder), i64("l_partkey", lPart), i64("l_suppkey", lSupp),
		i64("l_linenumber", lLine), f64("l_quantity", lQty), f64("l_extendedprice", lPrice),
		f64("l_discount", lDisc), f64("l_tax", lTax), str("l_returnflag", lRet), str("l_linestatus", lStat),
		date("l_shipdate", lShip), date("l_commitdate", lCommit), date("l_receiptdate", lReceipt),
		str("l_shipinstruct", lInstr), str("l_shipmode", lMode), str("l_comment", lComment))
	return orders, lineitem
}

func i64(name string, v []int64) *series.Series { return must(series.FromInt64(name, v, nil)) }
func f64(name string, v []float64) *series.Series {
	return must(series.FromFloat64(name, v, nil))
}
func str(name string, v []string) *series.Series { return must(series.FromString(name, v, nil)) }
func date(name string, v []int32) *series.Series { return must(series.FromDate(name, v, nil)) }

func must(s *series.Series, err error) *series.Series {
	if err != nil {
		fail("build column: %v", err)
	}
	return s
}

func frame(cols ...*series.Series) *dataframe.DataFrame {
	df, err := dataframe.New(cols...)
	if err != nil {
		fail("dataframe: %v", err)
	}
	return df
}

func write(dir, table string, df *dataframe.DataFrame) {
	defer df.Release()
	path := filepath.Join(dir, table+".parquet")
	if err := parquet.WriteFile(context.Background(), path, df); err != nil {
		fail("write %s: %v", table, err)
	}
	fmt.Printf("wrote %s (%d rows x %d cols)\n", path, df.Height(), df.Width())
}

func fail(f string, args ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", args...)
	os.Exit(1)
}
