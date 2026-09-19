#include "tree_sitter/parser.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif

#ifdef _MSC_VER
#pragma optimize("", off)
#elif defined(__clang__)
#pragma clang optimize off
#elif defined(__GNUC__)
#pragma GCC optimize ("O0")
#endif

#define LANGUAGE_VERSION 14
#define STATE_COUNT 88
#define LARGE_STATE_COUNT 9
#define SYMBOL_COUNT 158
#define ALIAS_COUNT 0
#define TOKEN_COUNT 141
#define EXTERNAL_TOKEN_COUNT 0
#define FIELD_COUNT 4
#define MAX_ALIAS_SEQUENCE_LENGTH 6
#define PRODUCTION_ID_COUNT 9

enum ts_symbol_identifiers {
  anon_sym_DOT = 1,
  anon_sym_load = 2,
  anon_sym_save = 3,
  anon_sym_use = 4,
  anon_sym_stash = 5,
  anon_sym_frames = 6,
  anon_sym_drop_frame = 7,
  anon_sym_with = 8,
  anon_sym_unnest = 9,
  anon_sym_explode = 10,
  anon_sym_upsample = 11,
  anon_sym_tree = 12,
  anon_sym_graph = 13,
  anon_sym_show_graph = 14,
  anon_sym_mermaid = 15,
  anon_sym_explain_tree = 16,
  anon_sym_select = 17,
  anon_sym_drop = 18,
  anon_sym_filter = 19,
  anon_sym_sort = 20,
  anon_sym_limit = 21,
  anon_sym_head = 22,
  anon_sym_tail = 23,
  anon_sym_show = 24,
  anon_sym_schema = 25,
  anon_sym_describe = 26,
  anon_sym_groupby = 27,
  anon_sym_join = 28,
  anon_sym_explain = 29,
  anon_sym_collect = 30,
  anon_sym_reset = 31,
  anon_sym_source = 32,
  anon_sym_timing = 33,
  anon_sym_info = 34,
  anon_sym_clear = 35,
  anon_sym_exit = 36,
  anon_sym_quit = 37,
  anon_sym_reverse = 38,
  anon_sym_sample = 39,
  anon_sym_shuffle = 40,
  anon_sym_unique = 41,
  anon_sym_null_count = 42,
  anon_sym_glimpse = 43,
  anon_sym_size = 44,
  anon_sym_cast = 45,
  anon_sym_fill_null = 46,
  anon_sym_drop_null = 47,
  anon_sym_rename = 48,
  anon_sym_sum = 49,
  anon_sym_mean = 50,
  anon_sym_avg = 51,
  anon_sym_min = 52,
  anon_sym_max = 53,
  anon_sym_median = 54,
  anon_sym_std = 55,
  anon_sym_write = 56,
  anon_sym_with_row_index = 57,
  anon_sym_pwd = 58,
  anon_sym_ls = 59,
  anon_sym_cd = 60,
  anon_sym_sum_horizontal = 61,
  anon_sym_mean_horizontal = 62,
  anon_sym_min_horizontal = 63,
  anon_sym_max_horizontal = 64,
  anon_sym_all_horizontal = 65,
  anon_sym_any_horizontal = 66,
  anon_sym_sum_all = 67,
  anon_sym_mean_all = 68,
  anon_sym_min_all = 69,
  anon_sym_max_all = 70,
  anon_sym_std_all = 71,
  anon_sym_var_all = 72,
  anon_sym_median_all = 73,
  anon_sym_count_all = 74,
  anon_sym_null_count_all = 75,
  anon_sym_scan_csv = 76,
  anon_sym_scan_parquet = 77,
  anon_sym_scan_ipc = 78,
  anon_sym_scan_arrow = 79,
  anon_sym_scan_json = 80,
  anon_sym_scan_ndjson = 81,
  anon_sym_scan_jsonl = 82,
  anon_sym_scan_auto = 83,
  anon_sym_fill_nan = 84,
  anon_sym_forward_fill = 85,
  anon_sym_backward_fill = 86,
  anon_sym_top_k = 87,
  anon_sym_bottom_k = 88,
  anon_sym_transpose = 89,
  anon_sym_unpivot = 90,
  anon_sym_melt = 91,
  anon_sym_partition_by = 92,
  anon_sym_skew = 93,
  anon_sym_kurtosis = 94,
  anon_sym_approx_n_unique = 95,
  anon_sym_corr = 96,
  anon_sym_cov = 97,
  anon_sym_pivot = 98,
  anon_sym_help = 99,
  anon_sym_EQ = 100,
  anon_sym_LPAREN = 101,
  anon_sym_RPAREN = 102,
  anon_sym_COMMA = 103,
  anon_sym_SLASH = 104,
  anon_sym_PERCENT = 105,
  anon_sym_PLUS = 106,
  anon_sym_STAR = 107,
  anon_sym_BANG = 108,
  anon_sym_AMP = 109,
  anon_sym_PIPE = 110,
  anon_sym_DASH = 111,
  anon_sym_as = 112,
  anon_sym_on = 113,
  anon_sym_asc = 114,
  anon_sym_desc = 115,
  anon_sym_and = 116,
  anon_sym_or = 117,
  anon_sym_is_null = 118,
  anon_sym_is_not_null = 119,
  anon_sym_inner = 120,
  anon_sym_left = 121,
  anon_sym_cross = 122,
  anon_sym_EQ_EQ = 123,
  anon_sym_BANG_EQ = 124,
  anon_sym_LT_EQ = 125,
  anon_sym_GT_EQ = 126,
  anon_sym_LT = 127,
  anon_sym_GT = 128,
  sym_agg_spec = 129,
  anon_sym_DQUOTE = 130,
  aux_sym_string_token1 = 131,
  anon_sym_BSLASH = 132,
  aux_sym_string_token2 = 133,
  sym_number = 134,
  anon_sym_true = 135,
  anon_sym_false = 136,
  sym_identifier = 137,
  sym_comment = 138,
  sym_line_continuation = 139,
  sym__newline = 140,
  sym_source_file = 141,
  sym_statement = 142,
  sym_command = 143,
  sym__arg = 144,
  sym_expr = 145,
  sym__expr_operand = 146,
  sym_method_call = 147,
  sym_keyword = 148,
  sym_operator = 149,
  sym_string = 150,
  sym_boolean = 151,
  aux_sym_source_file_repeat1 = 152,
  aux_sym_statement_repeat1 = 153,
  aux_sym_expr_repeat1 = 154,
  aux_sym_method_call_repeat1 = 155,
  aux_sym_method_call_repeat2 = 156,
  aux_sym_string_repeat1 = 157,
};

static const char * const ts_symbol_names[] = {
  [ts_builtin_sym_end] = "end",
  [anon_sym_DOT] = ".",
  [anon_sym_load] = "load",
  [anon_sym_save] = "save",
  [anon_sym_use] = "use",
  [anon_sym_stash] = "stash",
  [anon_sym_frames] = "frames",
  [anon_sym_drop_frame] = "drop_frame",
  [anon_sym_with] = "with",
  [anon_sym_unnest] = "unnest",
  [anon_sym_explode] = "explode",
  [anon_sym_upsample] = "upsample",
  [anon_sym_tree] = "tree",
  [anon_sym_graph] = "graph",
  [anon_sym_show_graph] = "show_graph",
  [anon_sym_mermaid] = "mermaid",
  [anon_sym_explain_tree] = "explain_tree",
  [anon_sym_select] = "select",
  [anon_sym_drop] = "drop",
  [anon_sym_filter] = "filter",
  [anon_sym_sort] = "sort",
  [anon_sym_limit] = "limit",
  [anon_sym_head] = "head",
  [anon_sym_tail] = "tail",
  [anon_sym_show] = "show",
  [anon_sym_schema] = "schema",
  [anon_sym_describe] = "describe",
  [anon_sym_groupby] = "groupby",
  [anon_sym_join] = "join",
  [anon_sym_explain] = "explain",
  [anon_sym_collect] = "collect",
  [anon_sym_reset] = "reset",
  [anon_sym_source] = "source",
  [anon_sym_timing] = "timing",
  [anon_sym_info] = "info",
  [anon_sym_clear] = "clear",
  [anon_sym_exit] = "exit",
  [anon_sym_quit] = "quit",
  [anon_sym_reverse] = "reverse",
  [anon_sym_sample] = "sample",
  [anon_sym_shuffle] = "shuffle",
  [anon_sym_unique] = "unique",
  [anon_sym_null_count] = "null_count",
  [anon_sym_glimpse] = "glimpse",
  [anon_sym_size] = "size",
  [anon_sym_cast] = "cast",
  [anon_sym_fill_null] = "fill_null",
  [anon_sym_drop_null] = "drop_null",
  [anon_sym_rename] = "rename",
  [anon_sym_sum] = "sum",
  [anon_sym_mean] = "mean",
  [anon_sym_avg] = "avg",
  [anon_sym_min] = "min",
  [anon_sym_max] = "max",
  [anon_sym_median] = "median",
  [anon_sym_std] = "std",
  [anon_sym_write] = "write",
  [anon_sym_with_row_index] = "with_row_index",
  [anon_sym_pwd] = "pwd",
  [anon_sym_ls] = "ls",
  [anon_sym_cd] = "cd",
  [anon_sym_sum_horizontal] = "sum_horizontal",
  [anon_sym_mean_horizontal] = "mean_horizontal",
  [anon_sym_min_horizontal] = "min_horizontal",
  [anon_sym_max_horizontal] = "max_horizontal",
  [anon_sym_all_horizontal] = "all_horizontal",
  [anon_sym_any_horizontal] = "any_horizontal",
  [anon_sym_sum_all] = "sum_all",
  [anon_sym_mean_all] = "mean_all",
  [anon_sym_min_all] = "min_all",
  [anon_sym_max_all] = "max_all",
  [anon_sym_std_all] = "std_all",
  [anon_sym_var_all] = "var_all",
  [anon_sym_median_all] = "median_all",
  [anon_sym_count_all] = "count_all",
  [anon_sym_null_count_all] = "null_count_all",
  [anon_sym_scan_csv] = "scan_csv",
  [anon_sym_scan_parquet] = "scan_parquet",
  [anon_sym_scan_ipc] = "scan_ipc",
  [anon_sym_scan_arrow] = "scan_arrow",
  [anon_sym_scan_json] = "scan_json",
  [anon_sym_scan_ndjson] = "scan_ndjson",
  [anon_sym_scan_jsonl] = "scan_jsonl",
  [anon_sym_scan_auto] = "scan_auto",
  [anon_sym_fill_nan] = "fill_nan",
  [anon_sym_forward_fill] = "forward_fill",
  [anon_sym_backward_fill] = "backward_fill",
  [anon_sym_top_k] = "top_k",
  [anon_sym_bottom_k] = "bottom_k",
  [anon_sym_transpose] = "transpose",
  [anon_sym_unpivot] = "unpivot",
  [anon_sym_melt] = "melt",
  [anon_sym_partition_by] = "partition_by",
  [anon_sym_skew] = "skew",
  [anon_sym_kurtosis] = "kurtosis",
  [anon_sym_approx_n_unique] = "approx_n_unique",
  [anon_sym_corr] = "corr",
  [anon_sym_cov] = "cov",
  [anon_sym_pivot] = "pivot",
  [anon_sym_help] = "help",
  [anon_sym_EQ] = "=",
  [anon_sym_LPAREN] = "(",
  [anon_sym_RPAREN] = ")",
  [anon_sym_COMMA] = ",",
  [anon_sym_SLASH] = "/",
  [anon_sym_PERCENT] = "%",
  [anon_sym_PLUS] = "+",
  [anon_sym_STAR] = "*",
  [anon_sym_BANG] = "!",
  [anon_sym_AMP] = "&",
  [anon_sym_PIPE] = "|",
  [anon_sym_DASH] = "-",
  [anon_sym_as] = "as",
  [anon_sym_on] = "on",
  [anon_sym_asc] = "asc",
  [anon_sym_desc] = "desc",
  [anon_sym_and] = "and",
  [anon_sym_or] = "or",
  [anon_sym_is_null] = "is_null",
  [anon_sym_is_not_null] = "is_not_null",
  [anon_sym_inner] = "inner",
  [anon_sym_left] = "left",
  [anon_sym_cross] = "cross",
  [anon_sym_EQ_EQ] = "==",
  [anon_sym_BANG_EQ] = "!=",
  [anon_sym_LT_EQ] = "<=",
  [anon_sym_GT_EQ] = ">=",
  [anon_sym_LT] = "<",
  [anon_sym_GT] = ">",
  [sym_agg_spec] = "agg_spec",
  [anon_sym_DQUOTE] = "\"",
  [aux_sym_string_token1] = "string_token1",
  [anon_sym_BSLASH] = "\\",
  [aux_sym_string_token2] = "string_token2",
  [sym_number] = "number",
  [anon_sym_true] = "true",
  [anon_sym_false] = "false",
  [sym_identifier] = "identifier",
  [sym_comment] = "comment",
  [sym_line_continuation] = "line_continuation",
  [sym__newline] = "_newline",
  [sym_source_file] = "source_file",
  [sym_statement] = "statement",
  [sym_command] = "command",
  [sym__arg] = "_arg",
  [sym_expr] = "expr",
  [sym__expr_operand] = "_expr_operand",
  [sym_method_call] = "method_call",
  [sym_keyword] = "keyword",
  [sym_operator] = "operator",
  [sym_string] = "string",
  [sym_boolean] = "boolean",
  [aux_sym_source_file_repeat1] = "source_file_repeat1",
  [aux_sym_statement_repeat1] = "statement_repeat1",
  [aux_sym_expr_repeat1] = "expr_repeat1",
  [aux_sym_method_call_repeat1] = "method_call_repeat1",
  [aux_sym_method_call_repeat2] = "method_call_repeat2",
  [aux_sym_string_repeat1] = "string_repeat1",
};

static const TSSymbol ts_symbol_map[] = {
  [ts_builtin_sym_end] = ts_builtin_sym_end,
  [anon_sym_DOT] = anon_sym_DOT,
  [anon_sym_load] = anon_sym_load,
  [anon_sym_save] = anon_sym_save,
  [anon_sym_use] = anon_sym_use,
  [anon_sym_stash] = anon_sym_stash,
  [anon_sym_frames] = anon_sym_frames,
  [anon_sym_drop_frame] = anon_sym_drop_frame,
  [anon_sym_with] = anon_sym_with,
  [anon_sym_unnest] = anon_sym_unnest,
  [anon_sym_explode] = anon_sym_explode,
  [anon_sym_upsample] = anon_sym_upsample,
  [anon_sym_tree] = anon_sym_tree,
  [anon_sym_graph] = anon_sym_graph,
  [anon_sym_show_graph] = anon_sym_show_graph,
  [anon_sym_mermaid] = anon_sym_mermaid,
  [anon_sym_explain_tree] = anon_sym_explain_tree,
  [anon_sym_select] = anon_sym_select,
  [anon_sym_drop] = anon_sym_drop,
  [anon_sym_filter] = anon_sym_filter,
  [anon_sym_sort] = anon_sym_sort,
  [anon_sym_limit] = anon_sym_limit,
  [anon_sym_head] = anon_sym_head,
  [anon_sym_tail] = anon_sym_tail,
  [anon_sym_show] = anon_sym_show,
  [anon_sym_schema] = anon_sym_schema,
  [anon_sym_describe] = anon_sym_describe,
  [anon_sym_groupby] = anon_sym_groupby,
  [anon_sym_join] = anon_sym_join,
  [anon_sym_explain] = anon_sym_explain,
  [anon_sym_collect] = anon_sym_collect,
  [anon_sym_reset] = anon_sym_reset,
  [anon_sym_source] = anon_sym_source,
  [anon_sym_timing] = anon_sym_timing,
  [anon_sym_info] = anon_sym_info,
  [anon_sym_clear] = anon_sym_clear,
  [anon_sym_exit] = anon_sym_exit,
  [anon_sym_quit] = anon_sym_quit,
  [anon_sym_reverse] = anon_sym_reverse,
  [anon_sym_sample] = anon_sym_sample,
  [anon_sym_shuffle] = anon_sym_shuffle,
  [anon_sym_unique] = anon_sym_unique,
  [anon_sym_null_count] = anon_sym_null_count,
  [anon_sym_glimpse] = anon_sym_glimpse,
  [anon_sym_size] = anon_sym_size,
  [anon_sym_cast] = anon_sym_cast,
  [anon_sym_fill_null] = anon_sym_fill_null,
  [anon_sym_drop_null] = anon_sym_drop_null,
  [anon_sym_rename] = anon_sym_rename,
  [anon_sym_sum] = anon_sym_sum,
  [anon_sym_mean] = anon_sym_mean,
  [anon_sym_avg] = anon_sym_avg,
  [anon_sym_min] = anon_sym_min,
  [anon_sym_max] = anon_sym_max,
  [anon_sym_median] = anon_sym_median,
  [anon_sym_std] = anon_sym_std,
  [anon_sym_write] = anon_sym_write,
  [anon_sym_with_row_index] = anon_sym_with_row_index,
  [anon_sym_pwd] = anon_sym_pwd,
  [anon_sym_ls] = anon_sym_ls,
  [anon_sym_cd] = anon_sym_cd,
  [anon_sym_sum_horizontal] = anon_sym_sum_horizontal,
  [anon_sym_mean_horizontal] = anon_sym_mean_horizontal,
  [anon_sym_min_horizontal] = anon_sym_min_horizontal,
  [anon_sym_max_horizontal] = anon_sym_max_horizontal,
  [anon_sym_all_horizontal] = anon_sym_all_horizontal,
  [anon_sym_any_horizontal] = anon_sym_any_horizontal,
  [anon_sym_sum_all] = anon_sym_sum_all,
  [anon_sym_mean_all] = anon_sym_mean_all,
  [anon_sym_min_all] = anon_sym_min_all,
  [anon_sym_max_all] = anon_sym_max_all,
  [anon_sym_std_all] = anon_sym_std_all,
  [anon_sym_var_all] = anon_sym_var_all,
  [anon_sym_median_all] = anon_sym_median_all,
  [anon_sym_count_all] = anon_sym_count_all,
  [anon_sym_null_count_all] = anon_sym_null_count_all,
  [anon_sym_scan_csv] = anon_sym_scan_csv,
  [anon_sym_scan_parquet] = anon_sym_scan_parquet,
  [anon_sym_scan_ipc] = anon_sym_scan_ipc,
  [anon_sym_scan_arrow] = anon_sym_scan_arrow,
  [anon_sym_scan_json] = anon_sym_scan_json,
  [anon_sym_scan_ndjson] = anon_sym_scan_ndjson,
  [anon_sym_scan_jsonl] = anon_sym_scan_jsonl,
  [anon_sym_scan_auto] = anon_sym_scan_auto,
  [anon_sym_fill_nan] = anon_sym_fill_nan,
  [anon_sym_forward_fill] = anon_sym_forward_fill,
  [anon_sym_backward_fill] = anon_sym_backward_fill,
  [anon_sym_top_k] = anon_sym_top_k,
  [anon_sym_bottom_k] = anon_sym_bottom_k,
  [anon_sym_transpose] = anon_sym_transpose,
  [anon_sym_unpivot] = anon_sym_unpivot,
  [anon_sym_melt] = anon_sym_melt,
  [anon_sym_partition_by] = anon_sym_partition_by,
  [anon_sym_skew] = anon_sym_skew,
  [anon_sym_kurtosis] = anon_sym_kurtosis,
  [anon_sym_approx_n_unique] = anon_sym_approx_n_unique,
  [anon_sym_corr] = anon_sym_corr,
  [anon_sym_cov] = anon_sym_cov,
  [anon_sym_pivot] = anon_sym_pivot,
  [anon_sym_help] = anon_sym_help,
  [anon_sym_EQ] = anon_sym_EQ,
  [anon_sym_LPAREN] = anon_sym_LPAREN,
  [anon_sym_RPAREN] = anon_sym_RPAREN,
  [anon_sym_COMMA] = anon_sym_COMMA,
  [anon_sym_SLASH] = anon_sym_SLASH,
  [anon_sym_PERCENT] = anon_sym_PERCENT,
  [anon_sym_PLUS] = anon_sym_PLUS,
  [anon_sym_STAR] = anon_sym_STAR,
  [anon_sym_BANG] = anon_sym_BANG,
  [anon_sym_AMP] = anon_sym_AMP,
  [anon_sym_PIPE] = anon_sym_PIPE,
  [anon_sym_DASH] = anon_sym_DASH,
  [anon_sym_as] = anon_sym_as,
  [anon_sym_on] = anon_sym_on,
  [anon_sym_asc] = anon_sym_asc,
  [anon_sym_desc] = anon_sym_desc,
  [anon_sym_and] = anon_sym_and,
  [anon_sym_or] = anon_sym_or,
  [anon_sym_is_null] = anon_sym_is_null,
  [anon_sym_is_not_null] = anon_sym_is_not_null,
  [anon_sym_inner] = anon_sym_inner,
  [anon_sym_left] = anon_sym_left,
  [anon_sym_cross] = anon_sym_cross,
  [anon_sym_EQ_EQ] = anon_sym_EQ_EQ,
  [anon_sym_BANG_EQ] = anon_sym_BANG_EQ,
  [anon_sym_LT_EQ] = anon_sym_LT_EQ,
  [anon_sym_GT_EQ] = anon_sym_GT_EQ,
  [anon_sym_LT] = anon_sym_LT,
  [anon_sym_GT] = anon_sym_GT,
  [sym_agg_spec] = sym_agg_spec,
  [anon_sym_DQUOTE] = anon_sym_DQUOTE,
  [aux_sym_string_token1] = aux_sym_string_token1,
  [anon_sym_BSLASH] = anon_sym_BSLASH,
  [aux_sym_string_token2] = aux_sym_string_token2,
  [sym_number] = sym_number,
  [anon_sym_true] = anon_sym_true,
  [anon_sym_false] = anon_sym_false,
  [sym_identifier] = sym_identifier,
  [sym_comment] = sym_comment,
  [sym_line_continuation] = sym_line_continuation,
  [sym__newline] = sym__newline,
  [sym_source_file] = sym_source_file,
  [sym_statement] = sym_statement,
  [sym_command] = sym_command,
  [sym__arg] = sym__arg,
  [sym_expr] = sym_expr,
  [sym__expr_operand] = sym__expr_operand,
  [sym_method_call] = sym_method_call,
  [sym_keyword] = sym_keyword,
  [sym_operator] = sym_operator,
  [sym_string] = sym_string,
  [sym_boolean] = sym_boolean,
  [aux_sym_source_file_repeat1] = aux_sym_source_file_repeat1,
  [aux_sym_statement_repeat1] = aux_sym_statement_repeat1,
  [aux_sym_expr_repeat1] = aux_sym_expr_repeat1,
  [aux_sym_method_call_repeat1] = aux_sym_method_call_repeat1,
  [aux_sym_method_call_repeat2] = aux_sym_method_call_repeat2,
  [aux_sym_string_repeat1] = aux_sym_string_repeat1,
};

static const TSSymbolMetadata ts_symbol_metadata[] = {
  [ts_builtin_sym_end] = {
    .visible = false,
    .named = true,
  },
  [anon_sym_DOT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_load] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_save] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_use] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_stash] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_frames] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_drop_frame] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_with] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_unnest] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_explode] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_upsample] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_tree] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_graph] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_show_graph] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_mermaid] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_explain_tree] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_select] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_drop] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_filter] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_sort] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_limit] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_head] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_tail] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_show] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_schema] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_describe] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_groupby] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_join] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_explain] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_collect] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_reset] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_source] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_timing] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_info] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_clear] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_exit] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_quit] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_reverse] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_sample] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_shuffle] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_unique] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_null_count] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_glimpse] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_size] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_cast] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_fill_null] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_drop_null] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_rename] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_sum] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_mean] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_avg] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_min] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_max] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_median] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_std] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_write] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_with_row_index] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_pwd] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_ls] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_cd] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_sum_horizontal] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_mean_horizontal] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_min_horizontal] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_max_horizontal] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_all_horizontal] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_any_horizontal] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_sum_all] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_mean_all] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_min_all] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_max_all] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_std_all] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_var_all] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_median_all] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_count_all] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_null_count_all] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_scan_csv] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_scan_parquet] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_scan_ipc] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_scan_arrow] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_scan_json] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_scan_ndjson] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_scan_jsonl] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_scan_auto] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_fill_nan] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_forward_fill] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_backward_fill] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_top_k] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_bottom_k] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_transpose] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_unpivot] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_melt] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_partition_by] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_skew] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_kurtosis] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_approx_n_unique] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_corr] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_cov] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_pivot] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_help] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_EQ] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_RPAREN] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_COMMA] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_SLASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PERCENT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PLUS] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_STAR] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_BANG] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_AMP] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_PIPE] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_DASH] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_as] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_on] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_asc] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_desc] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_and] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_or] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_is_null] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_is_not_null] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_inner] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_left] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_cross] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_EQ_EQ] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_BANG_EQ] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LT_EQ] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_GT_EQ] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_LT] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_GT] = {
    .visible = true,
    .named = false,
  },
  [sym_agg_spec] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_DQUOTE] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_string_token1] = {
    .visible = false,
    .named = false,
  },
  [anon_sym_BSLASH] = {
    .visible = true,
    .named = false,
  },
  [aux_sym_string_token2] = {
    .visible = false,
    .named = false,
  },
  [sym_number] = {
    .visible = true,
    .named = true,
  },
  [anon_sym_true] = {
    .visible = true,
    .named = false,
  },
  [anon_sym_false] = {
    .visible = true,
    .named = false,
  },
  [sym_identifier] = {
    .visible = true,
    .named = true,
  },
  [sym_comment] = {
    .visible = true,
    .named = true,
  },
  [sym_line_continuation] = {
    .visible = true,
    .named = true,
  },
  [sym__newline] = {
    .visible = false,
    .named = true,
  },
  [sym_source_file] = {
    .visible = true,
    .named = true,
  },
  [sym_statement] = {
    .visible = true,
    .named = true,
  },
  [sym_command] = {
    .visible = true,
    .named = true,
  },
  [sym__arg] = {
    .visible = false,
    .named = true,
  },
  [sym_expr] = {
    .visible = true,
    .named = true,
  },
  [sym__expr_operand] = {
    .visible = false,
    .named = true,
  },
  [sym_method_call] = {
    .visible = true,
    .named = true,
  },
  [sym_keyword] = {
    .visible = true,
    .named = true,
  },
  [sym_operator] = {
    .visible = true,
    .named = true,
  },
  [sym_string] = {
    .visible = true,
    .named = true,
  },
  [sym_boolean] = {
    .visible = true,
    .named = true,
  },
  [aux_sym_source_file_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_statement_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_expr_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_method_call_repeat1] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_method_call_repeat2] = {
    .visible = false,
    .named = false,
  },
  [aux_sym_string_repeat1] = {
    .visible = false,
    .named = false,
  },
};

enum ts_field_identifiers {
  field_args = 1,
  field_command = 2,
  field_method = 3,
  field_receiver = 4,
};

static const char * const ts_field_names[] = {
  [0] = NULL,
  [field_args] = "args",
  [field_command] = "command",
  [field_method] = "method",
  [field_receiver] = "receiver",
};

static const TSFieldMapSlice ts_field_map_slices[PRODUCTION_ID_COUNT] = {
  [1] = {.index = 0, .length = 1},
  [2] = {.index = 1, .length = 1},
  [3] = {.index = 2, .length = 2},
  [4] = {.index = 4, .length = 2},
  [5] = {.index = 6, .length = 1},
  [6] = {.index = 7, .length = 1},
  [7] = {.index = 8, .length = 2},
  [8] = {.index = 10, .length = 2},
};

static const TSFieldMapEntry ts_field_map_entries[] = {
  [0] =
    {field_command, 0},
  [1] =
    {field_command, 1},
  [2] =
    {field_args, 1},
    {field_command, 0},
  [4] =
    {field_args, 2},
    {field_command, 1},
  [6] =
    {field_method, 1},
  [7] =
    {field_receiver, 0},
  [8] =
    {field_method, 0, .inherited = true},
    {field_method, 1, .inherited = true},
  [10] =
    {field_method, 1, .inherited = true},
    {field_receiver, 0},
};

static const TSSymbol ts_alias_sequences[PRODUCTION_ID_COUNT][MAX_ALIAS_SEQUENCE_LENGTH] = {
  [0] = {0},
};

static const uint16_t ts_non_terminal_alias_map[] = {
  0,
};

static const TSStateId ts_primary_state_ids[STATE_COUNT] = {
  [0] = 0,
  [1] = 1,
  [2] = 2,
  [3] = 3,
  [4] = 4,
  [5] = 5,
  [6] = 6,
  [7] = 7,
  [8] = 8,
  [9] = 9,
  [10] = 10,
  [11] = 11,
  [12] = 12,
  [13] = 13,
  [14] = 14,
  [15] = 15,
  [16] = 16,
  [17] = 17,
  [18] = 18,
  [19] = 19,
  [20] = 20,
  [21] = 21,
  [22] = 22,
  [23] = 23,
  [24] = 24,
  [25] = 25,
  [26] = 26,
  [27] = 27,
  [28] = 28,
  [29] = 29,
  [30] = 30,
  [31] = 31,
  [32] = 32,
  [33] = 33,
  [34] = 34,
  [35] = 35,
  [36] = 36,
  [37] = 37,
  [38] = 36,
  [39] = 37,
  [40] = 40,
  [41] = 41,
  [42] = 40,
  [43] = 43,
  [44] = 44,
  [45] = 43,
  [46] = 44,
  [47] = 19,
  [48] = 16,
  [49] = 18,
  [50] = 17,
  [51] = 25,
  [52] = 27,
  [53] = 28,
  [54] = 22,
  [55] = 23,
  [56] = 24,
  [57] = 20,
  [58] = 32,
  [59] = 35,
  [60] = 34,
  [61] = 33,
  [62] = 30,
  [63] = 63,
  [64] = 64,
  [65] = 65,
  [66] = 64,
  [67] = 65,
  [68] = 68,
  [69] = 69,
  [70] = 70,
  [71] = 71,
  [72] = 72,
  [73] = 73,
  [74] = 74,
  [75] = 70,
  [76] = 76,
  [77] = 76,
  [78] = 71,
  [79] = 68,
  [80] = 72,
  [81] = 81,
  [82] = 82,
  [83] = 83,
  [84] = 84,
  [85] = 85,
  [86] = 83,
  [87] = 87,
};

static bool ts_lex(TSLexer *lexer, TSStateId state) {
  START_LEXER();
  eof = lexer->eof(lexer);
  switch (state) {
    case 0:
      if (eof) ADVANCE(13);
      ADVANCE_MAP(
        '\n', 643,
        '\r', 1,
        '!', 121,
        '"', 156,
        '#', 641,
        '%', 118,
        '&', 122,
        '(', 114,
        ')', 115,
        '*', 120,
        '+', 119,
        ',', 116,
        '-', 125,
        '.', 14,
        '/', 117,
        '<', 152,
        '=', 113,
        '>', 153,
        '\\', 159,
        'a', 383,
        'b', 222,
        'c', 223,
        'd', 316,
        'e', 626,
        'f', 228,
        'g', 385,
        'h', 292,
        'i', 448,
        'j', 481,
        'k', 605,
        'l', 293,
        'm', 227,
        'n', 607,
        'o', 449,
        'p', 256,
        'q', 606,
        'r', 294,
        's', 225,
        't', 236,
        'u', 450,
        'v', 232,
        'w', 351,
        '|', 123,
      );
      if (lookahead == '\t' ||
          lookahead == ' ') SKIP(0);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(163);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('x' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 1:
      if (lookahead == '\n') ADVANCE(643);
      END_STATE();
    case 2:
      ADVANCE_MAP(
        '\n', 643,
        '\r', 1,
        '!', 121,
        '"', 156,
        '%', 118,
        '&', 122,
        '(', 114,
        ')', 115,
        '*', 120,
        '+', 119,
        ',', 116,
        '-', 125,
        '.', 14,
        '/', 117,
        '<', 152,
        '=', 113,
        '>', 153,
        '\\', 4,
        'a', 185,
        'c', 193,
        'd', 174,
        'f', 171,
        'i', 186,
        'l', 175,
        'o', 187,
        't', 194,
        '|', 123,
      );
      if (lookahead == '\t' ||
          lookahead == ' ') SKIP(2);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(163);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 3:
      if (lookahead == '\n') ADVANCE(642);
      END_STATE();
    case 4:
      if (lookahead == '\n') ADVANCE(642);
      if (lookahead == '\r') ADVANCE(3);
      END_STATE();
    case 5:
      if (lookahead == '"') ADVANCE(156);
      if (lookahead == '(') ADVANCE(114);
      if (lookahead == ')') ADVANCE(115);
      if (lookahead == '-') ADVANCE(125);
      if (lookahead == '\\') ADVANCE(4);
      if (lookahead == 'f') ADVANCE(229);
      if (lookahead == 't') ADVANCE(545);
      if (lookahead == '\t' ||
          lookahead == ' ') SKIP(5);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(163);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 6:
      if (lookahead == '"') ADVANCE(156);
      if (lookahead == '\\') ADVANCE(159);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(158);
      if (lookahead != 0 &&
          lookahead != '\t' &&
          lookahead != '\n') ADVANCE(157);
      END_STATE();
    case 7:
      if (lookahead == '\\') ADVANCE(4);
      if (lookahead == '\t' ||
          lookahead == ' ') SKIP(7);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 8:
      if (lookahead == '\\') ADVANCE(161);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(162);
      if (lookahead != 0 &&
          lookahead != '\t' &&
          lookahead != '\n') ADVANCE(160);
      END_STATE();
    case 9:
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(164);
      END_STATE();
    case 10:
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(154);
      END_STATE();
    case 11:
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(155);
      END_STATE();
    case 12:
      if (eof) ADVANCE(13);
      ADVANCE_MAP(
        '\n', 643,
        '\r', 1,
        '#', 641,
        '%', 118,
        '(', 114,
        ')', 115,
        '*', 120,
        '+', 119,
        ',', 116,
        '-', 124,
        '.', 14,
        '/', 117,
        '\\', 4,
        'a', 384,
        'b', 222,
        'c', 224,
        'd', 325,
        'e', 626,
        'f', 354,
        'g', 385,
        'h', 292,
        'i', 462,
        'j', 481,
        'k', 605,
        'l', 349,
        'm', 227,
        'n', 607,
        'p', 256,
        'q', 606,
        'r', 294,
        's', 225,
        't', 237,
        'u', 450,
        'v', 232,
        'w', 351,
      );
      if (lookahead == '\t' ||
          lookahead == ' ') SKIP(12);
      if (('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('o' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 13:
      ACCEPT_TOKEN(ts_builtin_sym_end);
      END_STATE();
    case 14:
      ACCEPT_TOKEN(anon_sym_DOT);
      END_STATE();
    case 15:
      ACCEPT_TOKEN(anon_sym_load);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 16:
      ACCEPT_TOKEN(anon_sym_save);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 17:
      ACCEPT_TOKEN(anon_sym_use);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 18:
      ACCEPT_TOKEN(anon_sym_stash);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 19:
      ACCEPT_TOKEN(anon_sym_frames);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 20:
      ACCEPT_TOKEN(anon_sym_drop_frame);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 21:
      ACCEPT_TOKEN(anon_sym_with);
      if (lookahead == '_') ADVANCE(537);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 22:
      ACCEPT_TOKEN(anon_sym_unnest);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 23:
      ACCEPT_TOKEN(anon_sym_explode);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 24:
      ACCEPT_TOKEN(anon_sym_upsample);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 25:
      ACCEPT_TOKEN(anon_sym_tree);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 26:
      ACCEPT_TOKEN(anon_sym_graph);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 27:
      ACCEPT_TOKEN(anon_sym_show_graph);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 28:
      ACCEPT_TOKEN(anon_sym_mermaid);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 29:
      ACCEPT_TOKEN(anon_sym_explain_tree);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 30:
      ACCEPT_TOKEN(anon_sym_select);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 31:
      ACCEPT_TOKEN(anon_sym_drop);
      if (lookahead == '_') ADVANCE(336);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 32:
      ACCEPT_TOKEN(anon_sym_filter);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 33:
      ACCEPT_TOKEN(anon_sym_sort);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 34:
      ACCEPT_TOKEN(anon_sym_limit);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 35:
      ACCEPT_TOKEN(anon_sym_head);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 36:
      ACCEPT_TOKEN(anon_sym_tail);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 37:
      ACCEPT_TOKEN(anon_sym_show);
      if (lookahead == '_') ADVANCE(342);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 38:
      ACCEPT_TOKEN(anon_sym_schema);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 39:
      ACCEPT_TOKEN(anon_sym_describe);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 40:
      ACCEPT_TOKEN(anon_sym_groupby);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 41:
      ACCEPT_TOKEN(anon_sym_join);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 42:
      ACCEPT_TOKEN(anon_sym_explain);
      if (lookahead == '_') ADVANCE(593);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 43:
      ACCEPT_TOKEN(anon_sym_collect);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 44:
      ACCEPT_TOKEN(anon_sym_reset);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 45:
      ACCEPT_TOKEN(anon_sym_source);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 46:
      ACCEPT_TOKEN(anon_sym_timing);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 47:
      ACCEPT_TOKEN(anon_sym_info);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 48:
      ACCEPT_TOKEN(anon_sym_clear);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 49:
      ACCEPT_TOKEN(anon_sym_exit);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 50:
      ACCEPT_TOKEN(anon_sym_quit);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 51:
      ACCEPT_TOKEN(anon_sym_reverse);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 52:
      ACCEPT_TOKEN(anon_sym_sample);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 53:
      ACCEPT_TOKEN(anon_sym_shuffle);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 54:
      ACCEPT_TOKEN(anon_sym_unique);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 55:
      ACCEPT_TOKEN(anon_sym_null_count);
      if (lookahead == '_') ADVANCE(269);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 56:
      ACCEPT_TOKEN(anon_sym_glimpse);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 57:
      ACCEPT_TOKEN(anon_sym_size);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 58:
      ACCEPT_TOKEN(anon_sym_cast);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 59:
      ACCEPT_TOKEN(anon_sym_fill_null);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 60:
      ACCEPT_TOKEN(anon_sym_drop_null);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 61:
      ACCEPT_TOKEN(anon_sym_rename);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 62:
      ACCEPT_TOKEN(anon_sym_sum);
      if (lookahead == '_') ADVANCE(264);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 63:
      ACCEPT_TOKEN(anon_sym_mean);
      if (lookahead == '_') ADVANCE(266);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 64:
      ACCEPT_TOKEN(anon_sym_avg);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 65:
      ACCEPT_TOKEN(anon_sym_min);
      if (lookahead == '_') ADVANCE(262);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 66:
      ACCEPT_TOKEN(anon_sym_max);
      if (lookahead == '_') ADVANCE(259);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 67:
      ACCEPT_TOKEN(anon_sym_median);
      if (lookahead == '_') ADVANCE(268);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 68:
      ACCEPT_TOKEN(anon_sym_std);
      if (lookahead == '_') ADVANCE(263);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 69:
      ACCEPT_TOKEN(anon_sym_write);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 70:
      ACCEPT_TOKEN(anon_sym_with_row_index);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 71:
      ACCEPT_TOKEN(anon_sym_pwd);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 72:
      ACCEPT_TOKEN(anon_sym_ls);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 73:
      ACCEPT_TOKEN(anon_sym_cd);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 74:
      ACCEPT_TOKEN(anon_sym_sum_horizontal);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 75:
      ACCEPT_TOKEN(anon_sym_mean_horizontal);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 76:
      ACCEPT_TOKEN(anon_sym_min_horizontal);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 77:
      ACCEPT_TOKEN(anon_sym_max_horizontal);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 78:
      ACCEPT_TOKEN(anon_sym_all_horizontal);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 79:
      ACCEPT_TOKEN(anon_sym_any_horizontal);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 80:
      ACCEPT_TOKEN(anon_sym_sum_all);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 81:
      ACCEPT_TOKEN(anon_sym_mean_all);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 82:
      ACCEPT_TOKEN(anon_sym_min_all);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 83:
      ACCEPT_TOKEN(anon_sym_max_all);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 84:
      ACCEPT_TOKEN(anon_sym_std_all);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 85:
      ACCEPT_TOKEN(anon_sym_var_all);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 86:
      ACCEPT_TOKEN(anon_sym_median_all);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 87:
      ACCEPT_TOKEN(anon_sym_count_all);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 88:
      ACCEPT_TOKEN(anon_sym_null_count_all);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 89:
      ACCEPT_TOKEN(anon_sym_scan_csv);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 90:
      ACCEPT_TOKEN(anon_sym_scan_parquet);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 91:
      ACCEPT_TOKEN(anon_sym_scan_ipc);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 92:
      ACCEPT_TOKEN(anon_sym_scan_arrow);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 93:
      ACCEPT_TOKEN(anon_sym_scan_json);
      if (lookahead == 'l') ADVANCE(95);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 94:
      ACCEPT_TOKEN(anon_sym_scan_ndjson);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 95:
      ACCEPT_TOKEN(anon_sym_scan_jsonl);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 96:
      ACCEPT_TOKEN(anon_sym_scan_auto);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 97:
      ACCEPT_TOKEN(anon_sym_fill_nan);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 98:
      ACCEPT_TOKEN(anon_sym_forward_fill);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 99:
      ACCEPT_TOKEN(anon_sym_backward_fill);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 100:
      ACCEPT_TOKEN(anon_sym_top_k);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 101:
      ACCEPT_TOKEN(anon_sym_bottom_k);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 102:
      ACCEPT_TOKEN(anon_sym_transpose);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 103:
      ACCEPT_TOKEN(anon_sym_unpivot);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 104:
      ACCEPT_TOKEN(anon_sym_melt);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 105:
      ACCEPT_TOKEN(anon_sym_partition_by);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 106:
      ACCEPT_TOKEN(anon_sym_skew);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 107:
      ACCEPT_TOKEN(anon_sym_kurtosis);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 108:
      ACCEPT_TOKEN(anon_sym_approx_n_unique);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 109:
      ACCEPT_TOKEN(anon_sym_corr);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 110:
      ACCEPT_TOKEN(anon_sym_cov);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 111:
      ACCEPT_TOKEN(anon_sym_pivot);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 112:
      ACCEPT_TOKEN(anon_sym_help);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 113:
      ACCEPT_TOKEN(anon_sym_EQ);
      if (lookahead == '=') ADVANCE(148);
      END_STATE();
    case 114:
      ACCEPT_TOKEN(anon_sym_LPAREN);
      END_STATE();
    case 115:
      ACCEPT_TOKEN(anon_sym_RPAREN);
      END_STATE();
    case 116:
      ACCEPT_TOKEN(anon_sym_COMMA);
      END_STATE();
    case 117:
      ACCEPT_TOKEN(anon_sym_SLASH);
      END_STATE();
    case 118:
      ACCEPT_TOKEN(anon_sym_PERCENT);
      END_STATE();
    case 119:
      ACCEPT_TOKEN(anon_sym_PLUS);
      END_STATE();
    case 120:
      ACCEPT_TOKEN(anon_sym_STAR);
      END_STATE();
    case 121:
      ACCEPT_TOKEN(anon_sym_BANG);
      if (lookahead == '=') ADVANCE(149);
      END_STATE();
    case 122:
      ACCEPT_TOKEN(anon_sym_AMP);
      END_STATE();
    case 123:
      ACCEPT_TOKEN(anon_sym_PIPE);
      END_STATE();
    case 124:
      ACCEPT_TOKEN(anon_sym_DASH);
      END_STATE();
    case 125:
      ACCEPT_TOKEN(anon_sym_DASH);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(163);
      END_STATE();
    case 126:
      ACCEPT_TOKEN(anon_sym_as);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'c') ADVANCE(130);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 127:
      ACCEPT_TOKEN(anon_sym_as);
      if (lookahead == 'c') ADVANCE(131);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 128:
      ACCEPT_TOKEN(anon_sym_on);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 129:
      ACCEPT_TOKEN(anon_sym_on);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 130:
      ACCEPT_TOKEN(anon_sym_asc);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 131:
      ACCEPT_TOKEN(anon_sym_asc);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 132:
      ACCEPT_TOKEN(anon_sym_desc);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 133:
      ACCEPT_TOKEN(anon_sym_desc);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 134:
      ACCEPT_TOKEN(anon_sym_and);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 135:
      ACCEPT_TOKEN(anon_sym_and);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 136:
      ACCEPT_TOKEN(anon_sym_or);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 137:
      ACCEPT_TOKEN(anon_sym_or);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 138:
      ACCEPT_TOKEN(anon_sym_is_null);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 139:
      ACCEPT_TOKEN(anon_sym_is_null);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 140:
      ACCEPT_TOKEN(anon_sym_is_not_null);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 141:
      ACCEPT_TOKEN(anon_sym_is_not_null);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 142:
      ACCEPT_TOKEN(anon_sym_inner);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 143:
      ACCEPT_TOKEN(anon_sym_inner);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 144:
      ACCEPT_TOKEN(anon_sym_left);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 145:
      ACCEPT_TOKEN(anon_sym_left);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 146:
      ACCEPT_TOKEN(anon_sym_cross);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 147:
      ACCEPT_TOKEN(anon_sym_cross);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 148:
      ACCEPT_TOKEN(anon_sym_EQ_EQ);
      END_STATE();
    case 149:
      ACCEPT_TOKEN(anon_sym_BANG_EQ);
      END_STATE();
    case 150:
      ACCEPT_TOKEN(anon_sym_LT_EQ);
      END_STATE();
    case 151:
      ACCEPT_TOKEN(anon_sym_GT_EQ);
      END_STATE();
    case 152:
      ACCEPT_TOKEN(anon_sym_LT);
      if (lookahead == '=') ADVANCE(150);
      END_STATE();
    case 153:
      ACCEPT_TOKEN(anon_sym_GT);
      if (lookahead == '=') ADVANCE(151);
      END_STATE();
    case 154:
      ACCEPT_TOKEN(sym_agg_spec);
      if (lookahead == ':') ADVANCE(11);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(154);
      END_STATE();
    case 155:
      ACCEPT_TOKEN(sym_agg_spec);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(155);
      END_STATE();
    case 156:
      ACCEPT_TOKEN(anon_sym_DQUOTE);
      END_STATE();
    case 157:
      ACCEPT_TOKEN(aux_sym_string_token1);
      END_STATE();
    case 158:
      ACCEPT_TOKEN(aux_sym_string_token1);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(158);
      if (lookahead != 0 &&
          lookahead != '\t' &&
          lookahead != '\n' &&
          lookahead != '"' &&
          lookahead != '\\') ADVANCE(157);
      END_STATE();
    case 159:
      ACCEPT_TOKEN(anon_sym_BSLASH);
      if (lookahead == '\n') ADVANCE(642);
      if (lookahead == '\r') ADVANCE(3);
      END_STATE();
    case 160:
      ACCEPT_TOKEN(aux_sym_string_token2);
      END_STATE();
    case 161:
      ACCEPT_TOKEN(aux_sym_string_token2);
      if (lookahead == '\n') ADVANCE(642);
      if (lookahead == '\r') ADVANCE(3);
      END_STATE();
    case 162:
      ACCEPT_TOKEN(aux_sym_string_token2);
      if (lookahead == '\\') ADVANCE(161);
      if (lookahead == '\t' ||
          lookahead == ' ') ADVANCE(162);
      if (lookahead != 0 &&
          lookahead != '\t' &&
          lookahead != '\n') ADVANCE(160);
      END_STATE();
    case 163:
      ACCEPT_TOKEN(sym_number);
      if (lookahead == '.') ADVANCE(9);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(163);
      END_STATE();
    case 164:
      ACCEPT_TOKEN(sym_number);
      if (('0' <= lookahead && lookahead <= '9')) ADVANCE(164);
      END_STATE();
    case 165:
      ACCEPT_TOKEN(anon_sym_true);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 166:
      ACCEPT_TOKEN(anon_sym_true);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 167:
      ACCEPT_TOKEN(anon_sym_false);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 168:
      ACCEPT_TOKEN(anon_sym_false);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 169:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == '_') ADVANCE(188);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 170:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == '_') ADVANCE(189);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 171:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'a') ADVANCE(182);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 172:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'c') ADVANCE(132);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 173:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'd') ADVANCE(134);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 174:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'e') ADVANCE(196);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 175:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'e') ADVANCE(179);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 176:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'e') ADVANCE(165);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 177:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'e') ADVANCE(167);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 178:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'e') ADVANCE(195);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 179:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'f') ADVANCE(200);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 180:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'l') ADVANCE(138);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 181:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'l') ADVANCE(140);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 182:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'l') ADVANCE(199);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 183:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'l') ADVANCE(180);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 184:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'l') ADVANCE(181);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 185:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'n') ADVANCE(173);
      if (lookahead == 's') ADVANCE(126);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 186:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'n') ADVANCE(190);
      if (lookahead == 's') ADVANCE(169);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 187:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'n') ADVANCE(128);
      if (lookahead == 'r') ADVANCE(136);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 188:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'n') ADVANCE(192);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 189:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'n') ADVANCE(203);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 190:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'n') ADVANCE(178);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 191:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'o') ADVANCE(198);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 192:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'o') ADVANCE(201);
      if (lookahead == 'u') ADVANCE(183);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 193:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'r') ADVANCE(191);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 194:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'r') ADVANCE(202);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 195:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'r') ADVANCE(142);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 196:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 's') ADVANCE(172);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 197:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 's') ADVANCE(146);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 198:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 's') ADVANCE(197);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 199:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 's') ADVANCE(177);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 200:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 't') ADVANCE(144);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 201:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 't') ADVANCE(170);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 202:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'u') ADVANCE(176);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 203:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (lookahead == 'u') ADVANCE(184);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 204:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == ':') ADVANCE(10);
      if (('-' <= lookahead && lookahead <= '/')) ADVANCE(640);
      if (('0' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(204);
      END_STATE();
    case 205:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(231);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 206:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(380);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 207:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(347);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 208:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(460);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 209:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(381);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 210:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(337);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 211:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(277);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 212:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(610);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 213:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(454);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 214:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(465);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 215:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(474);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 216:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(272);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 217:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(369);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 218:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(265);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 219:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(267);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 220:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(339);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 221:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == '_') ADVANCE(348);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 222:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(274);
      if (lookahead == 'o') ADVANCE(589);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 223:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(559);
      if (lookahead == 'd') ADVANCE(73);
      if (lookahead == 'l') ADVANCE(329);
      if (lookahead == 'o') ADVANCE(435);
      if (lookahead == 'r') ADVANCE(485);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 224:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(559);
      if (lookahead == 'd') ADVANCE(73);
      if (lookahead == 'l') ADVANCE(329);
      if (lookahead == 'o') ADVANCE(435);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 225:
      ACCEPT_TOKEN(sym_identifier);
      ADVANCE_MAP(
        'a', 438,
        'c', 240,
        'e', 414,
        'h', 482,
        'i', 633,
        'k', 318,
        'o', 539,
        't', 239,
        'u', 436,
      );
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 226:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(38);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 227:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(627);
      if (lookahead == 'e') ADVANCE(238);
      if (lookahead == 'i') ADVANCE(451);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 228:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(411);
      if (lookahead == 'i') ADVANCE(386);
      if (lookahead == 'o') ADVANCE(525);
      if (lookahead == 'r') ADVANCE(233);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 229:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(411);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 230:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(284);
      if (lookahead == 'l') ADVANCE(511);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 231:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(543);
      if (lookahead == 'c') ADVANCE(558);
      if (lookahead == 'i') ADVANCE(517);
      if (lookahead == 'j') ADVANCE(571);
      if (lookahead == 'n') ADVANCE(287);
      if (lookahead == 'p') ADVANCE(249);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 232:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(531);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 233:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(442);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 234:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(285);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 235:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(514);
      if (lookahead == 'o') ADVANCE(608);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 236:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(360);
      if (lookahead == 'i') ADVANCE(445);
      if (lookahead == 'o') ADVANCE(513);
      if (lookahead == 'r') ADVANCE(243);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 237:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(360);
      if (lookahead == 'i') ADVANCE(445);
      if (lookahead == 'o') ADVANCE(513);
      if (lookahead == 'r') ADVANCE(244);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 238:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(453);
      if (lookahead == 'd') ADVANCE(355);
      if (lookahead == 'l') ADVANCE(577);
      if (lookahead == 'r') ADVANCE(437);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 239:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(560);
      if (lookahead == 'd') ADVANCE(68);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 240:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(463);
      if (lookahead == 'h') ADVANCE(320);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 241:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(527);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 242:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(359);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 243:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(464);
      if (lookahead == 'e') ADVANCE(298);
      if (lookahead == 'u') ADVANCE(299);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 244:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(464);
      if (lookahead == 'e') ADVANCE(298);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 245:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(455);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 246:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(516);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 247:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(532);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 248:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(457);
      if (lookahead == 'u') ADVANCE(423);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 249:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(546);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 250:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(402);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 251:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(403);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 252:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(404);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 253:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(405);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 254:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(407);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 255:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(408);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 256:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(536);
      if (lookahead == 'i') ADVANCE(618);
      if (lookahead == 'w') ADVANCE(283);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 257:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(443);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 258:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(447);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 259:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(415);
      if (lookahead == 'h') ADVANCE(506);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 260:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(364);
      if (lookahead == 'o') ADVANCE(289);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 261:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(444);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 262:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(416);
      if (lookahead == 'h') ADVANCE(507);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 263:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(417);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 264:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(418);
      if (lookahead == 'h') ADVANCE(508);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 265:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(419);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 266:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(420);
      if (lookahead == 'h') ADVANCE(509);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 267:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(421);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 268:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(425);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 269:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(431);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 270:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'a') ADVANCE(548);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('b' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 271:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'b') ADVANCE(630);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 272:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'b') ADVANCE(631);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 273:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'b') ADVANCE(315);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 274:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'c') ADVANCE(382);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 275:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'c') ADVANCE(133);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 276:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'c') ADVANCE(91);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 277:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'c') ADVANCE(488);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 278:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'c') ADVANCE(541);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 279:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'c') ADVANCE(583);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 280:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'c') ADVANCE(304);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 281:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'c') ADVANCE(585);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 282:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'd') ADVANCE(135);
      if (lookahead == 'y') ADVANCE(221);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 283:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'd') ADVANCE(71);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 284:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'd') ADVANCE(35);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 285:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'd') ADVANCE(15);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 286:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'd') ADVANCE(28);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 287:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'd') ADVANCE(379);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 288:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'd') ADVANCE(210);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 289:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'd') ADVANCE(306);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 290:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'd') ADVANCE(319);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 291:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'd') ADVANCE(220);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 292:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(230);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 293:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(335);
      if (lookahead == 'i') ADVANCE(446);
      if (lookahead == 'o') ADVANCE(234);
      if (lookahead == 's') ADVANCE(72);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 294:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(472);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 295:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(17);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 296:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(16);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 297:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(57);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 298:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(25);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 299:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(166);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 300:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(168);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 301:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(69);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 302:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(61);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 303:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(52);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 304:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(45);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 305:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(54);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 306:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(23);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 307:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(56);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 308:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(51);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 309:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(53);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 310:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(24);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 311:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(102);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 312:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(20);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 313:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(29);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 314:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(108);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 315:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(39);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 316:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(557);
      if (lookahead == 'r') ADVANCE(484);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 317:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(279);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 318:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(621);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 319:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(628);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 320:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(439);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 321:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(528);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 322:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(555);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 323:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(544);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 324:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(529);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 325:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(561);
      if (lookahead == 'r') ADVANCE(484);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 326:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(582);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 327:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(588);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 328:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(313);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 329:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(241);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 330:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(566);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 331:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'e') ADVANCE(281);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 332:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'f') ADVANCE(338);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 333:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'f') ADVANCE(479);
      if (lookahead == 'n') ADVANCE(321);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 334:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'f') ADVANCE(479);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 335:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'f') ADVANCE(576);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 336:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'f') ADVANCE(547);
      if (lookahead == 'n') ADVANCE(615);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 337:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'f') ADVANCE(370);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 338:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'f') ADVANCE(429);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 339:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'f') ADVANCE(371);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 340:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'g') ADVANCE(64);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 341:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'g') ADVANCE(46);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 342:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'g') ADVANCE(535);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 343:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'h') ADVANCE(21);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 344:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'h') ADVANCE(26);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 345:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'h') ADVANCE(18);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 346:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'h') ADVANCE(27);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 347:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'h') ADVANCE(491);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 348:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'h') ADVANCE(505);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 349:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(446);
      if (lookahead == 'o') ADVANCE(234);
      if (lookahead == 's') ADVANCE(72);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 350:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(522);
      if (lookahead == 'n') ADVANCE(330);
      if (lookahead == 'p') ADVANCE(372);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 351:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(573);
      if (lookahead == 'r') ADVANCE(365);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 352:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(634);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 353:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(273);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 354:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(386);
      if (lookahead == 'o') ADVANCE(525);
      if (lookahead == 'r') ADVANCE(233);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 355:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(245);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 356:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(440);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 357:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(452);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 358:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(575);
      if (lookahead == 'p') ADVANCE(410);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 359:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(286);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 360:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(387);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 361:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(461);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 362:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(578);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 363:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(493);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 364:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(456);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 365:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(594);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 366:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(556);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 367:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(580);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 368:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(591);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 369:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(471);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 370:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(427);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 371:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(428);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 372:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(619);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 373:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(524);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 374:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(635);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 375:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(636);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 376:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(637);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 377:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(638);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 378:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'i') ADVANCE(639);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 379:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'j') ADVANCE(572);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 380:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'k') ADVANCE(100);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 381:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'k') ADVANCE(101);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 382:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'k') ADVANCE(625);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 383:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(409);
      if (lookahead == 'n') ADVANCE(282);
      if (lookahead == 'p') ADVANCE(515);
      if (lookahead == 's') ADVANCE(127);
      if (lookahead == 'v') ADVANCE(340);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 384:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(409);
      if (lookahead == 'n') ADVANCE(632);
      if (lookahead == 'p') ADVANCE(515);
      if (lookahead == 'v') ADVANCE(340);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 385:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(356);
      if (lookahead == 'r') ADVANCE(235);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 386:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(433);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 387:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(36);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 388:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(139);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 389:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(83);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 390:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(82);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 391:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(84);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 392:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(80);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 393:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(85);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 394:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(81);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 395:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(87);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 396:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(60);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 397:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(59);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 398:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(86);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 399:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(141);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 400:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(98);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 401:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(99);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 402:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(78);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 403:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(79);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 404:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(77);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 405:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(76);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 406:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(88);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 407:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(74);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 408:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(75);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 409:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(207);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 410:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(260);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 411:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(565);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 412:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(211);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 413:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(388);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 414:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(317);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 415:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(389);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 416:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(390);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 417:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(391);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 418:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(392);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 419:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(393);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 420:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(394);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 421:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(395);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 422:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(396);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 423:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(397);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 424:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(303);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 425:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(398);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 426:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(399);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 427:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(400);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 428:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(401);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 429:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(309);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 430:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(310);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 431:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(406);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 432:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(412);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 433:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(213);
      if (lookahead == 't') ADVANCE(324);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 434:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(331);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 435:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'l') ADVANCE(434);
      if (lookahead == 'r') ADVANCE(526);
      if (lookahead == 'u') ADVANCE(468);
      if (lookahead == 'v') ADVANCE(110);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 436:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(62);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 437:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(242);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 438:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(518);
      if (lookahead == 'v') ADVANCE(296);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 439:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(226);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 440:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(519);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 441:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(209);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 442:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(322);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 443:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(302);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 444:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(312);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 445:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(361);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 446:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(367);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 447:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'm') ADVANCE(521);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 448:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(333);
      if (lookahead == 's') ADVANCE(208);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 449:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(129);
      if (lookahead == 'r') ADVANCE(137);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 450:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(350);
      if (lookahead == 'p') ADVANCE(570);
      if (lookahead == 's') ADVANCE(295);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 451:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(65);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 452:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(41);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 453:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(63);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 454:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(248);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 455:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(67);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 456:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(42);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 457:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(97);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 458:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(93);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 459:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(94);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 460:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(495);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 461:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(341);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 462:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(334);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 463:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(205);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 464:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(564);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 465:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(212);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 466:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(373);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 467:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(216);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 468:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(604);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 469:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(587);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 470:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(596);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 471:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(290);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 472:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(257);
      if (lookahead == 's') ADVANCE(326);
      if (lookahead == 'v') ADVANCE(323);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 473:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(597);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 474:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(616);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 475:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(600);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 476:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(601);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 477:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(602);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 478:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'n') ADVANCE(603);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 479:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(47);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 480:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(96);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 481:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(357);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 482:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(620);
      if (lookahead == 'u') ADVANCE(332);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 483:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(629);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 484:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(510);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 485:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(562);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 486:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(623);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 487:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(622);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 488:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(613);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 489:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(441);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 490:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(563);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 491:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(538);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 492:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(581);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 493:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(467);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 494:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(458);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 495:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(598);
      if (lookahead == 'u') ADVANCE(413);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 496:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(459);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 497:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(586);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 498:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(470);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 499:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(569);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 500:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(473);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 501:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(475);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 502:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(476);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 503:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(477);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 504:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(478);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 505:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(549);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 506:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(550);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 507:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(551);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 508:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(552);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 509:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'o') ADVANCE(553);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 510:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(31);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 511:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(112);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 512:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(271);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 513:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(206);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 514:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(344);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 515:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(533);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 516:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(346);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 517:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(276);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 518:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(424);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 519:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(567);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 520:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(499);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 521:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'p') ADVANCE(430);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 522:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'q') ADVANCE(611);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 523:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'q') ADVANCE(614);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 524:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'q') ADVANCE(612);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 525:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(624);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 526:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(109);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 527:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(48);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 528:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(143);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 529:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(32);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 530:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(280);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 531:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(218);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 532:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(288);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 533:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(483);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 534:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(595);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 535:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(246);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 536:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(599);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 537:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(486);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 538:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(352);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 539:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(579);
      if (lookahead == 'u') ADVANCE(530);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 540:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(487);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 541:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(353);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 542:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(328);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 543:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(540);
      if (lookahead == 'u') ADVANCE(592);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 544:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(568);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 545:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(609);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 546:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(523);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 547:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(261);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 548:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(291);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 549:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(374);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 550:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(375);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 551:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(376);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 552:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(377);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 553:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'r') ADVANCE(378);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 554:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(147);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 555:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(19);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 556:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(107);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 557:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(275);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 558:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(617);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 559:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(574);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 560:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(345);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 561:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(278);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 562:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(554);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 563:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(366);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 564:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(520);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 565:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(300);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 566:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(584);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 567:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(307);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 568:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(308);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 569:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(311);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 570:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(258);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 571:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(494);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 572:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 's') ADVANCE(496);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 573:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(343);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 574:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(58);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 575:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(49);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 576:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(145);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 577:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(104);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 578:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(50);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 579:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(33);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 580:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(34);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 581:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(111);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 582:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(44);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 583:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(30);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 584:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(22);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 585:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(43);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 586:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(103);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 587:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(55);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 588:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(90);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 589:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(590);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 590:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(489);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 591:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(363);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 592:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(480);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 593:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(542);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 594:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(301);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 595:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(490);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 596:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(250);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 597:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(251);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 598:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(215);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 599:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(368);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 600:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(252);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 601:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(253);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 602:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(254);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 603:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(255);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 604:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 't') ADVANCE(219);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 605:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(534);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 606:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(362);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 607:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(432);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 608:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(512);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 609:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(299);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 610:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(466);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 611:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(305);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 612:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(314);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 613:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(469);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 614:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(327);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 615:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(422);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 616:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'u') ADVANCE(426);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 617:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'v') ADVANCE(89);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 618:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'v') ADVANCE(492);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 619:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'v') ADVANCE(497);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 620:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'w') ADVANCE(37);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 621:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'w') ADVANCE(106);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 622:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'w') ADVANCE(92);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 623:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'w') ADVANCE(217);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 624:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'w') ADVANCE(247);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 625:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'w') ADVANCE(270);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 626:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'x') ADVANCE(358);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 627:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'x') ADVANCE(66);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 628:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'x') ADVANCE(70);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 629:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'x') ADVANCE(214);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 630:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'y') ADVANCE(40);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 631:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'y') ADVANCE(105);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 632:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'y') ADVANCE(221);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 633:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'z') ADVANCE(297);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'y')) ADVANCE(640);
      END_STATE();
    case 634:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'z') ADVANCE(498);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'y')) ADVANCE(640);
      END_STATE();
    case 635:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'z') ADVANCE(500);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'y')) ADVANCE(640);
      END_STATE();
    case 636:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'z') ADVANCE(501);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'y')) ADVANCE(640);
      END_STATE();
    case 637:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'z') ADVANCE(502);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'y')) ADVANCE(640);
      END_STATE();
    case 638:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'z') ADVANCE(503);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'y')) ADVANCE(640);
      END_STATE();
    case 639:
      ACCEPT_TOKEN(sym_identifier);
      if (lookahead == 'z') ADVANCE(504);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'y')) ADVANCE(640);
      END_STATE();
    case 640:
      ACCEPT_TOKEN(sym_identifier);
      if (('-' <= lookahead && lookahead <= '9') ||
          ('A' <= lookahead && lookahead <= 'Z') ||
          lookahead == '_' ||
          ('a' <= lookahead && lookahead <= 'z')) ADVANCE(640);
      END_STATE();
    case 641:
      ACCEPT_TOKEN(sym_comment);
      if (lookahead != 0 &&
          lookahead != '\n') ADVANCE(641);
      END_STATE();
    case 642:
      ACCEPT_TOKEN(sym_line_continuation);
      END_STATE();
    case 643:
      ACCEPT_TOKEN(sym__newline);
      END_STATE();
    default:
      return false;
  }
}

static const TSLexMode ts_lex_modes[STATE_COUNT] = {
  [0] = {.lex_state = 0},
  [1] = {.lex_state = 12},
  [2] = {.lex_state = 12},
  [3] = {.lex_state = 12},
  [4] = {.lex_state = 12},
  [5] = {.lex_state = 12},
  [6] = {.lex_state = 12},
  [7] = {.lex_state = 12},
  [8] = {.lex_state = 12},
  [9] = {.lex_state = 2},
  [10] = {.lex_state = 2},
  [11] = {.lex_state = 2},
  [12] = {.lex_state = 2},
  [13] = {.lex_state = 2},
  [14] = {.lex_state = 2},
  [15] = {.lex_state = 2},
  [16] = {.lex_state = 2},
  [17] = {.lex_state = 2},
  [18] = {.lex_state = 2},
  [19] = {.lex_state = 2},
  [20] = {.lex_state = 2},
  [21] = {.lex_state = 2},
  [22] = {.lex_state = 2},
  [23] = {.lex_state = 2},
  [24] = {.lex_state = 2},
  [25] = {.lex_state = 2},
  [26] = {.lex_state = 2},
  [27] = {.lex_state = 2},
  [28] = {.lex_state = 2},
  [29] = {.lex_state = 2},
  [30] = {.lex_state = 2},
  [31] = {.lex_state = 2},
  [32] = {.lex_state = 2},
  [33] = {.lex_state = 2},
  [34] = {.lex_state = 2},
  [35] = {.lex_state = 2},
  [36] = {.lex_state = 5},
  [37] = {.lex_state = 5},
  [38] = {.lex_state = 5},
  [39] = {.lex_state = 5},
  [40] = {.lex_state = 5},
  [41] = {.lex_state = 5},
  [42] = {.lex_state = 5},
  [43] = {.lex_state = 5},
  [44] = {.lex_state = 5},
  [45] = {.lex_state = 5},
  [46] = {.lex_state = 5},
  [47] = {.lex_state = 12},
  [48] = {.lex_state = 12},
  [49] = {.lex_state = 12},
  [50] = {.lex_state = 12},
  [51] = {.lex_state = 12},
  [52] = {.lex_state = 12},
  [53] = {.lex_state = 12},
  [54] = {.lex_state = 12},
  [55] = {.lex_state = 12},
  [56] = {.lex_state = 12},
  [57] = {.lex_state = 12},
  [58] = {.lex_state = 12},
  [59] = {.lex_state = 12},
  [60] = {.lex_state = 12},
  [61] = {.lex_state = 12},
  [62] = {.lex_state = 12},
  [63] = {.lex_state = 6},
  [64] = {.lex_state = 6},
  [65] = {.lex_state = 6},
  [66] = {.lex_state = 6},
  [67] = {.lex_state = 6},
  [68] = {.lex_state = 0},
  [69] = {.lex_state = 0},
  [70] = {.lex_state = 0},
  [71] = {.lex_state = 0},
  [72] = {.lex_state = 0},
  [73] = {.lex_state = 0},
  [74] = {.lex_state = 6},
  [75] = {.lex_state = 0},
  [76] = {.lex_state = 0},
  [77] = {.lex_state = 0},
  [78] = {.lex_state = 0},
  [79] = {.lex_state = 0},
  [80] = {.lex_state = 0},
  [81] = {.lex_state = 0},
  [82] = {.lex_state = 0},
  [83] = {.lex_state = 0},
  [84] = {.lex_state = 7},
  [85] = {.lex_state = 8},
  [86] = {.lex_state = 0},
  [87] = {.lex_state = 0},
};

static const uint16_t ts_parse_table[LARGE_STATE_COUNT][SYMBOL_COUNT] = {
  [0] = {
    [ts_builtin_sym_end] = ACTIONS(1),
    [anon_sym_DOT] = ACTIONS(1),
    [anon_sym_load] = ACTIONS(1),
    [anon_sym_save] = ACTIONS(1),
    [anon_sym_use] = ACTIONS(1),
    [anon_sym_stash] = ACTIONS(1),
    [anon_sym_frames] = ACTIONS(1),
    [anon_sym_drop_frame] = ACTIONS(1),
    [anon_sym_with] = ACTIONS(1),
    [anon_sym_unnest] = ACTIONS(1),
    [anon_sym_explode] = ACTIONS(1),
    [anon_sym_upsample] = ACTIONS(1),
    [anon_sym_tree] = ACTIONS(1),
    [anon_sym_graph] = ACTIONS(1),
    [anon_sym_show_graph] = ACTIONS(1),
    [anon_sym_mermaid] = ACTIONS(1),
    [anon_sym_explain_tree] = ACTIONS(1),
    [anon_sym_select] = ACTIONS(1),
    [anon_sym_drop] = ACTIONS(1),
    [anon_sym_filter] = ACTIONS(1),
    [anon_sym_sort] = ACTIONS(1),
    [anon_sym_limit] = ACTIONS(1),
    [anon_sym_head] = ACTIONS(1),
    [anon_sym_tail] = ACTIONS(1),
    [anon_sym_show] = ACTIONS(1),
    [anon_sym_schema] = ACTIONS(1),
    [anon_sym_groupby] = ACTIONS(1),
    [anon_sym_join] = ACTIONS(1),
    [anon_sym_explain] = ACTIONS(1),
    [anon_sym_collect] = ACTIONS(1),
    [anon_sym_reset] = ACTIONS(1),
    [anon_sym_source] = ACTIONS(1),
    [anon_sym_timing] = ACTIONS(1),
    [anon_sym_info] = ACTIONS(1),
    [anon_sym_clear] = ACTIONS(1),
    [anon_sym_exit] = ACTIONS(1),
    [anon_sym_quit] = ACTIONS(1),
    [anon_sym_reverse] = ACTIONS(1),
    [anon_sym_sample] = ACTIONS(1),
    [anon_sym_shuffle] = ACTIONS(1),
    [anon_sym_unique] = ACTIONS(1),
    [anon_sym_null_count] = ACTIONS(1),
    [anon_sym_glimpse] = ACTIONS(1),
    [anon_sym_size] = ACTIONS(1),
    [anon_sym_cast] = ACTIONS(1),
    [anon_sym_fill_null] = ACTIONS(1),
    [anon_sym_drop_null] = ACTIONS(1),
    [anon_sym_rename] = ACTIONS(1),
    [anon_sym_sum] = ACTIONS(1),
    [anon_sym_mean] = ACTIONS(1),
    [anon_sym_avg] = ACTIONS(1),
    [anon_sym_min] = ACTIONS(1),
    [anon_sym_max] = ACTIONS(1),
    [anon_sym_median] = ACTIONS(1),
    [anon_sym_std] = ACTIONS(1),
    [anon_sym_write] = ACTIONS(1),
    [anon_sym_with_row_index] = ACTIONS(1),
    [anon_sym_pwd] = ACTIONS(1),
    [anon_sym_ls] = ACTIONS(1),
    [anon_sym_cd] = ACTIONS(1),
    [anon_sym_sum_horizontal] = ACTIONS(1),
    [anon_sym_mean_horizontal] = ACTIONS(1),
    [anon_sym_min_horizontal] = ACTIONS(1),
    [anon_sym_max_horizontal] = ACTIONS(1),
    [anon_sym_all_horizontal] = ACTIONS(1),
    [anon_sym_any_horizontal] = ACTIONS(1),
    [anon_sym_sum_all] = ACTIONS(1),
    [anon_sym_mean_all] = ACTIONS(1),
    [anon_sym_min_all] = ACTIONS(1),
    [anon_sym_max_all] = ACTIONS(1),
    [anon_sym_std_all] = ACTIONS(1),
    [anon_sym_var_all] = ACTIONS(1),
    [anon_sym_median_all] = ACTIONS(1),
    [anon_sym_count_all] = ACTIONS(1),
    [anon_sym_null_count_all] = ACTIONS(1),
    [anon_sym_scan_csv] = ACTIONS(1),
    [anon_sym_scan_parquet] = ACTIONS(1),
    [anon_sym_scan_ipc] = ACTIONS(1),
    [anon_sym_scan_arrow] = ACTIONS(1),
    [anon_sym_scan_json] = ACTIONS(1),
    [anon_sym_scan_ndjson] = ACTIONS(1),
    [anon_sym_scan_jsonl] = ACTIONS(1),
    [anon_sym_scan_auto] = ACTIONS(1),
    [anon_sym_fill_nan] = ACTIONS(1),
    [anon_sym_forward_fill] = ACTIONS(1),
    [anon_sym_backward_fill] = ACTIONS(1),
    [anon_sym_top_k] = ACTIONS(1),
    [anon_sym_bottom_k] = ACTIONS(1),
    [anon_sym_transpose] = ACTIONS(1),
    [anon_sym_unpivot] = ACTIONS(1),
    [anon_sym_melt] = ACTIONS(1),
    [anon_sym_partition_by] = ACTIONS(1),
    [anon_sym_skew] = ACTIONS(1),
    [anon_sym_kurtosis] = ACTIONS(1),
    [anon_sym_approx_n_unique] = ACTIONS(1),
    [anon_sym_corr] = ACTIONS(1),
    [anon_sym_cov] = ACTIONS(1),
    [anon_sym_pivot] = ACTIONS(1),
    [anon_sym_help] = ACTIONS(1),
    [anon_sym_EQ] = ACTIONS(1),
    [anon_sym_LPAREN] = ACTIONS(1),
    [anon_sym_RPAREN] = ACTIONS(1),
    [anon_sym_COMMA] = ACTIONS(1),
    [anon_sym_SLASH] = ACTIONS(1),
    [anon_sym_PERCENT] = ACTIONS(1),
    [anon_sym_PLUS] = ACTIONS(1),
    [anon_sym_STAR] = ACTIONS(1),
    [anon_sym_BANG] = ACTIONS(1),
    [anon_sym_AMP] = ACTIONS(1),
    [anon_sym_PIPE] = ACTIONS(1),
    [anon_sym_DASH] = ACTIONS(1),
    [anon_sym_as] = ACTIONS(1),
    [anon_sym_on] = ACTIONS(1),
    [anon_sym_asc] = ACTIONS(1),
    [anon_sym_desc] = ACTIONS(1),
    [anon_sym_and] = ACTIONS(1),
    [anon_sym_or] = ACTIONS(1),
    [anon_sym_is_null] = ACTIONS(1),
    [anon_sym_is_not_null] = ACTIONS(1),
    [anon_sym_inner] = ACTIONS(1),
    [anon_sym_left] = ACTIONS(1),
    [anon_sym_cross] = ACTIONS(1),
    [anon_sym_EQ_EQ] = ACTIONS(1),
    [anon_sym_BANG_EQ] = ACTIONS(1),
    [anon_sym_LT_EQ] = ACTIONS(1),
    [anon_sym_GT_EQ] = ACTIONS(1),
    [anon_sym_LT] = ACTIONS(1),
    [anon_sym_GT] = ACTIONS(1),
    [anon_sym_DQUOTE] = ACTIONS(1),
    [anon_sym_BSLASH] = ACTIONS(1),
    [sym_number] = ACTIONS(1),
    [anon_sym_true] = ACTIONS(1),
    [anon_sym_false] = ACTIONS(1),
    [sym_identifier] = ACTIONS(1),
    [sym_comment] = ACTIONS(1),
    [sym_line_continuation] = ACTIONS(3),
    [sym__newline] = ACTIONS(1),
  },
  [1] = {
    [sym_source_file] = STATE(87),
    [sym_statement] = STATE(2),
    [sym_command] = STATE(9),
    [aux_sym_source_file_repeat1] = STATE(2),
    [ts_builtin_sym_end] = ACTIONS(5),
    [anon_sym_DOT] = ACTIONS(7),
    [anon_sym_load] = ACTIONS(9),
    [anon_sym_save] = ACTIONS(9),
    [anon_sym_use] = ACTIONS(9),
    [anon_sym_stash] = ACTIONS(9),
    [anon_sym_frames] = ACTIONS(9),
    [anon_sym_drop_frame] = ACTIONS(9),
    [anon_sym_with] = ACTIONS(9),
    [anon_sym_unnest] = ACTIONS(9),
    [anon_sym_explode] = ACTIONS(9),
    [anon_sym_upsample] = ACTIONS(9),
    [anon_sym_tree] = ACTIONS(9),
    [anon_sym_graph] = ACTIONS(9),
    [anon_sym_show_graph] = ACTIONS(9),
    [anon_sym_mermaid] = ACTIONS(9),
    [anon_sym_explain_tree] = ACTIONS(9),
    [anon_sym_select] = ACTIONS(9),
    [anon_sym_drop] = ACTIONS(9),
    [anon_sym_filter] = ACTIONS(9),
    [anon_sym_sort] = ACTIONS(9),
    [anon_sym_limit] = ACTIONS(9),
    [anon_sym_head] = ACTIONS(9),
    [anon_sym_tail] = ACTIONS(9),
    [anon_sym_show] = ACTIONS(9),
    [anon_sym_schema] = ACTIONS(9),
    [anon_sym_describe] = ACTIONS(9),
    [anon_sym_groupby] = ACTIONS(9),
    [anon_sym_join] = ACTIONS(9),
    [anon_sym_explain] = ACTIONS(9),
    [anon_sym_collect] = ACTIONS(9),
    [anon_sym_reset] = ACTIONS(9),
    [anon_sym_source] = ACTIONS(9),
    [anon_sym_timing] = ACTIONS(9),
    [anon_sym_info] = ACTIONS(9),
    [anon_sym_clear] = ACTIONS(9),
    [anon_sym_exit] = ACTIONS(9),
    [anon_sym_quit] = ACTIONS(9),
    [anon_sym_reverse] = ACTIONS(9),
    [anon_sym_sample] = ACTIONS(9),
    [anon_sym_shuffle] = ACTIONS(9),
    [anon_sym_unique] = ACTIONS(9),
    [anon_sym_null_count] = ACTIONS(9),
    [anon_sym_glimpse] = ACTIONS(9),
    [anon_sym_size] = ACTIONS(9),
    [anon_sym_cast] = ACTIONS(9),
    [anon_sym_fill_null] = ACTIONS(9),
    [anon_sym_drop_null] = ACTIONS(9),
    [anon_sym_rename] = ACTIONS(9),
    [anon_sym_sum] = ACTIONS(9),
    [anon_sym_mean] = ACTIONS(9),
    [anon_sym_avg] = ACTIONS(9),
    [anon_sym_min] = ACTIONS(9),
    [anon_sym_max] = ACTIONS(9),
    [anon_sym_median] = ACTIONS(9),
    [anon_sym_std] = ACTIONS(9),
    [anon_sym_write] = ACTIONS(9),
    [anon_sym_with_row_index] = ACTIONS(9),
    [anon_sym_pwd] = ACTIONS(9),
    [anon_sym_ls] = ACTIONS(9),
    [anon_sym_cd] = ACTIONS(9),
    [anon_sym_sum_horizontal] = ACTIONS(9),
    [anon_sym_mean_horizontal] = ACTIONS(9),
    [anon_sym_min_horizontal] = ACTIONS(9),
    [anon_sym_max_horizontal] = ACTIONS(9),
    [anon_sym_all_horizontal] = ACTIONS(9),
    [anon_sym_any_horizontal] = ACTIONS(9),
    [anon_sym_sum_all] = ACTIONS(9),
    [anon_sym_mean_all] = ACTIONS(9),
    [anon_sym_min_all] = ACTIONS(9),
    [anon_sym_max_all] = ACTIONS(9),
    [anon_sym_std_all] = ACTIONS(9),
    [anon_sym_var_all] = ACTIONS(9),
    [anon_sym_median_all] = ACTIONS(9),
    [anon_sym_count_all] = ACTIONS(9),
    [anon_sym_null_count_all] = ACTIONS(9),
    [anon_sym_scan_csv] = ACTIONS(9),
    [anon_sym_scan_parquet] = ACTIONS(9),
    [anon_sym_scan_ipc] = ACTIONS(9),
    [anon_sym_scan_arrow] = ACTIONS(9),
    [anon_sym_scan_json] = ACTIONS(9),
    [anon_sym_scan_ndjson] = ACTIONS(9),
    [anon_sym_scan_jsonl] = ACTIONS(9),
    [anon_sym_scan_auto] = ACTIONS(9),
    [anon_sym_fill_nan] = ACTIONS(9),
    [anon_sym_forward_fill] = ACTIONS(9),
    [anon_sym_backward_fill] = ACTIONS(9),
    [anon_sym_top_k] = ACTIONS(9),
    [anon_sym_bottom_k] = ACTIONS(9),
    [anon_sym_transpose] = ACTIONS(9),
    [anon_sym_unpivot] = ACTIONS(9),
    [anon_sym_melt] = ACTIONS(9),
    [anon_sym_partition_by] = ACTIONS(9),
    [anon_sym_skew] = ACTIONS(9),
    [anon_sym_kurtosis] = ACTIONS(9),
    [anon_sym_approx_n_unique] = ACTIONS(9),
    [anon_sym_corr] = ACTIONS(9),
    [anon_sym_cov] = ACTIONS(9),
    [anon_sym_pivot] = ACTIONS(9),
    [anon_sym_help] = ACTIONS(9),
    [sym_identifier] = ACTIONS(9),
    [sym_comment] = ACTIONS(11),
    [sym_line_continuation] = ACTIONS(3),
    [sym__newline] = ACTIONS(11),
  },
  [2] = {
    [sym_statement] = STATE(3),
    [sym_command] = STATE(9),
    [aux_sym_source_file_repeat1] = STATE(3),
    [ts_builtin_sym_end] = ACTIONS(13),
    [anon_sym_DOT] = ACTIONS(7),
    [anon_sym_load] = ACTIONS(9),
    [anon_sym_save] = ACTIONS(9),
    [anon_sym_use] = ACTIONS(9),
    [anon_sym_stash] = ACTIONS(9),
    [anon_sym_frames] = ACTIONS(9),
    [anon_sym_drop_frame] = ACTIONS(9),
    [anon_sym_with] = ACTIONS(9),
    [anon_sym_unnest] = ACTIONS(9),
    [anon_sym_explode] = ACTIONS(9),
    [anon_sym_upsample] = ACTIONS(9),
    [anon_sym_tree] = ACTIONS(9),
    [anon_sym_graph] = ACTIONS(9),
    [anon_sym_show_graph] = ACTIONS(9),
    [anon_sym_mermaid] = ACTIONS(9),
    [anon_sym_explain_tree] = ACTIONS(9),
    [anon_sym_select] = ACTIONS(9),
    [anon_sym_drop] = ACTIONS(9),
    [anon_sym_filter] = ACTIONS(9),
    [anon_sym_sort] = ACTIONS(9),
    [anon_sym_limit] = ACTIONS(9),
    [anon_sym_head] = ACTIONS(9),
    [anon_sym_tail] = ACTIONS(9),
    [anon_sym_show] = ACTIONS(9),
    [anon_sym_schema] = ACTIONS(9),
    [anon_sym_describe] = ACTIONS(9),
    [anon_sym_groupby] = ACTIONS(9),
    [anon_sym_join] = ACTIONS(9),
    [anon_sym_explain] = ACTIONS(9),
    [anon_sym_collect] = ACTIONS(9),
    [anon_sym_reset] = ACTIONS(9),
    [anon_sym_source] = ACTIONS(9),
    [anon_sym_timing] = ACTIONS(9),
    [anon_sym_info] = ACTIONS(9),
    [anon_sym_clear] = ACTIONS(9),
    [anon_sym_exit] = ACTIONS(9),
    [anon_sym_quit] = ACTIONS(9),
    [anon_sym_reverse] = ACTIONS(9),
    [anon_sym_sample] = ACTIONS(9),
    [anon_sym_shuffle] = ACTIONS(9),
    [anon_sym_unique] = ACTIONS(9),
    [anon_sym_null_count] = ACTIONS(9),
    [anon_sym_glimpse] = ACTIONS(9),
    [anon_sym_size] = ACTIONS(9),
    [anon_sym_cast] = ACTIONS(9),
    [anon_sym_fill_null] = ACTIONS(9),
    [anon_sym_drop_null] = ACTIONS(9),
    [anon_sym_rename] = ACTIONS(9),
    [anon_sym_sum] = ACTIONS(9),
    [anon_sym_mean] = ACTIONS(9),
    [anon_sym_avg] = ACTIONS(9),
    [anon_sym_min] = ACTIONS(9),
    [anon_sym_max] = ACTIONS(9),
    [anon_sym_median] = ACTIONS(9),
    [anon_sym_std] = ACTIONS(9),
    [anon_sym_write] = ACTIONS(9),
    [anon_sym_with_row_index] = ACTIONS(9),
    [anon_sym_pwd] = ACTIONS(9),
    [anon_sym_ls] = ACTIONS(9),
    [anon_sym_cd] = ACTIONS(9),
    [anon_sym_sum_horizontal] = ACTIONS(9),
    [anon_sym_mean_horizontal] = ACTIONS(9),
    [anon_sym_min_horizontal] = ACTIONS(9),
    [anon_sym_max_horizontal] = ACTIONS(9),
    [anon_sym_all_horizontal] = ACTIONS(9),
    [anon_sym_any_horizontal] = ACTIONS(9),
    [anon_sym_sum_all] = ACTIONS(9),
    [anon_sym_mean_all] = ACTIONS(9),
    [anon_sym_min_all] = ACTIONS(9),
    [anon_sym_max_all] = ACTIONS(9),
    [anon_sym_std_all] = ACTIONS(9),
    [anon_sym_var_all] = ACTIONS(9),
    [anon_sym_median_all] = ACTIONS(9),
    [anon_sym_count_all] = ACTIONS(9),
    [anon_sym_null_count_all] = ACTIONS(9),
    [anon_sym_scan_csv] = ACTIONS(9),
    [anon_sym_scan_parquet] = ACTIONS(9),
    [anon_sym_scan_ipc] = ACTIONS(9),
    [anon_sym_scan_arrow] = ACTIONS(9),
    [anon_sym_scan_json] = ACTIONS(9),
    [anon_sym_scan_ndjson] = ACTIONS(9),
    [anon_sym_scan_jsonl] = ACTIONS(9),
    [anon_sym_scan_auto] = ACTIONS(9),
    [anon_sym_fill_nan] = ACTIONS(9),
    [anon_sym_forward_fill] = ACTIONS(9),
    [anon_sym_backward_fill] = ACTIONS(9),
    [anon_sym_top_k] = ACTIONS(9),
    [anon_sym_bottom_k] = ACTIONS(9),
    [anon_sym_transpose] = ACTIONS(9),
    [anon_sym_unpivot] = ACTIONS(9),
    [anon_sym_melt] = ACTIONS(9),
    [anon_sym_partition_by] = ACTIONS(9),
    [anon_sym_skew] = ACTIONS(9),
    [anon_sym_kurtosis] = ACTIONS(9),
    [anon_sym_approx_n_unique] = ACTIONS(9),
    [anon_sym_corr] = ACTIONS(9),
    [anon_sym_cov] = ACTIONS(9),
    [anon_sym_pivot] = ACTIONS(9),
    [anon_sym_help] = ACTIONS(9),
    [sym_identifier] = ACTIONS(9),
    [sym_comment] = ACTIONS(15),
    [sym_line_continuation] = ACTIONS(3),
    [sym__newline] = ACTIONS(15),
  },
  [3] = {
    [sym_statement] = STATE(3),
    [sym_command] = STATE(9),
    [aux_sym_source_file_repeat1] = STATE(3),
    [ts_builtin_sym_end] = ACTIONS(17),
    [anon_sym_DOT] = ACTIONS(19),
    [anon_sym_load] = ACTIONS(22),
    [anon_sym_save] = ACTIONS(22),
    [anon_sym_use] = ACTIONS(22),
    [anon_sym_stash] = ACTIONS(22),
    [anon_sym_frames] = ACTIONS(22),
    [anon_sym_drop_frame] = ACTIONS(22),
    [anon_sym_with] = ACTIONS(22),
    [anon_sym_unnest] = ACTIONS(22),
    [anon_sym_explode] = ACTIONS(22),
    [anon_sym_upsample] = ACTIONS(22),
    [anon_sym_tree] = ACTIONS(22),
    [anon_sym_graph] = ACTIONS(22),
    [anon_sym_show_graph] = ACTIONS(22),
    [anon_sym_mermaid] = ACTIONS(22),
    [anon_sym_explain_tree] = ACTIONS(22),
    [anon_sym_select] = ACTIONS(22),
    [anon_sym_drop] = ACTIONS(22),
    [anon_sym_filter] = ACTIONS(22),
    [anon_sym_sort] = ACTIONS(22),
    [anon_sym_limit] = ACTIONS(22),
    [anon_sym_head] = ACTIONS(22),
    [anon_sym_tail] = ACTIONS(22),
    [anon_sym_show] = ACTIONS(22),
    [anon_sym_schema] = ACTIONS(22),
    [anon_sym_describe] = ACTIONS(22),
    [anon_sym_groupby] = ACTIONS(22),
    [anon_sym_join] = ACTIONS(22),
    [anon_sym_explain] = ACTIONS(22),
    [anon_sym_collect] = ACTIONS(22),
    [anon_sym_reset] = ACTIONS(22),
    [anon_sym_source] = ACTIONS(22),
    [anon_sym_timing] = ACTIONS(22),
    [anon_sym_info] = ACTIONS(22),
    [anon_sym_clear] = ACTIONS(22),
    [anon_sym_exit] = ACTIONS(22),
    [anon_sym_quit] = ACTIONS(22),
    [anon_sym_reverse] = ACTIONS(22),
    [anon_sym_sample] = ACTIONS(22),
    [anon_sym_shuffle] = ACTIONS(22),
    [anon_sym_unique] = ACTIONS(22),
    [anon_sym_null_count] = ACTIONS(22),
    [anon_sym_glimpse] = ACTIONS(22),
    [anon_sym_size] = ACTIONS(22),
    [anon_sym_cast] = ACTIONS(22),
    [anon_sym_fill_null] = ACTIONS(22),
    [anon_sym_drop_null] = ACTIONS(22),
    [anon_sym_rename] = ACTIONS(22),
    [anon_sym_sum] = ACTIONS(22),
    [anon_sym_mean] = ACTIONS(22),
    [anon_sym_avg] = ACTIONS(22),
    [anon_sym_min] = ACTIONS(22),
    [anon_sym_max] = ACTIONS(22),
    [anon_sym_median] = ACTIONS(22),
    [anon_sym_std] = ACTIONS(22),
    [anon_sym_write] = ACTIONS(22),
    [anon_sym_with_row_index] = ACTIONS(22),
    [anon_sym_pwd] = ACTIONS(22),
    [anon_sym_ls] = ACTIONS(22),
    [anon_sym_cd] = ACTIONS(22),
    [anon_sym_sum_horizontal] = ACTIONS(22),
    [anon_sym_mean_horizontal] = ACTIONS(22),
    [anon_sym_min_horizontal] = ACTIONS(22),
    [anon_sym_max_horizontal] = ACTIONS(22),
    [anon_sym_all_horizontal] = ACTIONS(22),
    [anon_sym_any_horizontal] = ACTIONS(22),
    [anon_sym_sum_all] = ACTIONS(22),
    [anon_sym_mean_all] = ACTIONS(22),
    [anon_sym_min_all] = ACTIONS(22),
    [anon_sym_max_all] = ACTIONS(22),
    [anon_sym_std_all] = ACTIONS(22),
    [anon_sym_var_all] = ACTIONS(22),
    [anon_sym_median_all] = ACTIONS(22),
    [anon_sym_count_all] = ACTIONS(22),
    [anon_sym_null_count_all] = ACTIONS(22),
    [anon_sym_scan_csv] = ACTIONS(22),
    [anon_sym_scan_parquet] = ACTIONS(22),
    [anon_sym_scan_ipc] = ACTIONS(22),
    [anon_sym_scan_arrow] = ACTIONS(22),
    [anon_sym_scan_json] = ACTIONS(22),
    [anon_sym_scan_ndjson] = ACTIONS(22),
    [anon_sym_scan_jsonl] = ACTIONS(22),
    [anon_sym_scan_auto] = ACTIONS(22),
    [anon_sym_fill_nan] = ACTIONS(22),
    [anon_sym_forward_fill] = ACTIONS(22),
    [anon_sym_backward_fill] = ACTIONS(22),
    [anon_sym_top_k] = ACTIONS(22),
    [anon_sym_bottom_k] = ACTIONS(22),
    [anon_sym_transpose] = ACTIONS(22),
    [anon_sym_unpivot] = ACTIONS(22),
    [anon_sym_melt] = ACTIONS(22),
    [anon_sym_partition_by] = ACTIONS(22),
    [anon_sym_skew] = ACTIONS(22),
    [anon_sym_kurtosis] = ACTIONS(22),
    [anon_sym_approx_n_unique] = ACTIONS(22),
    [anon_sym_corr] = ACTIONS(22),
    [anon_sym_cov] = ACTIONS(22),
    [anon_sym_pivot] = ACTIONS(22),
    [anon_sym_help] = ACTIONS(22),
    [sym_identifier] = ACTIONS(22),
    [sym_comment] = ACTIONS(25),
    [sym_line_continuation] = ACTIONS(3),
    [sym__newline] = ACTIONS(25),
  },
  [4] = {
    [ts_builtin_sym_end] = ACTIONS(28),
    [anon_sym_DOT] = ACTIONS(28),
    [anon_sym_load] = ACTIONS(30),
    [anon_sym_save] = ACTIONS(30),
    [anon_sym_use] = ACTIONS(30),
    [anon_sym_stash] = ACTIONS(30),
    [anon_sym_frames] = ACTIONS(30),
    [anon_sym_drop_frame] = ACTIONS(30),
    [anon_sym_with] = ACTIONS(30),
    [anon_sym_unnest] = ACTIONS(30),
    [anon_sym_explode] = ACTIONS(30),
    [anon_sym_upsample] = ACTIONS(30),
    [anon_sym_tree] = ACTIONS(30),
    [anon_sym_graph] = ACTIONS(30),
    [anon_sym_show_graph] = ACTIONS(30),
    [anon_sym_mermaid] = ACTIONS(30),
    [anon_sym_explain_tree] = ACTIONS(30),
    [anon_sym_select] = ACTIONS(30),
    [anon_sym_drop] = ACTIONS(30),
    [anon_sym_filter] = ACTIONS(30),
    [anon_sym_sort] = ACTIONS(30),
    [anon_sym_limit] = ACTIONS(30),
    [anon_sym_head] = ACTIONS(30),
    [anon_sym_tail] = ACTIONS(30),
    [anon_sym_show] = ACTIONS(30),
    [anon_sym_schema] = ACTIONS(30),
    [anon_sym_describe] = ACTIONS(30),
    [anon_sym_groupby] = ACTIONS(30),
    [anon_sym_join] = ACTIONS(30),
    [anon_sym_explain] = ACTIONS(30),
    [anon_sym_collect] = ACTIONS(30),
    [anon_sym_reset] = ACTIONS(30),
    [anon_sym_source] = ACTIONS(30),
    [anon_sym_timing] = ACTIONS(30),
    [anon_sym_info] = ACTIONS(30),
    [anon_sym_clear] = ACTIONS(30),
    [anon_sym_exit] = ACTIONS(30),
    [anon_sym_quit] = ACTIONS(30),
    [anon_sym_reverse] = ACTIONS(30),
    [anon_sym_sample] = ACTIONS(30),
    [anon_sym_shuffle] = ACTIONS(30),
    [anon_sym_unique] = ACTIONS(30),
    [anon_sym_null_count] = ACTIONS(30),
    [anon_sym_glimpse] = ACTIONS(30),
    [anon_sym_size] = ACTIONS(30),
    [anon_sym_cast] = ACTIONS(30),
    [anon_sym_fill_null] = ACTIONS(30),
    [anon_sym_drop_null] = ACTIONS(30),
    [anon_sym_rename] = ACTIONS(30),
    [anon_sym_sum] = ACTIONS(30),
    [anon_sym_mean] = ACTIONS(30),
    [anon_sym_avg] = ACTIONS(30),
    [anon_sym_min] = ACTIONS(30),
    [anon_sym_max] = ACTIONS(30),
    [anon_sym_median] = ACTIONS(30),
    [anon_sym_std] = ACTIONS(30),
    [anon_sym_write] = ACTIONS(30),
    [anon_sym_with_row_index] = ACTIONS(30),
    [anon_sym_pwd] = ACTIONS(30),
    [anon_sym_ls] = ACTIONS(30),
    [anon_sym_cd] = ACTIONS(30),
    [anon_sym_sum_horizontal] = ACTIONS(30),
    [anon_sym_mean_horizontal] = ACTIONS(30),
    [anon_sym_min_horizontal] = ACTIONS(30),
    [anon_sym_max_horizontal] = ACTIONS(30),
    [anon_sym_all_horizontal] = ACTIONS(30),
    [anon_sym_any_horizontal] = ACTIONS(30),
    [anon_sym_sum_all] = ACTIONS(30),
    [anon_sym_mean_all] = ACTIONS(30),
    [anon_sym_min_all] = ACTIONS(30),
    [anon_sym_max_all] = ACTIONS(30),
    [anon_sym_std_all] = ACTIONS(30),
    [anon_sym_var_all] = ACTIONS(30),
    [anon_sym_median_all] = ACTIONS(30),
    [anon_sym_count_all] = ACTIONS(30),
    [anon_sym_null_count_all] = ACTIONS(30),
    [anon_sym_scan_csv] = ACTIONS(30),
    [anon_sym_scan_parquet] = ACTIONS(30),
    [anon_sym_scan_ipc] = ACTIONS(30),
    [anon_sym_scan_arrow] = ACTIONS(30),
    [anon_sym_scan_json] = ACTIONS(30),
    [anon_sym_scan_ndjson] = ACTIONS(30),
    [anon_sym_scan_jsonl] = ACTIONS(30),
    [anon_sym_scan_auto] = ACTIONS(30),
    [anon_sym_fill_nan] = ACTIONS(30),
    [anon_sym_forward_fill] = ACTIONS(30),
    [anon_sym_backward_fill] = ACTIONS(30),
    [anon_sym_top_k] = ACTIONS(30),
    [anon_sym_bottom_k] = ACTIONS(30),
    [anon_sym_transpose] = ACTIONS(30),
    [anon_sym_unpivot] = ACTIONS(30),
    [anon_sym_melt] = ACTIONS(30),
    [anon_sym_partition_by] = ACTIONS(30),
    [anon_sym_skew] = ACTIONS(30),
    [anon_sym_kurtosis] = ACTIONS(30),
    [anon_sym_approx_n_unique] = ACTIONS(30),
    [anon_sym_corr] = ACTIONS(30),
    [anon_sym_cov] = ACTIONS(30),
    [anon_sym_pivot] = ACTIONS(30),
    [anon_sym_help] = ACTIONS(30),
    [sym_identifier] = ACTIONS(30),
    [sym_comment] = ACTIONS(28),
    [sym_line_continuation] = ACTIONS(3),
    [sym__newline] = ACTIONS(28),
  },
  [5] = {
    [ts_builtin_sym_end] = ACTIONS(32),
    [anon_sym_DOT] = ACTIONS(32),
    [anon_sym_load] = ACTIONS(34),
    [anon_sym_save] = ACTIONS(34),
    [anon_sym_use] = ACTIONS(34),
    [anon_sym_stash] = ACTIONS(34),
    [anon_sym_frames] = ACTIONS(34),
    [anon_sym_drop_frame] = ACTIONS(34),
    [anon_sym_with] = ACTIONS(34),
    [anon_sym_unnest] = ACTIONS(34),
    [anon_sym_explode] = ACTIONS(34),
    [anon_sym_upsample] = ACTIONS(34),
    [anon_sym_tree] = ACTIONS(34),
    [anon_sym_graph] = ACTIONS(34),
    [anon_sym_show_graph] = ACTIONS(34),
    [anon_sym_mermaid] = ACTIONS(34),
    [anon_sym_explain_tree] = ACTIONS(34),
    [anon_sym_select] = ACTIONS(34),
    [anon_sym_drop] = ACTIONS(34),
    [anon_sym_filter] = ACTIONS(34),
    [anon_sym_sort] = ACTIONS(34),
    [anon_sym_limit] = ACTIONS(34),
    [anon_sym_head] = ACTIONS(34),
    [anon_sym_tail] = ACTIONS(34),
    [anon_sym_show] = ACTIONS(34),
    [anon_sym_schema] = ACTIONS(34),
    [anon_sym_describe] = ACTIONS(34),
    [anon_sym_groupby] = ACTIONS(34),
    [anon_sym_join] = ACTIONS(34),
    [anon_sym_explain] = ACTIONS(34),
    [anon_sym_collect] = ACTIONS(34),
    [anon_sym_reset] = ACTIONS(34),
    [anon_sym_source] = ACTIONS(34),
    [anon_sym_timing] = ACTIONS(34),
    [anon_sym_info] = ACTIONS(34),
    [anon_sym_clear] = ACTIONS(34),
    [anon_sym_exit] = ACTIONS(34),
    [anon_sym_quit] = ACTIONS(34),
    [anon_sym_reverse] = ACTIONS(34),
    [anon_sym_sample] = ACTIONS(34),
    [anon_sym_shuffle] = ACTIONS(34),
    [anon_sym_unique] = ACTIONS(34),
    [anon_sym_null_count] = ACTIONS(34),
    [anon_sym_glimpse] = ACTIONS(34),
    [anon_sym_size] = ACTIONS(34),
    [anon_sym_cast] = ACTIONS(34),
    [anon_sym_fill_null] = ACTIONS(34),
    [anon_sym_drop_null] = ACTIONS(34),
    [anon_sym_rename] = ACTIONS(34),
    [anon_sym_sum] = ACTIONS(34),
    [anon_sym_mean] = ACTIONS(34),
    [anon_sym_avg] = ACTIONS(34),
    [anon_sym_min] = ACTIONS(34),
    [anon_sym_max] = ACTIONS(34),
    [anon_sym_median] = ACTIONS(34),
    [anon_sym_std] = ACTIONS(34),
    [anon_sym_write] = ACTIONS(34),
    [anon_sym_with_row_index] = ACTIONS(34),
    [anon_sym_pwd] = ACTIONS(34),
    [anon_sym_ls] = ACTIONS(34),
    [anon_sym_cd] = ACTIONS(34),
    [anon_sym_sum_horizontal] = ACTIONS(34),
    [anon_sym_mean_horizontal] = ACTIONS(34),
    [anon_sym_min_horizontal] = ACTIONS(34),
    [anon_sym_max_horizontal] = ACTIONS(34),
    [anon_sym_all_horizontal] = ACTIONS(34),
    [anon_sym_any_horizontal] = ACTIONS(34),
    [anon_sym_sum_all] = ACTIONS(34),
    [anon_sym_mean_all] = ACTIONS(34),
    [anon_sym_min_all] = ACTIONS(34),
    [anon_sym_max_all] = ACTIONS(34),
    [anon_sym_std_all] = ACTIONS(34),
    [anon_sym_var_all] = ACTIONS(34),
    [anon_sym_median_all] = ACTIONS(34),
    [anon_sym_count_all] = ACTIONS(34),
    [anon_sym_null_count_all] = ACTIONS(34),
    [anon_sym_scan_csv] = ACTIONS(34),
    [anon_sym_scan_parquet] = ACTIONS(34),
    [anon_sym_scan_ipc] = ACTIONS(34),
    [anon_sym_scan_arrow] = ACTIONS(34),
    [anon_sym_scan_json] = ACTIONS(34),
    [anon_sym_scan_ndjson] = ACTIONS(34),
    [anon_sym_scan_jsonl] = ACTIONS(34),
    [anon_sym_scan_auto] = ACTIONS(34),
    [anon_sym_fill_nan] = ACTIONS(34),
    [anon_sym_forward_fill] = ACTIONS(34),
    [anon_sym_backward_fill] = ACTIONS(34),
    [anon_sym_top_k] = ACTIONS(34),
    [anon_sym_bottom_k] = ACTIONS(34),
    [anon_sym_transpose] = ACTIONS(34),
    [anon_sym_unpivot] = ACTIONS(34),
    [anon_sym_melt] = ACTIONS(34),
    [anon_sym_partition_by] = ACTIONS(34),
    [anon_sym_skew] = ACTIONS(34),
    [anon_sym_kurtosis] = ACTIONS(34),
    [anon_sym_approx_n_unique] = ACTIONS(34),
    [anon_sym_corr] = ACTIONS(34),
    [anon_sym_cov] = ACTIONS(34),
    [anon_sym_pivot] = ACTIONS(34),
    [anon_sym_help] = ACTIONS(34),
    [sym_identifier] = ACTIONS(34),
    [sym_comment] = ACTIONS(32),
    [sym_line_continuation] = ACTIONS(3),
    [sym__newline] = ACTIONS(32),
  },
  [6] = {
    [ts_builtin_sym_end] = ACTIONS(36),
    [anon_sym_DOT] = ACTIONS(36),
    [anon_sym_load] = ACTIONS(38),
    [anon_sym_save] = ACTIONS(38),
    [anon_sym_use] = ACTIONS(38),
    [anon_sym_stash] = ACTIONS(38),
    [anon_sym_frames] = ACTIONS(38),
    [anon_sym_drop_frame] = ACTIONS(38),
    [anon_sym_with] = ACTIONS(38),
    [anon_sym_unnest] = ACTIONS(38),
    [anon_sym_explode] = ACTIONS(38),
    [anon_sym_upsample] = ACTIONS(38),
    [anon_sym_tree] = ACTIONS(38),
    [anon_sym_graph] = ACTIONS(38),
    [anon_sym_show_graph] = ACTIONS(38),
    [anon_sym_mermaid] = ACTIONS(38),
    [anon_sym_explain_tree] = ACTIONS(38),
    [anon_sym_select] = ACTIONS(38),
    [anon_sym_drop] = ACTIONS(38),
    [anon_sym_filter] = ACTIONS(38),
    [anon_sym_sort] = ACTIONS(38),
    [anon_sym_limit] = ACTIONS(38),
    [anon_sym_head] = ACTIONS(38),
    [anon_sym_tail] = ACTIONS(38),
    [anon_sym_show] = ACTIONS(38),
    [anon_sym_schema] = ACTIONS(38),
    [anon_sym_describe] = ACTIONS(38),
    [anon_sym_groupby] = ACTIONS(38),
    [anon_sym_join] = ACTIONS(38),
    [anon_sym_explain] = ACTIONS(38),
    [anon_sym_collect] = ACTIONS(38),
    [anon_sym_reset] = ACTIONS(38),
    [anon_sym_source] = ACTIONS(38),
    [anon_sym_timing] = ACTIONS(38),
    [anon_sym_info] = ACTIONS(38),
    [anon_sym_clear] = ACTIONS(38),
    [anon_sym_exit] = ACTIONS(38),
    [anon_sym_quit] = ACTIONS(38),
    [anon_sym_reverse] = ACTIONS(38),
    [anon_sym_sample] = ACTIONS(38),
    [anon_sym_shuffle] = ACTIONS(38),
    [anon_sym_unique] = ACTIONS(38),
    [anon_sym_null_count] = ACTIONS(38),
    [anon_sym_glimpse] = ACTIONS(38),
    [anon_sym_size] = ACTIONS(38),
    [anon_sym_cast] = ACTIONS(38),
    [anon_sym_fill_null] = ACTIONS(38),
    [anon_sym_drop_null] = ACTIONS(38),
    [anon_sym_rename] = ACTIONS(38),
    [anon_sym_sum] = ACTIONS(38),
    [anon_sym_mean] = ACTIONS(38),
    [anon_sym_avg] = ACTIONS(38),
    [anon_sym_min] = ACTIONS(38),
    [anon_sym_max] = ACTIONS(38),
    [anon_sym_median] = ACTIONS(38),
    [anon_sym_std] = ACTIONS(38),
    [anon_sym_write] = ACTIONS(38),
    [anon_sym_with_row_index] = ACTIONS(38),
    [anon_sym_pwd] = ACTIONS(38),
    [anon_sym_ls] = ACTIONS(38),
    [anon_sym_cd] = ACTIONS(38),
    [anon_sym_sum_horizontal] = ACTIONS(38),
    [anon_sym_mean_horizontal] = ACTIONS(38),
    [anon_sym_min_horizontal] = ACTIONS(38),
    [anon_sym_max_horizontal] = ACTIONS(38),
    [anon_sym_all_horizontal] = ACTIONS(38),
    [anon_sym_any_horizontal] = ACTIONS(38),
    [anon_sym_sum_all] = ACTIONS(38),
    [anon_sym_mean_all] = ACTIONS(38),
    [anon_sym_min_all] = ACTIONS(38),
    [anon_sym_max_all] = ACTIONS(38),
    [anon_sym_std_all] = ACTIONS(38),
    [anon_sym_var_all] = ACTIONS(38),
    [anon_sym_median_all] = ACTIONS(38),
    [anon_sym_count_all] = ACTIONS(38),
    [anon_sym_null_count_all] = ACTIONS(38),
    [anon_sym_scan_csv] = ACTIONS(38),
    [anon_sym_scan_parquet] = ACTIONS(38),
    [anon_sym_scan_ipc] = ACTIONS(38),
    [anon_sym_scan_arrow] = ACTIONS(38),
    [anon_sym_scan_json] = ACTIONS(38),
    [anon_sym_scan_ndjson] = ACTIONS(38),
    [anon_sym_scan_jsonl] = ACTIONS(38),
    [anon_sym_scan_auto] = ACTIONS(38),
    [anon_sym_fill_nan] = ACTIONS(38),
    [anon_sym_forward_fill] = ACTIONS(38),
    [anon_sym_backward_fill] = ACTIONS(38),
    [anon_sym_top_k] = ACTIONS(38),
    [anon_sym_bottom_k] = ACTIONS(38),
    [anon_sym_transpose] = ACTIONS(38),
    [anon_sym_unpivot] = ACTIONS(38),
    [anon_sym_melt] = ACTIONS(38),
    [anon_sym_partition_by] = ACTIONS(38),
    [anon_sym_skew] = ACTIONS(38),
    [anon_sym_kurtosis] = ACTIONS(38),
    [anon_sym_approx_n_unique] = ACTIONS(38),
    [anon_sym_corr] = ACTIONS(38),
    [anon_sym_cov] = ACTIONS(38),
    [anon_sym_pivot] = ACTIONS(38),
    [anon_sym_help] = ACTIONS(38),
    [sym_identifier] = ACTIONS(38),
    [sym_comment] = ACTIONS(36),
    [sym_line_continuation] = ACTIONS(3),
    [sym__newline] = ACTIONS(36),
  },
  [7] = {
    [ts_builtin_sym_end] = ACTIONS(40),
    [anon_sym_DOT] = ACTIONS(40),
    [anon_sym_load] = ACTIONS(42),
    [anon_sym_save] = ACTIONS(42),
    [anon_sym_use] = ACTIONS(42),
    [anon_sym_stash] = ACTIONS(42),
    [anon_sym_frames] = ACTIONS(42),
    [anon_sym_drop_frame] = ACTIONS(42),
    [anon_sym_with] = ACTIONS(42),
    [anon_sym_unnest] = ACTIONS(42),
    [anon_sym_explode] = ACTIONS(42),
    [anon_sym_upsample] = ACTIONS(42),
    [anon_sym_tree] = ACTIONS(42),
    [anon_sym_graph] = ACTIONS(42),
    [anon_sym_show_graph] = ACTIONS(42),
    [anon_sym_mermaid] = ACTIONS(42),
    [anon_sym_explain_tree] = ACTIONS(42),
    [anon_sym_select] = ACTIONS(42),
    [anon_sym_drop] = ACTIONS(42),
    [anon_sym_filter] = ACTIONS(42),
    [anon_sym_sort] = ACTIONS(42),
    [anon_sym_limit] = ACTIONS(42),
    [anon_sym_head] = ACTIONS(42),
    [anon_sym_tail] = ACTIONS(42),
    [anon_sym_show] = ACTIONS(42),
    [anon_sym_schema] = ACTIONS(42),
    [anon_sym_describe] = ACTIONS(42),
    [anon_sym_groupby] = ACTIONS(42),
    [anon_sym_join] = ACTIONS(42),
    [anon_sym_explain] = ACTIONS(42),
    [anon_sym_collect] = ACTIONS(42),
    [anon_sym_reset] = ACTIONS(42),
    [anon_sym_source] = ACTIONS(42),
    [anon_sym_timing] = ACTIONS(42),
    [anon_sym_info] = ACTIONS(42),
    [anon_sym_clear] = ACTIONS(42),
    [anon_sym_exit] = ACTIONS(42),
    [anon_sym_quit] = ACTIONS(42),
    [anon_sym_reverse] = ACTIONS(42),
    [anon_sym_sample] = ACTIONS(42),
    [anon_sym_shuffle] = ACTIONS(42),
    [anon_sym_unique] = ACTIONS(42),
    [anon_sym_null_count] = ACTIONS(42),
    [anon_sym_glimpse] = ACTIONS(42),
    [anon_sym_size] = ACTIONS(42),
    [anon_sym_cast] = ACTIONS(42),
    [anon_sym_fill_null] = ACTIONS(42),
    [anon_sym_drop_null] = ACTIONS(42),
    [anon_sym_rename] = ACTIONS(42),
    [anon_sym_sum] = ACTIONS(42),
    [anon_sym_mean] = ACTIONS(42),
    [anon_sym_avg] = ACTIONS(42),
    [anon_sym_min] = ACTIONS(42),
    [anon_sym_max] = ACTIONS(42),
    [anon_sym_median] = ACTIONS(42),
    [anon_sym_std] = ACTIONS(42),
    [anon_sym_write] = ACTIONS(42),
    [anon_sym_with_row_index] = ACTIONS(42),
    [anon_sym_pwd] = ACTIONS(42),
    [anon_sym_ls] = ACTIONS(42),
    [anon_sym_cd] = ACTIONS(42),
    [anon_sym_sum_horizontal] = ACTIONS(42),
    [anon_sym_mean_horizontal] = ACTIONS(42),
    [anon_sym_min_horizontal] = ACTIONS(42),
    [anon_sym_max_horizontal] = ACTIONS(42),
    [anon_sym_all_horizontal] = ACTIONS(42),
    [anon_sym_any_horizontal] = ACTIONS(42),
    [anon_sym_sum_all] = ACTIONS(42),
    [anon_sym_mean_all] = ACTIONS(42),
    [anon_sym_min_all] = ACTIONS(42),
    [anon_sym_max_all] = ACTIONS(42),
    [anon_sym_std_all] = ACTIONS(42),
    [anon_sym_var_all] = ACTIONS(42),
    [anon_sym_median_all] = ACTIONS(42),
    [anon_sym_count_all] = ACTIONS(42),
    [anon_sym_null_count_all] = ACTIONS(42),
    [anon_sym_scan_csv] = ACTIONS(42),
    [anon_sym_scan_parquet] = ACTIONS(42),
    [anon_sym_scan_ipc] = ACTIONS(42),
    [anon_sym_scan_arrow] = ACTIONS(42),
    [anon_sym_scan_json] = ACTIONS(42),
    [anon_sym_scan_ndjson] = ACTIONS(42),
    [anon_sym_scan_jsonl] = ACTIONS(42),
    [anon_sym_scan_auto] = ACTIONS(42),
    [anon_sym_fill_nan] = ACTIONS(42),
    [anon_sym_forward_fill] = ACTIONS(42),
    [anon_sym_backward_fill] = ACTIONS(42),
    [anon_sym_top_k] = ACTIONS(42),
    [anon_sym_bottom_k] = ACTIONS(42),
    [anon_sym_transpose] = ACTIONS(42),
    [anon_sym_unpivot] = ACTIONS(42),
    [anon_sym_melt] = ACTIONS(42),
    [anon_sym_partition_by] = ACTIONS(42),
    [anon_sym_skew] = ACTIONS(42),
    [anon_sym_kurtosis] = ACTIONS(42),
    [anon_sym_approx_n_unique] = ACTIONS(42),
    [anon_sym_corr] = ACTIONS(42),
    [anon_sym_cov] = ACTIONS(42),
    [anon_sym_pivot] = ACTIONS(42),
    [anon_sym_help] = ACTIONS(42),
    [sym_identifier] = ACTIONS(42),
    [sym_comment] = ACTIONS(40),
    [sym_line_continuation] = ACTIONS(3),
    [sym__newline] = ACTIONS(40),
  },
  [8] = {
    [sym_command] = STATE(10),
    [anon_sym_load] = ACTIONS(9),
    [anon_sym_save] = ACTIONS(9),
    [anon_sym_use] = ACTIONS(9),
    [anon_sym_stash] = ACTIONS(9),
    [anon_sym_frames] = ACTIONS(9),
    [anon_sym_drop_frame] = ACTIONS(9),
    [anon_sym_with] = ACTIONS(9),
    [anon_sym_unnest] = ACTIONS(9),
    [anon_sym_explode] = ACTIONS(9),
    [anon_sym_upsample] = ACTIONS(9),
    [anon_sym_tree] = ACTIONS(9),
    [anon_sym_graph] = ACTIONS(9),
    [anon_sym_show_graph] = ACTIONS(9),
    [anon_sym_mermaid] = ACTIONS(9),
    [anon_sym_explain_tree] = ACTIONS(9),
    [anon_sym_select] = ACTIONS(9),
    [anon_sym_drop] = ACTIONS(9),
    [anon_sym_filter] = ACTIONS(9),
    [anon_sym_sort] = ACTIONS(9),
    [anon_sym_limit] = ACTIONS(9),
    [anon_sym_head] = ACTIONS(9),
    [anon_sym_tail] = ACTIONS(9),
    [anon_sym_show] = ACTIONS(9),
    [anon_sym_schema] = ACTIONS(9),
    [anon_sym_describe] = ACTIONS(9),
    [anon_sym_groupby] = ACTIONS(9),
    [anon_sym_join] = ACTIONS(9),
    [anon_sym_explain] = ACTIONS(9),
    [anon_sym_collect] = ACTIONS(9),
    [anon_sym_reset] = ACTIONS(9),
    [anon_sym_source] = ACTIONS(9),
    [anon_sym_timing] = ACTIONS(9),
    [anon_sym_info] = ACTIONS(9),
    [anon_sym_clear] = ACTIONS(9),
    [anon_sym_exit] = ACTIONS(9),
    [anon_sym_quit] = ACTIONS(9),
    [anon_sym_reverse] = ACTIONS(9),
    [anon_sym_sample] = ACTIONS(9),
    [anon_sym_shuffle] = ACTIONS(9),
    [anon_sym_unique] = ACTIONS(9),
    [anon_sym_null_count] = ACTIONS(9),
    [anon_sym_glimpse] = ACTIONS(9),
    [anon_sym_size] = ACTIONS(9),
    [anon_sym_cast] = ACTIONS(9),
    [anon_sym_fill_null] = ACTIONS(9),
    [anon_sym_drop_null] = ACTIONS(9),
    [anon_sym_rename] = ACTIONS(9),
    [anon_sym_sum] = ACTIONS(9),
    [anon_sym_mean] = ACTIONS(9),
    [anon_sym_avg] = ACTIONS(9),
    [anon_sym_min] = ACTIONS(9),
    [anon_sym_max] = ACTIONS(9),
    [anon_sym_median] = ACTIONS(9),
    [anon_sym_std] = ACTIONS(9),
    [anon_sym_write] = ACTIONS(9),
    [anon_sym_with_row_index] = ACTIONS(9),
    [anon_sym_pwd] = ACTIONS(9),
    [anon_sym_ls] = ACTIONS(9),
    [anon_sym_cd] = ACTIONS(9),
    [anon_sym_sum_horizontal] = ACTIONS(9),
    [anon_sym_mean_horizontal] = ACTIONS(9),
    [anon_sym_min_horizontal] = ACTIONS(9),
    [anon_sym_max_horizontal] = ACTIONS(9),
    [anon_sym_all_horizontal] = ACTIONS(9),
    [anon_sym_any_horizontal] = ACTIONS(9),
    [anon_sym_sum_all] = ACTIONS(9),
    [anon_sym_mean_all] = ACTIONS(9),
    [anon_sym_min_all] = ACTIONS(9),
    [anon_sym_max_all] = ACTIONS(9),
    [anon_sym_std_all] = ACTIONS(9),
    [anon_sym_var_all] = ACTIONS(9),
    [anon_sym_median_all] = ACTIONS(9),
    [anon_sym_count_all] = ACTIONS(9),
    [anon_sym_null_count_all] = ACTIONS(9),
    [anon_sym_scan_csv] = ACTIONS(9),
    [anon_sym_scan_parquet] = ACTIONS(9),
    [anon_sym_scan_ipc] = ACTIONS(9),
    [anon_sym_scan_arrow] = ACTIONS(9),
    [anon_sym_scan_json] = ACTIONS(9),
    [anon_sym_scan_ndjson] = ACTIONS(9),
    [anon_sym_scan_jsonl] = ACTIONS(9),
    [anon_sym_scan_auto] = ACTIONS(9),
    [anon_sym_fill_nan] = ACTIONS(9),
    [anon_sym_forward_fill] = ACTIONS(9),
    [anon_sym_backward_fill] = ACTIONS(9),
    [anon_sym_top_k] = ACTIONS(9),
    [anon_sym_bottom_k] = ACTIONS(9),
    [anon_sym_transpose] = ACTIONS(9),
    [anon_sym_unpivot] = ACTIONS(9),
    [anon_sym_melt] = ACTIONS(9),
    [anon_sym_partition_by] = ACTIONS(9),
    [anon_sym_skew] = ACTIONS(9),
    [anon_sym_kurtosis] = ACTIONS(9),
    [anon_sym_approx_n_unique] = ACTIONS(9),
    [anon_sym_corr] = ACTIONS(9),
    [anon_sym_cov] = ACTIONS(9),
    [anon_sym_pivot] = ACTIONS(9),
    [anon_sym_help] = ACTIONS(9),
    [sym_identifier] = ACTIONS(9),
    [sym_line_continuation] = ACTIONS(3),
  },
};

static const uint16_t ts_small_parse_table[] = {
  [0] = 16,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(48), 1,
      anon_sym_LPAREN,
    ACTIONS(50), 1,
      anon_sym_DASH,
    ACTIONS(58), 1,
      anon_sym_DQUOTE,
    ACTIONS(60), 1,
      sym_number,
    ACTIONS(64), 1,
      sym_identifier,
    ACTIONS(66), 1,
      sym__newline,
    ACTIONS(46), 2,
      anon_sym_EQ,
      anon_sym_BANG,
    ACTIONS(56), 2,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(62), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(16), 2,
      sym__expr_operand,
      sym_method_call,
    STATE(21), 2,
      sym_string,
      sym_boolean,
    ACTIONS(54), 4,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
    STATE(11), 5,
      sym__arg,
      sym_expr,
      sym_keyword,
      sym_operator,
      aux_sym_statement_repeat1,
    ACTIONS(44), 10,
      anon_sym_DOT,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      sym_agg_spec,
    ACTIONS(52), 11,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
  [80] = 16,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(48), 1,
      anon_sym_LPAREN,
    ACTIONS(50), 1,
      anon_sym_DASH,
    ACTIONS(58), 1,
      anon_sym_DQUOTE,
    ACTIONS(60), 1,
      sym_number,
    ACTIONS(64), 1,
      sym_identifier,
    ACTIONS(72), 1,
      sym__newline,
    ACTIONS(56), 2,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(62), 2,
      anon_sym_true,
      anon_sym_false,
    ACTIONS(70), 2,
      anon_sym_EQ,
      anon_sym_BANG,
    STATE(16), 2,
      sym__expr_operand,
      sym_method_call,
    STATE(21), 2,
      sym_string,
      sym_boolean,
    ACTIONS(54), 4,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
    STATE(12), 5,
      sym__arg,
      sym_expr,
      sym_keyword,
      sym_operator,
      aux_sym_statement_repeat1,
    ACTIONS(68), 10,
      anon_sym_DOT,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      sym_agg_spec,
    ACTIONS(52), 11,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
  [160] = 16,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(48), 1,
      anon_sym_LPAREN,
    ACTIONS(50), 1,
      anon_sym_DASH,
    ACTIONS(58), 1,
      anon_sym_DQUOTE,
    ACTIONS(60), 1,
      sym_number,
    ACTIONS(64), 1,
      sym_identifier,
    ACTIONS(78), 1,
      sym__newline,
    ACTIONS(56), 2,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(62), 2,
      anon_sym_true,
      anon_sym_false,
    ACTIONS(76), 2,
      anon_sym_EQ,
      anon_sym_BANG,
    STATE(16), 2,
      sym__expr_operand,
      sym_method_call,
    STATE(21), 2,
      sym_string,
      sym_boolean,
    ACTIONS(54), 4,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
    STATE(13), 5,
      sym__arg,
      sym_expr,
      sym_keyword,
      sym_operator,
      aux_sym_statement_repeat1,
    ACTIONS(74), 10,
      anon_sym_DOT,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      sym_agg_spec,
    ACTIONS(52), 11,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
  [240] = 16,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(48), 1,
      anon_sym_LPAREN,
    ACTIONS(50), 1,
      anon_sym_DASH,
    ACTIONS(58), 1,
      anon_sym_DQUOTE,
    ACTIONS(60), 1,
      sym_number,
    ACTIONS(64), 1,
      sym_identifier,
    ACTIONS(80), 1,
      sym__newline,
    ACTIONS(56), 2,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(62), 2,
      anon_sym_true,
      anon_sym_false,
    ACTIONS(76), 2,
      anon_sym_EQ,
      anon_sym_BANG,
    STATE(16), 2,
      sym__expr_operand,
      sym_method_call,
    STATE(21), 2,
      sym_string,
      sym_boolean,
    ACTIONS(54), 4,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
    STATE(13), 5,
      sym__arg,
      sym_expr,
      sym_keyword,
      sym_operator,
      aux_sym_statement_repeat1,
    ACTIONS(74), 10,
      anon_sym_DOT,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      sym_agg_spec,
    ACTIONS(52), 11,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
  [320] = 16,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(88), 1,
      anon_sym_LPAREN,
    ACTIONS(91), 1,
      anon_sym_DASH,
    ACTIONS(103), 1,
      anon_sym_DQUOTE,
    ACTIONS(106), 1,
      sym_number,
    ACTIONS(112), 1,
      sym_identifier,
    ACTIONS(115), 1,
      sym__newline,
    ACTIONS(85), 2,
      anon_sym_EQ,
      anon_sym_BANG,
    ACTIONS(100), 2,
      anon_sym_LT,
      anon_sym_GT,
    ACTIONS(109), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(16), 2,
      sym__expr_operand,
      sym_method_call,
    STATE(21), 2,
      sym_string,
      sym_boolean,
    ACTIONS(97), 4,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
    STATE(13), 5,
      sym__arg,
      sym_expr,
      sym_keyword,
      sym_operator,
      aux_sym_statement_repeat1,
    ACTIONS(82), 10,
      anon_sym_DOT,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      sym_agg_spec,
    ACTIONS(94), 11,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
  [400] = 11,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(121), 1,
      anon_sym_LPAREN,
    ACTIONS(124), 1,
      anon_sym_DASH,
    ACTIONS(127), 1,
      anon_sym_DQUOTE,
    ACTIONS(130), 1,
      sym_number,
    ACTIONS(136), 1,
      sym_identifier,
    STATE(83), 1,
      sym_expr,
    ACTIONS(133), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(48), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
    ACTIONS(117), 15,
      anon_sym_DOT,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      sym__newline,
    ACTIONS(119), 15,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
  [466] = 6,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(139), 1,
      anon_sym_DOT,
    ACTIONS(146), 1,
      anon_sym_LPAREN,
    STATE(72), 1,
      aux_sym_method_call_repeat1,
    ACTIONS(150), 16,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(143), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [518] = 6,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(159), 1,
      anon_sym_DASH,
    STATE(18), 1,
      aux_sym_expr_repeat1,
    ACTIONS(157), 4,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
    ACTIONS(153), 14,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(155), 18,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [570] = 6,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(168), 1,
      anon_sym_DASH,
    STATE(17), 1,
      aux_sym_expr_repeat1,
    ACTIONS(165), 4,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
    ACTIONS(161), 14,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(163), 18,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [622] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    STATE(17), 1,
      aux_sym_expr_repeat1,
    ACTIONS(171), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(173), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [670] = 6,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(175), 1,
      anon_sym_DOT,
    ACTIONS(180), 1,
      anon_sym_LPAREN,
    STATE(72), 1,
      aux_sym_method_call_repeat1,
    ACTIONS(183), 16,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(178), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [722] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(185), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(187), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [767] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(150), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(143), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [812] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(189), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(191), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [857] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(193), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(195), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [902] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(197), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(199), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [947] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(201), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(203), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [992] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(205), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(207), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [1037] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(209), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(211), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [1082] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(213), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(215), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [1127] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(217), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(219), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [1172] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(161), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(163), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [1217] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(221), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(223), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [1262] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(225), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(227), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [1307] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(229), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(231), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [1352] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(233), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(235), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [1397] = 3,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(237), 18,
      anon_sym_DOT,
      anon_sym_LPAREN,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_AMP,
      anon_sym_PIPE,
      anon_sym_EQ_EQ,
      anon_sym_BANG_EQ,
      anon_sym_LT_EQ,
      anon_sym_GT_EQ,
      sym_agg_spec,
      anon_sym_DQUOTE,
      sym_number,
      sym__newline,
    ACTIONS(239), 19,
      anon_sym_EQ,
      anon_sym_BANG,
      anon_sym_DASH,
      anon_sym_as,
      anon_sym_on,
      anon_sym_asc,
      anon_sym_desc,
      anon_sym_and,
      anon_sym_or,
      anon_sym_is_null,
      anon_sym_is_not_null,
      anon_sym_inner,
      anon_sym_left,
      anon_sym_cross,
      anon_sym_LT,
      anon_sym_GT,
      anon_sym_true,
      anon_sym_false,
      sym_identifier,
  [1442] = 10,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(241), 1,
      anon_sym_LPAREN,
    ACTIONS(243), 1,
      anon_sym_RPAREN,
    ACTIONS(245), 1,
      anon_sym_DASH,
    ACTIONS(247), 1,
      anon_sym_DQUOTE,
    ACTIONS(249), 1,
      sym_number,
    ACTIONS(253), 1,
      sym_identifier,
    STATE(70), 1,
      sym_expr,
    ACTIONS(251), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(48), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1477] = 10,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(241), 1,
      anon_sym_LPAREN,
    ACTIONS(245), 1,
      anon_sym_DASH,
    ACTIONS(247), 1,
      anon_sym_DQUOTE,
    ACTIONS(249), 1,
      sym_number,
    ACTIONS(253), 1,
      sym_identifier,
    ACTIONS(255), 1,
      anon_sym_RPAREN,
    STATE(79), 1,
      sym_expr,
    ACTIONS(251), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(48), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1512] = 10,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(241), 1,
      anon_sym_LPAREN,
    ACTIONS(245), 1,
      anon_sym_DASH,
    ACTIONS(247), 1,
      anon_sym_DQUOTE,
    ACTIONS(249), 1,
      sym_number,
    ACTIONS(253), 1,
      sym_identifier,
    ACTIONS(257), 1,
      anon_sym_RPAREN,
    STATE(75), 1,
      sym_expr,
    ACTIONS(251), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(48), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1547] = 10,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(241), 1,
      anon_sym_LPAREN,
    ACTIONS(245), 1,
      anon_sym_DASH,
    ACTIONS(247), 1,
      anon_sym_DQUOTE,
    ACTIONS(249), 1,
      sym_number,
    ACTIONS(253), 1,
      sym_identifier,
    ACTIONS(259), 1,
      anon_sym_RPAREN,
    STATE(68), 1,
      sym_expr,
    ACTIONS(251), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(48), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1582] = 9,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(241), 1,
      anon_sym_LPAREN,
    ACTIONS(245), 1,
      anon_sym_DASH,
    ACTIONS(247), 1,
      anon_sym_DQUOTE,
    ACTIONS(249), 1,
      sym_number,
    ACTIONS(253), 1,
      sym_identifier,
    STATE(86), 1,
      sym_expr,
    ACTIONS(251), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(48), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1614] = 9,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(241), 1,
      anon_sym_LPAREN,
    ACTIONS(245), 1,
      anon_sym_DASH,
    ACTIONS(247), 1,
      anon_sym_DQUOTE,
    ACTIONS(249), 1,
      sym_number,
    ACTIONS(253), 1,
      sym_identifier,
    STATE(81), 1,
      sym_expr,
    ACTIONS(251), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(48), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1646] = 9,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(241), 1,
      anon_sym_LPAREN,
    ACTIONS(245), 1,
      anon_sym_DASH,
    ACTIONS(247), 1,
      anon_sym_DQUOTE,
    ACTIONS(249), 1,
      sym_number,
    ACTIONS(253), 1,
      sym_identifier,
    STATE(83), 1,
      sym_expr,
    ACTIONS(251), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(48), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1678] = 8,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(241), 1,
      anon_sym_LPAREN,
    ACTIONS(245), 1,
      anon_sym_DASH,
    ACTIONS(247), 1,
      anon_sym_DQUOTE,
    ACTIONS(253), 1,
      sym_identifier,
    ACTIONS(261), 1,
      sym_number,
    ACTIONS(251), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(62), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1707] = 8,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(50), 1,
      anon_sym_DASH,
    ACTIONS(58), 1,
      anon_sym_DQUOTE,
    ACTIONS(263), 1,
      anon_sym_LPAREN,
    ACTIONS(265), 1,
      sym_number,
    ACTIONS(267), 1,
      sym_identifier,
    ACTIONS(62), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(22), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1736] = 8,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(50), 1,
      anon_sym_DASH,
    ACTIONS(58), 1,
      anon_sym_DQUOTE,
    ACTIONS(263), 1,
      anon_sym_LPAREN,
    ACTIONS(267), 1,
      sym_identifier,
    ACTIONS(269), 1,
      sym_number,
    ACTIONS(62), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(30), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1765] = 8,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(241), 1,
      anon_sym_LPAREN,
    ACTIONS(245), 1,
      anon_sym_DASH,
    ACTIONS(247), 1,
      anon_sym_DQUOTE,
    ACTIONS(253), 1,
      sym_identifier,
    ACTIONS(271), 1,
      sym_number,
    ACTIONS(251), 2,
      anon_sym_true,
      anon_sym_false,
    STATE(54), 4,
      sym__expr_operand,
      sym_method_call,
      sym_string,
      sym_boolean,
  [1794] = 5,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(273), 1,
      anon_sym_DOT,
    ACTIONS(275), 1,
      anon_sym_LPAREN,
    STATE(80), 1,
      aux_sym_method_call_repeat1,
    ACTIONS(183), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1816] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    STATE(49), 1,
      aux_sym_expr_repeat1,
    ACTIONS(153), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(277), 5,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1834] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    STATE(50), 1,
      aux_sym_expr_repeat1,
    ACTIONS(171), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(277), 5,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1852] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    STATE(50), 1,
      aux_sym_expr_repeat1,
    ACTIONS(161), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
    ACTIONS(279), 5,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1870] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(201), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1883] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(209), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1896] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(213), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1909] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(189), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1922] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(193), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1935] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(197), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1948] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(185), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1961] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(225), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1974] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(237), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [1987] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(233), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [2000] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(229), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [2013] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(161), 7,
      anon_sym_RPAREN,
      anon_sym_COMMA,
      anon_sym_SLASH,
      anon_sym_PERCENT,
      anon_sym_PLUS,
      anon_sym_STAR,
      anon_sym_DASH,
  [2026] = 5,
    ACTIONS(282), 1,
      anon_sym_DQUOTE,
    ACTIONS(284), 1,
      aux_sym_string_token1,
    ACTIONS(287), 1,
      anon_sym_BSLASH,
    ACTIONS(290), 1,
      sym_line_continuation,
    STATE(63), 1,
      aux_sym_string_repeat1,
  [2042] = 5,
    ACTIONS(290), 1,
      sym_line_continuation,
    ACTIONS(292), 1,
      anon_sym_DQUOTE,
    ACTIONS(294), 1,
      aux_sym_string_token1,
    ACTIONS(296), 1,
      anon_sym_BSLASH,
    STATE(67), 1,
      aux_sym_string_repeat1,
  [2058] = 5,
    ACTIONS(290), 1,
      sym_line_continuation,
    ACTIONS(296), 1,
      anon_sym_BSLASH,
    ACTIONS(298), 1,
      anon_sym_DQUOTE,
    ACTIONS(300), 1,
      aux_sym_string_token1,
    STATE(63), 1,
      aux_sym_string_repeat1,
  [2074] = 5,
    ACTIONS(290), 1,
      sym_line_continuation,
    ACTIONS(296), 1,
      anon_sym_BSLASH,
    ACTIONS(302), 1,
      anon_sym_DQUOTE,
    ACTIONS(304), 1,
      aux_sym_string_token1,
    STATE(65), 1,
      aux_sym_string_repeat1,
  [2090] = 5,
    ACTIONS(290), 1,
      sym_line_continuation,
    ACTIONS(296), 1,
      anon_sym_BSLASH,
    ACTIONS(300), 1,
      aux_sym_string_token1,
    ACTIONS(306), 1,
      anon_sym_DQUOTE,
    STATE(63), 1,
      aux_sym_string_repeat1,
  [2106] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(308), 1,
      anon_sym_RPAREN,
    ACTIONS(310), 1,
      anon_sym_COMMA,
    STATE(78), 1,
      aux_sym_method_call_repeat2,
  [2119] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(312), 1,
      anon_sym_RPAREN,
    ACTIONS(314), 1,
      anon_sym_COMMA,
    STATE(69), 1,
      aux_sym_method_call_repeat2,
  [2132] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(310), 1,
      anon_sym_COMMA,
    ACTIONS(317), 1,
      anon_sym_RPAREN,
    STATE(76), 1,
      aux_sym_method_call_repeat2,
  [2145] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(310), 1,
      anon_sym_COMMA,
    ACTIONS(319), 1,
      anon_sym_RPAREN,
    STATE(69), 1,
      aux_sym_method_call_repeat2,
  [2158] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(273), 1,
      anon_sym_DOT,
    ACTIONS(321), 1,
      anon_sym_LPAREN,
    STATE(73), 1,
      aux_sym_method_call_repeat1,
  [2171] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(323), 1,
      anon_sym_DOT,
    ACTIONS(326), 1,
      anon_sym_LPAREN,
    STATE(73), 1,
      aux_sym_method_call_repeat1,
  [2184] = 3,
    ACTIONS(290), 1,
      sym_line_continuation,
    ACTIONS(328), 1,
      aux_sym_string_token1,
    ACTIONS(282), 2,
      anon_sym_DQUOTE,
      anon_sym_BSLASH,
  [2195] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(310), 1,
      anon_sym_COMMA,
    ACTIONS(330), 1,
      anon_sym_RPAREN,
    STATE(77), 1,
      aux_sym_method_call_repeat2,
  [2208] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(310), 1,
      anon_sym_COMMA,
    ACTIONS(332), 1,
      anon_sym_RPAREN,
    STATE(69), 1,
      aux_sym_method_call_repeat2,
  [2221] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(310), 1,
      anon_sym_COMMA,
    ACTIONS(334), 1,
      anon_sym_RPAREN,
    STATE(69), 1,
      aux_sym_method_call_repeat2,
  [2234] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(310), 1,
      anon_sym_COMMA,
    ACTIONS(336), 1,
      anon_sym_RPAREN,
    STATE(69), 1,
      aux_sym_method_call_repeat2,
  [2247] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(310), 1,
      anon_sym_COMMA,
    ACTIONS(338), 1,
      anon_sym_RPAREN,
    STATE(71), 1,
      aux_sym_method_call_repeat2,
  [2260] = 4,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(273), 1,
      anon_sym_DOT,
    ACTIONS(340), 1,
      anon_sym_LPAREN,
    STATE(73), 1,
      aux_sym_method_call_repeat1,
  [2273] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(312), 2,
      anon_sym_RPAREN,
      anon_sym_COMMA,
  [2281] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(342), 2,
      anon_sym_DOT,
      anon_sym_LPAREN,
  [2289] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(344), 1,
      anon_sym_RPAREN,
  [2296] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(346), 1,
      sym_identifier,
  [2303] = 2,
    ACTIONS(290), 1,
      sym_line_continuation,
    ACTIONS(348), 1,
      aux_sym_string_token2,
  [2310] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(350), 1,
      anon_sym_RPAREN,
  [2317] = 2,
    ACTIONS(3), 1,
      sym_line_continuation,
    ACTIONS(352), 1,
      ts_builtin_sym_end,
};

static const uint32_t ts_small_parse_table_map[] = {
  [SMALL_STATE(9)] = 0,
  [SMALL_STATE(10)] = 80,
  [SMALL_STATE(11)] = 160,
  [SMALL_STATE(12)] = 240,
  [SMALL_STATE(13)] = 320,
  [SMALL_STATE(14)] = 400,
  [SMALL_STATE(15)] = 466,
  [SMALL_STATE(16)] = 518,
  [SMALL_STATE(17)] = 570,
  [SMALL_STATE(18)] = 622,
  [SMALL_STATE(19)] = 670,
  [SMALL_STATE(20)] = 722,
  [SMALL_STATE(21)] = 767,
  [SMALL_STATE(22)] = 812,
  [SMALL_STATE(23)] = 857,
  [SMALL_STATE(24)] = 902,
  [SMALL_STATE(25)] = 947,
  [SMALL_STATE(26)] = 992,
  [SMALL_STATE(27)] = 1037,
  [SMALL_STATE(28)] = 1082,
  [SMALL_STATE(29)] = 1127,
  [SMALL_STATE(30)] = 1172,
  [SMALL_STATE(31)] = 1217,
  [SMALL_STATE(32)] = 1262,
  [SMALL_STATE(33)] = 1307,
  [SMALL_STATE(34)] = 1352,
  [SMALL_STATE(35)] = 1397,
  [SMALL_STATE(36)] = 1442,
  [SMALL_STATE(37)] = 1477,
  [SMALL_STATE(38)] = 1512,
  [SMALL_STATE(39)] = 1547,
  [SMALL_STATE(40)] = 1582,
  [SMALL_STATE(41)] = 1614,
  [SMALL_STATE(42)] = 1646,
  [SMALL_STATE(43)] = 1678,
  [SMALL_STATE(44)] = 1707,
  [SMALL_STATE(45)] = 1736,
  [SMALL_STATE(46)] = 1765,
  [SMALL_STATE(47)] = 1794,
  [SMALL_STATE(48)] = 1816,
  [SMALL_STATE(49)] = 1834,
  [SMALL_STATE(50)] = 1852,
  [SMALL_STATE(51)] = 1870,
  [SMALL_STATE(52)] = 1883,
  [SMALL_STATE(53)] = 1896,
  [SMALL_STATE(54)] = 1909,
  [SMALL_STATE(55)] = 1922,
  [SMALL_STATE(56)] = 1935,
  [SMALL_STATE(57)] = 1948,
  [SMALL_STATE(58)] = 1961,
  [SMALL_STATE(59)] = 1974,
  [SMALL_STATE(60)] = 1987,
  [SMALL_STATE(61)] = 2000,
  [SMALL_STATE(62)] = 2013,
  [SMALL_STATE(63)] = 2026,
  [SMALL_STATE(64)] = 2042,
  [SMALL_STATE(65)] = 2058,
  [SMALL_STATE(66)] = 2074,
  [SMALL_STATE(67)] = 2090,
  [SMALL_STATE(68)] = 2106,
  [SMALL_STATE(69)] = 2119,
  [SMALL_STATE(70)] = 2132,
  [SMALL_STATE(71)] = 2145,
  [SMALL_STATE(72)] = 2158,
  [SMALL_STATE(73)] = 2171,
  [SMALL_STATE(74)] = 2184,
  [SMALL_STATE(75)] = 2195,
  [SMALL_STATE(76)] = 2208,
  [SMALL_STATE(77)] = 2221,
  [SMALL_STATE(78)] = 2234,
  [SMALL_STATE(79)] = 2247,
  [SMALL_STATE(80)] = 2260,
  [SMALL_STATE(81)] = 2273,
  [SMALL_STATE(82)] = 2281,
  [SMALL_STATE(83)] = 2289,
  [SMALL_STATE(84)] = 2296,
  [SMALL_STATE(85)] = 2303,
  [SMALL_STATE(86)] = 2310,
  [SMALL_STATE(87)] = 2317,
};

static const TSParseActionEntry ts_parse_actions[] = {
  [0] = {.entry = {.count = 0, .reusable = false}},
  [1] = {.entry = {.count = 1, .reusable = false}}, RECOVER(),
  [3] = {.entry = {.count = 1, .reusable = true}}, SHIFT_EXTRA(),
  [5] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 0, 0, 0),
  [7] = {.entry = {.count = 1, .reusable = true}}, SHIFT(8),
  [9] = {.entry = {.count = 1, .reusable = false}}, SHIFT(26),
  [11] = {.entry = {.count = 1, .reusable = true}}, SHIFT(2),
  [13] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_source_file, 1, 0, 0),
  [15] = {.entry = {.count = 1, .reusable = true}}, SHIFT(3),
  [17] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0),
  [19] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(8),
  [22] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(26),
  [25] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_source_file_repeat1, 2, 0, 0), SHIFT_REPEAT(3),
  [28] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_statement, 2, 0, 1),
  [30] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_statement, 2, 0, 1),
  [32] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_statement, 3, 0, 2),
  [34] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_statement, 3, 0, 2),
  [36] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_statement, 3, 0, 3),
  [38] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_statement, 3, 0, 3),
  [40] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_statement, 4, 0, 4),
  [42] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_statement, 4, 0, 4),
  [44] = {.entry = {.count = 1, .reusable = true}}, SHIFT(11),
  [46] = {.entry = {.count = 1, .reusable = false}}, SHIFT(11),
  [48] = {.entry = {.count = 1, .reusable = true}}, SHIFT(14),
  [50] = {.entry = {.count = 1, .reusable = false}}, SHIFT(44),
  [52] = {.entry = {.count = 1, .reusable = false}}, SHIFT(31),
  [54] = {.entry = {.count = 1, .reusable = true}}, SHIFT(29),
  [56] = {.entry = {.count = 1, .reusable = false}}, SHIFT(29),
  [58] = {.entry = {.count = 1, .reusable = true}}, SHIFT(64),
  [60] = {.entry = {.count = 1, .reusable = true}}, SHIFT(21),
  [62] = {.entry = {.count = 1, .reusable = false}}, SHIFT(25),
  [64] = {.entry = {.count = 1, .reusable = false}}, SHIFT(15),
  [66] = {.entry = {.count = 1, .reusable = true}}, SHIFT(4),
  [68] = {.entry = {.count = 1, .reusable = true}}, SHIFT(12),
  [70] = {.entry = {.count = 1, .reusable = false}}, SHIFT(12),
  [72] = {.entry = {.count = 1, .reusable = true}}, SHIFT(5),
  [74] = {.entry = {.count = 1, .reusable = true}}, SHIFT(13),
  [76] = {.entry = {.count = 1, .reusable = false}}, SHIFT(13),
  [78] = {.entry = {.count = 1, .reusable = true}}, SHIFT(6),
  [80] = {.entry = {.count = 1, .reusable = true}}, SHIFT(7),
  [82] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(13),
  [85] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(13),
  [88] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(14),
  [91] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(44),
  [94] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(31),
  [97] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(29),
  [100] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(29),
  [103] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(64),
  [106] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(21),
  [109] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(25),
  [112] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0), SHIFT_REPEAT(15),
  [115] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_statement_repeat1, 2, 0, 0),
  [117] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__arg, 1, 0, 0),
  [119] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__arg, 1, 0, 0),
  [121] = {.entry = {.count = 2, .reusable = true}}, REDUCE(sym__arg, 1, 0, 0), SHIFT(40),
  [124] = {.entry = {.count = 2, .reusable = false}}, REDUCE(sym__arg, 1, 0, 0), SHIFT(46),
  [127] = {.entry = {.count = 2, .reusable = true}}, REDUCE(sym__arg, 1, 0, 0), SHIFT(66),
  [130] = {.entry = {.count = 2, .reusable = true}}, REDUCE(sym__arg, 1, 0, 0), SHIFT(48),
  [133] = {.entry = {.count = 2, .reusable = false}}, REDUCE(sym__arg, 1, 0, 0), SHIFT(51),
  [136] = {.entry = {.count = 2, .reusable = false}}, REDUCE(sym__arg, 1, 0, 0), SHIFT(47),
  [139] = {.entry = {.count = 3, .reusable = true}}, REDUCE(sym__arg, 1, 0, 0), REDUCE(sym__expr_operand, 1, 0, 0), SHIFT(84),
  [143] = {.entry = {.count = 2, .reusable = false}}, REDUCE(sym__arg, 1, 0, 0), REDUCE(sym__expr_operand, 1, 0, 0),
  [146] = {.entry = {.count = 3, .reusable = true}}, REDUCE(sym__arg, 1, 0, 0), REDUCE(sym__expr_operand, 1, 0, 0), SHIFT(36),
  [150] = {.entry = {.count = 2, .reusable = true}}, REDUCE(sym__arg, 1, 0, 0), REDUCE(sym__expr_operand, 1, 0, 0),
  [153] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_expr, 1, 0, 0),
  [155] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_expr, 1, 0, 0),
  [157] = {.entry = {.count = 1, .reusable = true}}, SHIFT(45),
  [159] = {.entry = {.count = 1, .reusable = false}}, SHIFT(45),
  [161] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_expr_repeat1, 2, 0, 0),
  [163] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_expr_repeat1, 2, 0, 0),
  [165] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_expr_repeat1, 2, 0, 0), SHIFT_REPEAT(45),
  [168] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_expr_repeat1, 2, 0, 0), SHIFT_REPEAT(45),
  [171] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_expr, 2, 0, 0),
  [173] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_expr, 2, 0, 0),
  [175] = {.entry = {.count = 2, .reusable = true}}, REDUCE(sym__expr_operand, 1, 0, 0), SHIFT(84),
  [178] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__expr_operand, 1, 0, 0),
  [180] = {.entry = {.count = 2, .reusable = true}}, REDUCE(sym__expr_operand, 1, 0, 0), SHIFT(36),
  [183] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__expr_operand, 1, 0, 0),
  [185] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_method_call, 4, 0, 6),
  [187] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_method_call, 4, 0, 6),
  [189] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__expr_operand, 2, 0, 0),
  [191] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__expr_operand, 2, 0, 0),
  [193] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_string, 2, 0, 0),
  [195] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_string, 2, 0, 0),
  [197] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_method_call, 3, 0, 6),
  [199] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_method_call, 3, 0, 6),
  [201] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_boolean, 1, 0, 0),
  [203] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_boolean, 1, 0, 0),
  [205] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_command, 1, 0, 0),
  [207] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_command, 1, 0, 0),
  [209] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym__expr_operand, 3, 0, 0),
  [211] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym__expr_operand, 3, 0, 0),
  [213] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_string, 3, 0, 0),
  [215] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_string, 3, 0, 0),
  [217] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_operator, 1, 0, 0),
  [219] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_operator, 1, 0, 0),
  [221] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_keyword, 1, 0, 0),
  [223] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_keyword, 1, 0, 0),
  [225] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_method_call, 5, 0, 8),
  [227] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_method_call, 5, 0, 8),
  [229] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_method_call, 4, 0, 8),
  [231] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_method_call, 4, 0, 8),
  [233] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_method_call, 5, 0, 6),
  [235] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_method_call, 5, 0, 6),
  [237] = {.entry = {.count = 1, .reusable = true}}, REDUCE(sym_method_call, 6, 0, 8),
  [239] = {.entry = {.count = 1, .reusable = false}}, REDUCE(sym_method_call, 6, 0, 8),
  [241] = {.entry = {.count = 1, .reusable = true}}, SHIFT(40),
  [243] = {.entry = {.count = 1, .reusable = true}}, SHIFT(24),
  [245] = {.entry = {.count = 1, .reusable = false}}, SHIFT(46),
  [247] = {.entry = {.count = 1, .reusable = true}}, SHIFT(66),
  [249] = {.entry = {.count = 1, .reusable = true}}, SHIFT(48),
  [251] = {.entry = {.count = 1, .reusable = false}}, SHIFT(51),
  [253] = {.entry = {.count = 1, .reusable = false}}, SHIFT(47),
  [255] = {.entry = {.count = 1, .reusable = true}}, SHIFT(33),
  [257] = {.entry = {.count = 1, .reusable = true}}, SHIFT(56),
  [259] = {.entry = {.count = 1, .reusable = true}}, SHIFT(61),
  [261] = {.entry = {.count = 1, .reusable = true}}, SHIFT(62),
  [263] = {.entry = {.count = 1, .reusable = true}}, SHIFT(42),
  [265] = {.entry = {.count = 1, .reusable = true}}, SHIFT(22),
  [267] = {.entry = {.count = 1, .reusable = false}}, SHIFT(19),
  [269] = {.entry = {.count = 1, .reusable = true}}, SHIFT(30),
  [271] = {.entry = {.count = 1, .reusable = true}}, SHIFT(54),
  [273] = {.entry = {.count = 1, .reusable = true}}, SHIFT(84),
  [275] = {.entry = {.count = 1, .reusable = true}}, SHIFT(38),
  [277] = {.entry = {.count = 1, .reusable = true}}, SHIFT(43),
  [279] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_expr_repeat1, 2, 0, 0), SHIFT_REPEAT(43),
  [282] = {.entry = {.count = 1, .reusable = false}}, REDUCE(aux_sym_string_repeat1, 2, 0, 0),
  [284] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_string_repeat1, 2, 0, 0), SHIFT_REPEAT(63),
  [287] = {.entry = {.count = 2, .reusable = false}}, REDUCE(aux_sym_string_repeat1, 2, 0, 0), SHIFT_REPEAT(85),
  [290] = {.entry = {.count = 1, .reusable = false}}, SHIFT_EXTRA(),
  [292] = {.entry = {.count = 1, .reusable = false}}, SHIFT(23),
  [294] = {.entry = {.count = 1, .reusable = true}}, SHIFT(67),
  [296] = {.entry = {.count = 1, .reusable = false}}, SHIFT(85),
  [298] = {.entry = {.count = 1, .reusable = false}}, SHIFT(53),
  [300] = {.entry = {.count = 1, .reusable = true}}, SHIFT(63),
  [302] = {.entry = {.count = 1, .reusable = false}}, SHIFT(55),
  [304] = {.entry = {.count = 1, .reusable = true}}, SHIFT(65),
  [306] = {.entry = {.count = 1, .reusable = false}}, SHIFT(28),
  [308] = {.entry = {.count = 1, .reusable = true}}, SHIFT(58),
  [310] = {.entry = {.count = 1, .reusable = true}}, SHIFT(41),
  [312] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_method_call_repeat2, 2, 0, 0),
  [314] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_method_call_repeat2, 2, 0, 0), SHIFT_REPEAT(41),
  [317] = {.entry = {.count = 1, .reusable = true}}, SHIFT(20),
  [319] = {.entry = {.count = 1, .reusable = true}}, SHIFT(35),
  [321] = {.entry = {.count = 1, .reusable = true}}, SHIFT(37),
  [323] = {.entry = {.count = 2, .reusable = true}}, REDUCE(aux_sym_method_call_repeat1, 2, 0, 7), SHIFT_REPEAT(84),
  [326] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_method_call_repeat1, 2, 0, 7),
  [328] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_string_repeat1, 2, 0, 0),
  [330] = {.entry = {.count = 1, .reusable = true}}, SHIFT(57),
  [332] = {.entry = {.count = 1, .reusable = true}}, SHIFT(34),
  [334] = {.entry = {.count = 1, .reusable = true}}, SHIFT(60),
  [336] = {.entry = {.count = 1, .reusable = true}}, SHIFT(59),
  [338] = {.entry = {.count = 1, .reusable = true}}, SHIFT(32),
  [340] = {.entry = {.count = 1, .reusable = true}}, SHIFT(39),
  [342] = {.entry = {.count = 1, .reusable = true}}, REDUCE(aux_sym_method_call_repeat1, 2, 0, 5),
  [344] = {.entry = {.count = 1, .reusable = true}}, SHIFT(27),
  [346] = {.entry = {.count = 1, .reusable = true}}, SHIFT(82),
  [348] = {.entry = {.count = 1, .reusable = false}}, SHIFT(74),
  [350] = {.entry = {.count = 1, .reusable = true}}, SHIFT(52),
  [352] = {.entry = {.count = 1, .reusable = true}},  ACCEPT_INPUT(),
};

#ifdef __cplusplus
extern "C" {
#endif
#ifdef TREE_SITTER_HIDE_SYMBOLS
#define TS_PUBLIC
#elif defined(_WIN32)
#define TS_PUBLIC __declspec(dllexport)
#else
#define TS_PUBLIC __attribute__((visibility("default")))
#endif

TS_PUBLIC const TSLanguage *tree_sitter_golars(void) {
  static const TSLanguage language = {
    .version = LANGUAGE_VERSION,
    .symbol_count = SYMBOL_COUNT,
    .alias_count = ALIAS_COUNT,
    .token_count = TOKEN_COUNT,
    .external_token_count = EXTERNAL_TOKEN_COUNT,
    .state_count = STATE_COUNT,
    .large_state_count = LARGE_STATE_COUNT,
    .production_id_count = PRODUCTION_ID_COUNT,
    .field_count = FIELD_COUNT,
    .max_alias_sequence_length = MAX_ALIAS_SEQUENCE_LENGTH,
    .parse_table = &ts_parse_table[0][0],
    .small_parse_table = ts_small_parse_table,
    .small_parse_table_map = ts_small_parse_table_map,
    .parse_actions = ts_parse_actions,
    .symbol_names = ts_symbol_names,
    .field_names = ts_field_names,
    .field_map_slices = ts_field_map_slices,
    .field_map_entries = ts_field_map_entries,
    .symbol_metadata = ts_symbol_metadata,
    .public_symbol_map = ts_symbol_map,
    .alias_map = ts_non_terminal_alias_map,
    .alias_sequences = &ts_alias_sequences[0][0],
    .lex_modes = ts_lex_modes,
    .lex_fn = ts_lex,
    .primary_state_ids = ts_primary_state_ids,
  };
  return &language;
}
#ifdef __cplusplus
}
#endif
