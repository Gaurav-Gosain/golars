; Tree-sitter injection queries for golars .glr
;
; No nested languages to inject for now. The filter predicate DSL is
; simple enough that a dedicated grammar would just duplicate the
; token rules from grammar.js. Leave this file in place so editors
; that expect it don't warn; add actual injections later if we grow
; an SQL-in-filter feature or similar.
