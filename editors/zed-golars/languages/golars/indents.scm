; Indentation rules for golars .glr
; Statements are one line each. A statement continued with a trailing
; backslash indents its continuation lines; an open bracket indents
; until it closes.
(statement) @indent
(argument_list ")" @end) @indent
(list "]" @end) @indent
