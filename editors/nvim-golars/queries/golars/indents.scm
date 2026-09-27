; Indentation rules for golars .glr
; Statements are one line each. A statement continued with a trailing
; backslash indents its continuation lines.
(statement) @indent.begin
(argument_list ")" @indent.branch)
(list "]" @indent.branch)
(comment) @indent.auto
