## Grammar

The grammar is specified using ABNF, as described in RFC4234

```
json-path = root-selector / selector_rhs
root-selector = "$"

selector        = sub-selector / index-selector  / identifier
selector        =/ "*" / union_expression
selector        =/ function-expression 

selector_rhs    = "." ( selector / index-selector / identifier / "*" )
selector_rhs    =/ union_selector 

index-selector  = selector bracket-specifier / bracket-specifier

union = "[" ( selector *( "," selector ) ) "]"
bracket-specifier = "[" (number / "*" / slice-selector) "]" / "[]"
bracket-specifier =/ "[?" selector "]"
slice-selector  = [number] ":" [number] [ ":" [number] ]
function-selector = unquoted-string  (
                        no-args  /
                        one-or-more-args )
no-args             = "(" ")"
one-or-more-args    = "(" ( function-arg *( "," function-arg ) ) ")"
function-arg        = selector 
current-node        = "@"

preserved-escape  = escape (%x20-26 / %28-5B / %x5D-10FFFF)
raw-string-escape = escape ("'" / escape)
literal           = "`" json-value "`"
unescaped-literal = %x20-21 /       ; space !
                        %x23-5B /   ; # - [
                        %x5D-5F /   ; ] ^ _
                        %x61-7A     ; a-z
                        %x7C-10FFFF ; |}~ ...
escaped-literal   = escaped-char / (escape %x60)
number            = ["-"]1*digit
digit             = %x30-39
identifier        = unquoted-string / quoted-string
unquoted-string   = (%x41-5A / %x61-7A / %x5F) *(  ; A-Za-z_
                        %x30-39  /  ; 0-9
                        %x41-5A /  ; A-Z
                        %x5F    /  ; _
                        %x61-7A)   ; a-z
quoted-string     = quote 1*(unescaped-char / escaped-char) quote
unescaped-char    = %x20-21 / %x23-5B / %x5D-10FFFF
escape            = %x5C   ; Back slash: \
quote             = %x22   ; Double quote: '"'
escaped-char      = escape (
                        %x22 /          ; "    quotation mark  U+0022
                        %x5C /          ; \    reverse solidus U+005C
                        %x2F /          ; /    solidus         U+002F
                        %x62 /          ; b    backspace       U+0008
                        %x66 /          ; f    form feed       U+000C
                        %x6E /          ; n    line feed       U+000A
                        %x72 /          ; r    carriage return U+000D
                        %x74 /          ; t    tab             U+0009
                        %x75 4HEXDIG )  ; uXXXX                U+XXXX

; The ``json-value`` is any valid JSON value
```
