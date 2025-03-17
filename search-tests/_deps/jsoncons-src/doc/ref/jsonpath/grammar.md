path = absolute-path | relative-path

absolute-path = "$" [qualified-path]

qualified-path = recursive-location | relative-location
 
recursive-location = ".." relative-path

relative-location = "." relative-path

expression = single-quoted-string
expression =/ json-literal
expression =/ jsonpath-expression 
expression =/ unary-expression / binary-expression / regex-expression / paren-expression 
paren-expression  = "(" expression ")"
unary-expression=unary-operator expression
binary-expression = expression binary-operator expression
regex-expression = expression regex-operator regex-literal
regex-literal = "/" regex-character-literals "/"
unary-operator = "!" / "-"
binary-operator  = "*" / "/" / "%" / "+" / "-" 
binary-operator =/ "&&" / "||" 
binary-operator =/ <" / "<=" / "==" / ">=" / ">" / "!=" 
regex-operator = "=~"

function-expression = unquoted-string  (
                        no-args  /
                        one-or-more-args )
no-args             = "(" ")"
one-or-more-args    = "(" ( function-arg *( "," function-arg ) ) ")"
function-arg        = expression
