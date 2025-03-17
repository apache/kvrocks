### jsoncons::json_errc

```cpp
#include <jsoncons/json_error.hpp>
```

<br>

The constant integer values scoped by `jsoncons::json_errc` define the values for json text error codes.

### Member constants

constant                            |Description
------------------------------------|------------------------------
`unexpected_eof`                    |Unexpected end of file
`syntax_error`                    |JSON syntax_error
`extra_character`          |Unexpected non-whitespace character after JSON text
`json_nesting_too_deep`         |Maximum JSON depth exceeded
`single_quote`        |JSON strings cannot be quoted with single quotes
`illegal_character_in_string`        |Illegal character in string
`extra_comma`        |Extra comma      
`expected_key`                     |Expected object member key
`expected_value`                    |Expected value                     
`invalid_value`                    |Invalid value                     
`expected_colon`           |Expected name separator ':'       
`illegal_control_character`         |Illegal control character in string
`illegal_escaped_character`         |Illegal escaped character in string
`expected_codepoint_surrogate_pair`  |Invalid codepoint, expected another \\u token to begin the second half of a codepoint surrogate pair.
`invalid_hex_escape_sequence`       |Invalid codepoint, expected hexadecimal digit.
`invalid_unicode_escape_sequence`   |Invalid codepoint, expected four hexadecimal digits.
`leading_zero`                    |A number cannot have a leading zero
`invalid_number`                    |Invalid number
`expected_comma_or_rbrace`           |Expected comma or right brace ']'        
`expected_comma_or_rbracket`          |Expected comma or right bracket '}'       
`unexpected_rbrace`          |Unexpected right brace '}'       
`unexpected_rbracket`           |Unexpected right bracket ']'        




