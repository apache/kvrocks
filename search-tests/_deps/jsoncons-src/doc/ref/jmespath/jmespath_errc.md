### jsoncons::jmespath::jmespath_errc

```cpp
#include <jsoncons_ext/jmespath/jmespath_error.hpp>
```

<br>

The constant integer values scoped by `jmespath_errc` define the values for jmespath error codes.

### Member constants

constant                            |Description
------------------------------------|------------------------------
`expected_identifier`               | Expected identifier                           
`expected_index`                    | Expected index                                
`expected_A_Za_Z_`                  | Expected A-Z, a-z, or _                       
`expected_rbracket`            | Expected ]                                    
`expected_rbrace`              | Expected }                                    
`expected_colon`                    | Expected :                                    
`expected_dot`                      | Expected \".\"                                
`expected_or`                       | Expected \"\|\|\"                               
`expected_and`                      | Expected \"&&\"                               
`expected_multi_select_list`        | Expected multi-select-list                    
`invalid_number`                    | Invalid number                                
`invalid_literal`                   | Invalid literal                               
`expected_comparator`               | Expected <, <=, ==, >=, > or !=               
`expected_key`                      | Expected key                                  
`invalid_argument`                  | Invalid argument type                         
`unknown_function`                  | Unknown function                              
`invalid_type`                      | Invalid type                                  
`unexpected_end_of_input`           | Unexpected end of jmespath input              
`step_cannot_be_zero`               | Slice step cannot be zero                     
`syntax_error`                      | Syntax error                           
`invalid_codepoint`                 | Invalid codepoint                             
`illegal_escaped_character`         | Illegal escaped character                     
`unbalanced_parentheses`            | Unbalanced parentheses                        
`invalid_arity`                     | Function called with wrong number of arguments
