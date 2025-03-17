### jsoncons::ser_context

```cpp
#include <jsoncons/ser_context.hpp>

class ser_context;
```

Provides contextual information for serializing and deserializing JSON and JSON-like data formats.
This information may be used for error reporting.

    virtual size_t line() const;
Returns the line number for the text being parsed.
Line numbers (if available) start at 1. The default implementation returns 0.

    virtual size_t column() const;
Returns the column number to the end of the text being parsed.
Column numbers (if available) start at 1. The default implementation returns 0.

    virtual size_t position() const;
`position()` is defined for all JSON elements reported to the visitor, and indicates
the position of the character at the beginning of the element, e.g. '"' for a string
or the first digit for a positive number.
Currently only supported for the JSON parser.

    virtual size_t end_position() const;
`end_position()` is defined for all JSON elements reported to the visitor, and indicates
the position after the character at the end of the element, e.g. one past the closing '"' for a string
or the one past the last digit for a number.
Currently only supported for the JSON parser.
