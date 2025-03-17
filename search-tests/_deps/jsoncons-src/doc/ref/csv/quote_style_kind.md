### jsoncons::csv::quote_style_kind

```cpp
#include <jsoncons_ext/csv/csv_options.hpp>

enum class quote_style_kind
{
    minimal,
    all,
    nonnumeric,
    none
};
```

Value      |Definition
-----------|-----------
minimal    | Only quote fields that contain special characters, such as a line, field or subfield delimiter, or a quote character.
all        | Quote all fields
nonnumeric | Quote all non-numeric fields
none       | Never quote fields

