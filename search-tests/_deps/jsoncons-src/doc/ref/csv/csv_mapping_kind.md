### jsoncons::csv::csv_mapping_kind

```cpp
#include <jsoncons_ext/csv/csv_options.hpp>

enum class csv_mapping_kind
{
    n_rows = 1, 
    n_objects, 
    m_columns
};
```

Value      |Definition
-----------|-----------
n_rows    | Map a CSV file to an array of arrays in a `basic_json`.
n_objects | Map a CSV file to an array of objects in a `basic_json`.
m_columns | Map a CSV file to an object with name/array-value pairs. 


