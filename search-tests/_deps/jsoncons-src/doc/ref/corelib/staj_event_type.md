### jsoncons::staj_event_type

```cpp
#include <jsoncons/staj_cursor.hpp>

enum class staj_event_type
{
    begin_array,
    end_array,
    begin_object,
    end_object,
    name, // until 0.150.0
    key, // since 0.150.0
    string_value,
    byte_string_value,
    null_value,
    bool_value,
    int64_value,
    uint64_value,
    half_value,
    double_value
};
```

