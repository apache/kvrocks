### jsoncons::jsonpointer::unflatten_options

```cpp
#include <jsoncons/jsonpointer/jsonpointer.hpp>

enum class unflatten_options{none,object=1};
```
Specifies how to preserve arrays while unflattening a json object of JSON Pointer-value pairs.
There is no unique solution.
An integer appearing in a path could be an array index or it could be an object key.

Value      |Definition
-----------|-----------
none|Default is to attempt to preserve arrays
assume_object|Assume an integer appearing in a path is an object key

