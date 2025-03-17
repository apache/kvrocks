### jsoncons::bson::bson_options

```cpp
#include <jsoncons_ext/bson/bson_options.hpp>

class bson_options;
```

<br>

![bson_options](./diagrams/bson_options.png)

Specifies options for reading and writing CBOR.

#### Constructors

    bson_options()
Constructs a `bson_options` with default values. 

#### Modifiers

    void max_nesting_depth(int depth)
The maximum nesting depth allowed when decoding and encoding BSON. 
Default is 1024. Parsing can have an arbitrarily large depth
limited only by available memory. Serializing a [basic_json](../basic_json.md) to
BSON is limited by stack size.

