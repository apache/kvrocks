### Built-in Specializations

jsoncons supports many types in the standard library.

* [integer](#integer) Integer types, including `__int128` and `__uint128` if supported on the platform.
* float and double
* bool
* [nullptr_t](https://en.cppreference.com/w/cpp/types/nullptr_t) (since 0.155.0)
* [basic_string](#basic_string) - jsoncons supports [std::basic_string](https://en.cppreference.com/w/cpp/string/basic_string) 
with character types `char` and `wchar_t`
* [basic_string_view](#basic_string_view) - jsoncons supports [std::basic_string_view](https://en.cppreference.com/w/cpp/string/basic_string_view) 
with character types `char` and `wchar_t`
* [duration](#duration) (since 0.155.0) - covers [std::chrono::duration](https://en.cppreference.com/w/cpp/chrono/duration)
for tick periods `std::ratio<1>` (one second), `std::milli` and  `std::nano`.
* [pair](#pair)
* [tuple](#tuple)
* [optional](#optional)
* [shared_ptr and unique_ptr](#smart_ptr) - if `T` is a class that is not a polymorphic class,
jsoncons provides specializations for `std::shared_ptr<T>` and `std::unique_ptr<T>`
* [variant](#variant) (since 0.154.0)
* [sequence containers](#sequence) - includes [std::array](https://en.cppreference.com/w/cpp/container/array), 
[std::vector](https://en.cppreference.com/w/cpp/container/vector), [std::deque](https://en.cppreference.com/w/cpp/container/deque), 
[std::forward_list](https://en.cppreference.com/w/cpp/container/forward_list) and [std::list](https://en.cppreference.com/w/cpp/container/list).
* [associative containers](#associative) - includes [std::set](https://en.cppreference.com/w/cpp/container/set), 
[std::map](https://en.cppreference.com/w/cpp/container/map), [std::multiset](https://en.cppreference.com/w/cpp/container/multiset), 
and [std::multimap](https://en.cppreference.com/w/cpp/container/multimap).
* [unordered associative containers](#unordered) - includes unordered associative containers
[std::unordered_set](https://en.cppreference.com/w/cpp/container/unordered_set), 
[std::unordered_map](https://en.cppreference.com/w/cpp/container/unordered_map), 
[std::unordered_multiset](https://en.cppreference.com/w/cpp/container/unordered_multiset), and 
[std::unordered_multimap](https://en.cppreference.com/w/cpp/container/unordered_multimap).
* [bitset](#bitset) (since 0.156.0)

### integer

Supported integer types include integral types such as `char`, `int8_t`, `int`, `unsigned long long`, `int64_t`, and `uint64_t`.
Also supported are 128 bit integer types `__int128` and `unsigned __int128`, if supported on the platform.
jsoncons encodes integer types with size greater than 64 bit to strings if JSON,
`bignum` if CBOR, and strings for all other formats.

#### JSON example assuming gcc or clang

```cpp
#include <jsoncons/json.hpp>
#include <iostream>
#include <cassert>

using jsoncons::json;

int main()
{
    json j1("-18446744073709551617", semantic_tag::bigint);
    std::cout << j1 << "\n\n";

    __int128 val1 = j1.as<__int128>();

    json j2(val1);
    assert(j2 == j1);

    __int128 val2 = j2.as<__int128>();
    assert(val2 == val1);
}
```
Output:
```
"-18446744073709551617"
```

#### CBOR example assuming gcc or clang

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <iostream>
#include <cassert>

using jsoncons::json;
namespace cbor = jsoncons::cbor;

int main()
{
    json j1("-18446744073709551617", semantic_tag::bigint);
    std::cout << "(1) " << j1 << "\n\n";

    __int128 val1 = j1.as<__int128>();

    std::vector<uint8_t> data;
    cbor::encode_cbor(val1, data);
    std::cout << "(2) " << jsoncons::byte_string_view(data) << "\n\n";
    /*
       c3, // Negative bignum
         49,01,00,00,00,00,00,00,00,00
    */

    auto val2 = cbor::decode_cbor<unsigned __int128>(data);
    CHECK((val2 == val1));
}
```
Output:
```
(1) "-18446744073709551617"

(2) c3,49,01,00,00,00,00,00,00,00,00
```

#### MessagePack example assuming gcc or clang

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/msgpack/msgpack.hpp>
#include <iostream>
#include <cassert>

using jsoncons::json;
namespace msgpack = jsoncons::msgpack;

int main()
{
    json j1("-18446744073709551617", semantic_tag::bigint);
    std::cout << "(1) " << j1 << "\n\n";

    __int128 val1 = j1.as<__int128>();

    std::vector<uint8_t> data;
    msgpack::encode_msgpack(val1, data);
    std::cout << "(2) " << byte_string_view(data) << "\n\n";
    /*
        b5, // fixstr, length 21
          2d,31,38,34,34,36,37,34,34,30,37,33,37,30,39,35,35,31,36,31,37
    */

    auto val2 = msgpack::decode_msgpack<unsigned __int128>(data);
    CHECK((val2 == val1));
}
```
Output:
```
(1) "-18446744073709551617"

(2) b5,2d,31,38,34,34,36,37,34,34,30,37,33,37,30,39,35,35,31,36,31,37
```

### duration

jsoncons supports [std::chrono::duration](https://en.cppreference.com/w/cpp/chrono/duration)
for tick periods `std::ratio<1>` (one second), `std::milli` and  `std::nano`.

#### CBOR example (integer)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <iostream>

using jsoncons::json;
namespace cbor = jsoncons::cbor;

int main()
{
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto time = std::chrono::duration_cast<std::chrono::seconds>(duration);

    std::vector<uint8_t> data;
    cbor::encode_cbor(time, data);

    /*
      c1, // Tag 1 (epoch time)
        1a, // 32 bit unsigned integer
          5f,23,29,18 // 1596139800
    */

    std::cout << "CBOR bytes:\n" << jsoncons::byte_string_view(data) << "\n\n";

    auto seconds = cbor::decode_cbor<std::chrono::seconds>(data);
    std::cout << "Time since epoch (seconds): " << seconds.count() << "\n";
}
```
Output:
```
CBOR bytes: 
c1,1a,5f,23,29,18

Time since epoch (seconds): 1596139800
```

#### CBOR example (double)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <iostream>

using jsoncons::json;
namespace cbor = jsoncons::cbor;

int main()
{
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto time = std::chrono::duration_cast<std::chrono::duration<double>>(duration);

    std::vector<uint8_t> data;
    cbor::encode_cbor(time, data);

    /*
      c1, // Tag 1 (epoch time)
        fb,  // Double
          41,d7,c8,ca,46,1c,0f,87 // 1596139800.43845
    */

    std::cout << "CBOR bytes:\n" << jsoncons::byte_string_view(data) << "\n\n";

    auto seconds = cbor::decode_cbor<std::chrono::duration<double>>(data);
    std::cout << "Time since epoch (seconds): " << seconds.count() << "\n";

    auto milliseconds = cbor::decode_cbor<std::chrono::milliseconds>(data);
    std::cout << "Time since epoch (milliseconds): " << milliseconds.count() << "\n";
}
```
Output:
```
CBOR bytes:
c1,fb,41,d7,c8,ca,46,1c,0f,87

Time since epoch (seconds): 1596139800.43845
Time since epoch (milliseconds): 1596139800438
```

#### MessagePack example (timestamp 32)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/msgpack/msgpack.hpp>
#include <iostream>

using jsoncons::json;
namespace msgpack = jsoncons::msgpack;

int main()
{
    std::vector<uint8_t> data = {
        0xd6, 0xff, // timestamp 32
        0x5a,0x4a,0xf6,0xa5 // 1514862245
    };
    auto seconds = msgpack::decode_msgpack<std::chrono::seconds>(data);
    std::cout << "Seconds elapsed since 1970-01-01 00:00:00 UTC: " << seconds.count() << "\n";
}
```
Output:
```
Seconds elapsed since 1970-01-01 00:00:00 UTC: 1514862245
```

#### MessagePack example (timestamp 64)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/msgpack/msgpack.hpp>
#include <iostream>

using jsoncons::json;
namespace msgpack = jsoncons::msgpack;

int main()
{
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto dur_nano = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);

    std::vector<uint8_t> data;
    msgpack::encode_msgpack(dur_nano, data);

    /*
        d7, ff, // timestamp 64
        e3,94,56,e0, // nanoseconds in 30-bit unsigned int
        5f,22,b6,8b // seconds in 34-bit unsigned int         
    */ 

    std::cout << "MessagePack bytes:\n" << jsoncons::byte_string_view(data) << "\n\n";

    auto nanoseconds = msgpack::decode_msgpack<std::chrono::nanoseconds>(data);
    std::cout << "nanoseconds elapsed since 1970-01-01 00:00:00 UTC: " << nanoseconds.count() << "\n";

    auto milliseconds = msgpack::decode_msgpack<std::chrono::milliseconds>(data);
    std::cout << "milliseconds elapsed since 1970-01-01 00:00:00 UTC: " << milliseconds.count() << "\n";

    auto seconds = msgpack::decode_msgpack<std::chrono::seconds>(data);
    std::cout << "seconds elapsed since 1970-01-01 00:00:00 UTC: " << seconds.count() << "\n";
}
```
Output:
```
nanoseconds elapsed since 1970-01-01 00:00:00 UTC: 1596128821304212600
milliseconds elapsed since 1970-01-01 00:00:00 UTC: 1596128821304
seconds elapsed since 1970-01-01 00:00:00 UTC: 1596128821
```

#### MessagePack example (timestamp 96)

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/msgpack/msgpack.hpp>
#include <iostream>

using jsoncons::json;
namespace msgpack = jsoncons::msgpack;

int main()
{
    std::vector<uint8_t> input = {
        0xc7,0x0c,0xff, // timestamp 96
        0x3b,0x9a,0xc9,0xff, // 999999999 nanoseconds in 32-bit unsigned int
        0xff,0xff,0xff,0xff,0x7c,0x55,0x81,0x7f // -2208988801 seconds in 64-bit signed int
    };

    auto milliseconds = msgpack::decode_msgpack<std::chrono::milliseconds>(input);
    std::cout << "milliseconds elapsed since 1970-01-01 00:00:00 UTC: " << milliseconds.count() << "\n";

    auto seconds = msgpack::decode_msgpack<std::chrono::seconds>(input);
    std::cout << "seconds elapsed since 1970-01-01 00:00:00 UTC: " << seconds.count() << "\n";
}
```
Output:
```
milliseconds elapsed since 1970-01-01 00:00:00 UTC: -2208988801999
seconds elapsed since 1970-01-01 00:00:00 UTC: -2208988801
```

#### BSON example

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <iostream>

using jsoncons::json;
namespace bson = jsoncons::bson;

int main()
{
    auto duration = std::chrono::system_clock::now().time_since_epoch();
    auto time = std::chrono::duration_cast<std::chrono::milliseconds>(duration);

    json j;
    j.try_emplace("time", time);

    auto milliseconds = j["time"].as<std::chrono::milliseconds>();
    std::cout << "Time since epoch (milliseconds): " << milliseconds.count() << "\n\n";
    auto seconds = j["time"].as<std::chrono::seconds>();
    std::cout << "Time since epoch (seconds): " << seconds.count() << "\n\n";

    std::vector<uint8_t> data;
    bson::encode_bson(j, data);

    std::cout << "BSON bytes:\n" << jsoncons::byte_string_view(data) << "\n\n";

/*
    13,00,00,00, // document has 19 bytes
      09, // UTC datetime
        74,69,6d,65,00, // "time"
        ea,14,7f,96,73,01,00,00, // 1595957777642
    00 // terminating null    
*/
}
```
Output:
```
Time since epoch (milliseconds): 1595957777642

Time since epoch (seconds): 1595957777

BSON bytes:
13,00,00,00,09,74,69,6d,65,00,ea,14,7f,96,73,01,00,00,00
```

### pair

The pair specialization encodes an `std::pair` as a JSON array of size 2.

### tuple

The tuple specialization encodes an `std::tuple` as a fixed size JSON array.

#### Example

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/msgpack/msgpack.hpp>
#include <jsoncons_ext/ubjson/ubjson.hpp>
#include <cassert>
#include <iostream>

using qualifying_results_type = std::tuple<std::size_t,std::string,std::string,std::string,std::chrono::milliseconds>;

int main()
{
    std::vector<qualifying_results_type> results = {
        {1,"Lewis Hamilton","Mercedes","1'24.303",std::chrono::milliseconds(0)},
        {2,"Valtteri Bottas","Mercedes","1'24.616",std::chrono::milliseconds(313)},
        {3,"Max Verstappen","Red Bull","1'25.325",std::chrono::milliseconds(1022)}
    };

    std::string json_data;
    encode_json(results, json_data, indenting::indent);
    std::cout << json_data << "\n\n";
    auto results1 = decode_json<std::vector<qualifying_results_type>>(json_data);
    assert(results1 == results);

    auto csv_options = csv::csv_options{}          
        .column_names("Pos,Driver,Entrant,Time,Gap")
        .mapping_kind(csv::csv_mapping_kind::n_rows)
        .header_lines(1);
    std::string csv_data;
    csv::encode_csv(results, csv_data, csv_options);
    std::cout << csv_data << "\n\n";
    auto results2 = csv::decode_csv<std::vector<qualifying_results_type>>(csv_data, csv_options);
    assert(results2 == results);

    std::vector<uint8_t> bson_data;
    bson::encode_bson(results, bson_data);
    auto results3 = bson::decode_bson<std::vector<qualifying_results_type>>(bson_data);
    assert(results3 == results);

    std::vector<uint8_t> cbor_data;
    cbor::encode_cbor(results, cbor_data);
    auto results4 = cbor::decode_cbor<std::vector<qualifying_results_type>>(cbor_data);
    assert(results4 == results);

    std::vector<uint8_t> msgpack_data;
    msgpack::encode_msgpack(results, msgpack_data);
    auto results5 = msgpack::decode_msgpack<std::vector<qualifying_results_type>>(msgpack_data);
    assert(results5 == results);

    std::vector<uint8_t> ubjson_data;
    ubjson::encode_ubjson(results, ubjson_data);
    auto results6 = ubjson::decode_ubjson<std::vector<qualifying_results_type>>(ubjson_data);
    assert(results6 == results);
}

```
Output:
```
[
    [1, "Lewis Hamilton", "Mercedes", "1'24.303", 0],
    [2, "Valtteri Bottas", "Mercedes", "1'24.616", 313],
    [3, "Max Verstappen", "Red Bull", "1'25.325", 1022]
]

Pos,Driver,Entrant,Time,Gap
1,Lewis Hamilton,Mercedes,1'24.303,0
2,Valtteri Bottas,Mercedes,1'24.616,313
3,Max Verstappen,Red Bull,1'25.325,1022
```

<div id="smart_ptr"/> 

### shared_ptr and unique_ptr

If `T` is a class that is not a polymorphic class (does not have any virtual functions),
jsoncons provides specializations for `std::shared_ptr<T>` and `std::unique_ptr<T>`.

#### Example

```cpp
#include <jsoncons/json.hpp>
#include <vector>
#include <string>
#include <iostream>

namespace ns {
    struct Model {
        Model() : x(0), y(0), z(0) {}

        std::string path;
        int x;
        int y;
        int z;
    };

    struct Models {
        std::vector<std::unique_ptr<Model>> models;
    };
}

JSONCONS_ALL_MEMBER_TRAITS(ns::Model, path, x, y, z)
JSONCONS_ALL_MEMBER_TRAITS(ns::Models, models)

int main()
{
    std::string input = R"(
    {
      "models" : [
        {"path" : "foo", "x" : 1, "y" : 2, "z" : 3}, 
        {"path" : "bar", "x" : 4, "y" : 5, "z" : 6}, 
        {"path" : "baz", "x" : 7, "y" : 8, "z" : 9} 
      ]
    }
    )";

    auto models = jsoncons::decode_json<std::shared_ptr<ns::Models>>(input);

    for (auto& it : models->models)
    {
        std::cout << "path: " << it->path 
                  << ", x: " << it->x << ", y: " << it->y << ", z: " << it->z << "\n";
    }

    std::cout << "\n";
    jsoncons::encode_json(models, std::cout, indenting::indent);
    std::cout << "\n\n";
}
```
Output:
```
path: foo, x: 1, y: 2, z: 3
path: bar, x: 4, y: 5, z: 6
path: baz, x: 7, y: 8, z: 9

{
    "models": [
        {
            "path": "foo",
            "x": 1,
            "y": 2,
            "z": 3
        },
        {
            "path": "bar",
            "x": 4,
            "y": 5,
            "z": 6
        },
        {
            "path": "baz",
            "x": 7,
            "y": 8,
            "z": 9
        }
    ]
}
```

### variant

#### Example

```cpp
int main()
{
    using variant_type = std::variant<std::nullptr_t, int, double, bool, std::string>;
    
    std::vector<variant_type> v = {nullptr, 10, 5.1, true, std::string("Hello World")}; 

    std::string buffer;
    jsoncons::encode_json(v, buffer, indenting::indent);
    std::cout << "(1)\n" << buffer << "\n\n";

    auto v2 = jsoncons::decode_json<std::vector<variant_type>>(buffer);

    auto visitor = [](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, std::nullptr_t>)
                std::cout << "nullptr " << arg << '\n';
            else if constexpr (std::is_same_v<T, int>)
                std::cout << "int " << arg << '\n';
            else if constexpr (std::is_same_v<T, double>)
                std::cout << "double " << arg << '\n';
            else if constexpr (std::is_same_v<T, bool>)
                std::cout << "bool " << arg << '\n';
            else if constexpr (std::is_same_v<T, std::string>)
                std::cout << "std::string " << arg << '\n';
        };

    std::cout << "(2)\n";
    for (const auto& item : v2)
    {
        std::visit(visitor, item);
    }
}
```
Output:
```
(1)
[
    null,
    10,
    5.1,
    true,
    "Hello World"
]

(2)
nullptr nullptr
int 10
double 5.1
bool true
std::string Hello World
```

### sequence containers 

#### Example

```cpp
std::vector<int> v{1, 2, 3, 4};
json j(v);
std::cout << "(1) "<< j << '\n';
std::deque<int> d = j.as<std::deque<int>>();
```
Output:
```
(1) [1,2,3,4]
```

### associative containers 

#### From std::map, to std::unordered_map

```cpp
std::map<std::string,int> m{{"one",1},{"two",2},{"three",3}};
json j(m);
std::cout << j << '\n';
std::unordered_map<std::string,int> um = j.as<std::unordered_map<std::string,int>>();
```
Output:
```
{"one":1,"three":3,"two":2}
```
#### std::map with integer key

```cpp
std::map<short, std::string> m{ {1,"foo",},{2,"baz"} };

json j{m};

std::cout << "(1)\n";
std::cout << pretty_print(j) << "\n\n";

auto other = j.as<std::map<uint64_t, std::string>>();

std::cout << "(2)\n";
for (const auto& item : other)
{
    std::cout << item.first << " | " << item.second << "\n";
}
std::cout << "\n\n";
```
Output:

```cpp
(1)
{
    "1": "foo",
    "2": "baz"
}

(2)
1 | foo
2 | baz
```

### bitset

jsoncons encodes a `std::bitset` into `base16` encoded strings (JSON) and
byte strings (binary formats.) jsoncons can decode a `std::bitset` 
from integers, `base16` encoded strings and byte strings.

#### JSON example

```cpp
#include <cassert>
#include <string>
#include <climits>
#include <iostream>
#include <jsoncons/json.hpp>

int main()
{
     std::bitset<70> bs1(ULLONG_MAX);

     std::string s;
     encode_json(bs1, s);
     std::cout << s << "\n\n";

     auto bs2 = decode_json<std::bitset<70>>(s);

     assert(bs2 == bs1);
}
```
Output:
```
"FFFFFFFFFFFFFFFF00"
```

#### CBOR example

```cpp
#include <cassert>
#include <string>
#include <climits>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

int main()
{
    std::bitset<8> bs1(42);

    std::vector<uint8_t> data;
    cbor::encode_cbor(bs1, data);
    std::cout << byte_string_view(data) << "\n\n";
    /*
      0xd7, // Expected conversion to base16
        0x41, // Byte string value of length 1 
          0x54
    */

    auto bs2 = cbor::decode_cbor<std::bitset<8>>(data);

    assert(bs2 == bs1);
}
```
Output:
```
d7,41,54
```
