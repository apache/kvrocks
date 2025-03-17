### jsoncons::bignum_format_kind 

```cpp
#include <jsoncons/json_options.hpp>

enum class bignum_format_kind : uint8_t {raw, 
    number=raw,   (deprecated) 
    base10, 
    base64, 
    base64url};
```

Specifies `bignum` formatting. 

### Examples

#### Initializing with bigint


```cpp
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    std::string s = "-18446744073709551617";

    json j(bigint::from_string(s.c_str()));

    std::cout << "(1) " << print(j) << "\n\n";

    auto options2 = json_options{}
        .bignum_format(bignum_format_kind::raw);
    std::cout << "(2) " << print(j, options2) << "\n\n";

    auto options3 = json_options{}
        .bignum_format(bignum_format_kind::base64);
    std::cout << "(3) " << print(j, options3) << "\n\n";

    auto options4 = json_options{}
    .bignum_format(bignum_format_kind::base64url);
    std::cout << "(4) " << print(j, options4) << "\n\n";
}
```
Output:
```
(1) -18446744073709551617

(2) -18446744073709551617

(3) "~AQAAAAAAAAAA"

(4) "~AQAAAAAAAAAA"
```

#### Integer overflow during parsing

```cpp
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    std::string s = "-18446744073709551617";

    json j = json::parse(s);

    std::cout << "(1) " << print(j) << "\n\n";

    auto options2 = json_options{}
        .bignum_format(bignum_format_kind::raw);
    std::cout << "(2) " << print(j, options2) << "\n\n";

    auto options3 = json_options{}
        .bignum_format(bignum_format_kind::base64);
    std::cout << "(3) " << print(j, options3) << "\n\n";

    auto options4 = json_options{}
        .bignum_format(bignum_format_kind::base64url);
    std::cout << "(4) " << print(j, options4) << "\n\n";
}
```
Output:
```
(1) -18446744073709551617

(2) -18446744073709551617

(3) "~AQAAAAAAAAAA"

(4) "~AQAAAAAAAAAA"
```

