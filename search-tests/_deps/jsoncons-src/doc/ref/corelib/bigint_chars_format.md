### jsoncons::bigint_chars_format (deprecated in 1.0.0)

```cpp
#include <jsoncons/json_options.hpp>

enum class bigint_chars_format : uint8_t {number, base10, base64, base64url};   (until 1.0.0)

using bigint_chars_format= bignum_format_kind;                                  (since 1.0.0)
```

Specifies `bigint` formatting. 

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
        .bigint_format(bigint_chars_format::number);
    std::cout << "(2) " << print(j, options2) << "\n\n";

    auto options3 = json_options{}
        .bigint_format(bigint_chars_format::base64);
    std::cout << "(3) " << print(j, options3) << "\n\n";

    auto options4 = json_options{}
    .bigint_format(bigint_chars_format::base64url);
    std::cout << "(4) " << print(j, options4) << "\n\n";
}
```
Output:
```
(1) "-18446744073709551617"     (until 1.0.0)

(1) -18446744073709551617       (since 1.0.0)

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
        .bigint_format(bigint_chars_format::number);
    std::cout << "(2) " << print(j, options2) << "\n\n";

    auto options3 = json_options{}
        .bigint_format(bigint_chars_format::base64);
    std::cout << "(3) " << print(j, options3) << "\n\n";

    auto options4 = json_options{}
        .bigint_format(bigint_chars_format::base64url);
    std::cout << "(4) " << print(j, options4) << "\n\n";
}
```
Output:
```
(1) "-18446744073709551617"     (until 1.0.0)

(1) -18446744073709551617       (since 1.0.0)

(2) -18446744073709551617

(3) "~AQAAAAAAAAAA"

(4) "~AQAAAAAAAAAA"
```

