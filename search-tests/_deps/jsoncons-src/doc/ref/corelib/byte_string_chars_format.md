### jsoncons::byte_string_chars_format

```cpp
#include <jsoncons/json_options.hpp>

enum class byte_string_chars_format : uint8_t {base16, base64, base64url};
```

Specifies byte string formatting. 

### Examples

#### Byte string formatting

```cpp
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    std::vector<uint8_t> bytes = {'H','e','l','l','o'};

    json j(byte_string(bytes.data(),bytes.size()));

    // default
    std::cout << "(1) "<< j << "\n\n";

    // base16
    auto options2 = json_options{}
        .byte_string_format(byte_string_chars_format::base16);
    std::cout << "(2) "<< print(j, options2) << "\n\n";

    // base64
    auto options3 = json_options{}
        .byte_string_format(byte_string_chars_format::base64);
    std::cout << "(3) "<< print(j, options3) << "\n\n";

    // base64url
    auto options4 = json_options{}
        .byte_string_format(byte_string_chars_format::base64url);
    std::cout << "(4) "<< print(j, options4) << "\n\n";
}
```
Output:
```
(1) "SGVsbG8"

(2) "48656C6C6F"

(3) "SGVsbG8="

(4) "SGVsbG8"
```

