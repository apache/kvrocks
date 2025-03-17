### jsoncons::conv_error

```cpp
#include <jsoncons/json_exception.hpp>
```

<br>

`jsoncons::conv_error` defines an exception type for reporting serialization and deserialization failures.

![conv_error](./diagrams/conv_error.png)

std::exception

#### Constructors

    conv_error(std::error_code ec);

    conv_error(std::error_code ec, std::size_t position);

    conv_error(std::error_code ec,
                        std::size_t line,
                        std::size_t column);

    conv_error(const conv_error& other);

#### Member functions

    std::size_t line() const noexcept
Returns the line number to the end of the text where the exception occurred.
Line numbers start at 1.

    std::size_t column() const noexcept
Returns the column number to the end of the text where the exception occurred.
Column numbers start at 1.

    const char* what() const noexcept
Constructs an error message, including line and column position

#### Inherited from std::system_error

    const std::error_code code() const noexcept
Returns an error code for this exception

### Example

```cpp
#include <jsoncons/json.hpp>

using jsoncons::json;

int main()
{
    string s = "[1,2,3,4,]";
    try 
    {
        jsoncons::json j = jsoncons::json::parse(s);
    } 
    catch(const jsoncons::conv_error& e) 
    {
        std::cout << "Caught conv_error with category " 
                  << e.code().category().name() 
                  << ", code " << e.code().value() 
                  << " and message " << e.what() << '\n';
    }
}
```

Output:
```
Caught conv_error with category json_input, code 1 and message Unexpected value separator ',' at line 1 and column 10
```
