### jsoncons::jsonpointer::basic_json_pointer

```cpp
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template <typename CharT>
class basic_json_pointer
```

Two specializations for common character types are defined:

Type      |Definition
----------|------------------------------
json_pointer   |`basic_json_pointer<char>` (since 0.159.0)
json_ptr       |`basic_json_pointer<char>` (until 0.159.0)
wjson_pointer  |`basic_json_pointer<wchar_t>` (since 0.159.0)
wjson_ptr      |`basic_json_pointer<wchar_t>` (unitl 0.159.0)

Objects of type `basic_json_pointer` represent a JSON Pointer.

#### Member types
Type        |Definition
------------|------------------------------
char_type   | `CharT`
string_type | `std::basic_string<char_type>`
string_view_type | `jsoncons::basic_string_view<char_type>`
const_iterator | A constant [LegacyRandomAccessIterator](https://en.cppreference.com/w/cpp/named_req/RandomAccessIterator) with a `value_type` of `std::basic_string<char_type>`
iterator    | An alias to `const_iterator`

#### Constructors

    basic_json_pointer();                                      (1)

    explicit basic_json_pointer(const string_view_type& str);  

    explicit basic_json_pointer(const string_view_type& str, 
                                std::error_code& ec);          (2)

    basic_json_pointer(const basic_json_pointer&);             (3)

    basic_json_pointer(basic_json_pointer&&) noexcept;         (4)

(1) Constructs an empty `basic_json_pointer`.

(2) Constructs a `basic_json_pointer` from a string representation or a 
URI fragment identifier (starts with `#`).

#### operator=

    basic_json_pointer& operator=(const basic_json_pointer&);

    basic_json_pointer& operator=(basic_json_pointer&&);

#### Modifiers

    basic_json_pointer& append(const string_type& s);     (since 0.172.0)
Appends the token s.

    template <typename IntegerType>
    basic_json_pointer& append(IntegerType index)         (since 0.172.0)
Appends the token `index`.
This overload only participates in overload resolution if `IntegerType` is an integer type.

    basic_json_pointer& operator/=(const string_type& s)
Appends the token s.

    template <typename IntegerType>
    basic_json_pointer& operator/=(IntegerType index) 
Appends the token `index`.
This overload only participates in overload resolution if `IntegerType` is an integer type.

    basic_json_pointer& operator+=(const basic_json_pointer& ptr)
Concatenates the current pointer and the specified pointer `ptr`. 

#### Iterators

    iterator begin() const;
    iterator end() const;
Iterator access to the tokens in the pointer.

#### Accessors

    bool empty() const
Checks if the pointer is empty

   string_type to_string() const
Returns a JSON Pointer represented as a string value, escaping any `/` or `~` characters.


#### Static member functions

   static parse(const string_view_type& str);
   static parse(const string_view_type& str, std::error_code& ec);
Constructs a `basic_json_pointer` from a string representation or a 
URI fragment identifier (starts with `#`).

#### Non-member functions
    basic_json_pointer<CharT> operator/(const basic_json_pointer<CharT>& lhs, const basic_string<CharT>& s);
Concatenates a JSON Pointer pointer and a string. Effectively returns basic_json_pointer<CharT>(lhs) /= s.

    template <typename CharT,class IntegerType>
    basic_json_pointer<CharT> operator/(const basic_json_pointer<CharT>& lhs, IntegerType index);
Concatenates a JSON Pointer pointer and an index. Effectively returns basic_json_pointer<CharT>(lhs) /= index.
This overload only participates in overload resolution if `IntegerType` is an integer type.

    template <typename CharT,class IntegerType>
    basic_json_pointer<CharT> operator+( const basic_json_pointer<CharT>& lhs, const basic_json_pointer<CharT>& rhs );
Concatenates two JSON Pointers. Effectively returns basic_json_pointer<CharT>(lhs) += rhs.

    template <typename CharT,class IntegerType>
    bool operator==(const basic_json_pointer<CharT>& lhs, const basic_json_pointer<CharT>& rhs);

    template <typename CharT,class IntegerType>
    bool operator!=(const basic_json_pointer<CharT>& lhs, const basic_json_pointer<CharT>& rhs);

    std::string to_string(const json_pointer& ptr);      (since 0.172.0)

    std::wstring to_wstring(const wjson_pointer& ptr);   (since 0.172.0)

    template <typename CharT>
    std::basic_ostream<CharT>& operator<<(std::basic_ostream<CharT>& os, const basic_json_pointer<CharT>& ptr);
Performs stream output

### Examples

#### Iterate over the tokens in a JSON Pointer

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

// for brevity
namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    jsonpointer::json_pointer ptr("/store/book/1/author");

    std::cout << "(1) " << ptr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& token : ptr)
    {
        std::cout << token << "\n";
    }

    std::cout << "\n";
}
```
Output:
```
(1) /store/book/1/author

(2)
store
book
1
author
```

#### Append tokens to a JSON Pointer

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    jsonpointer::json_pointer ptr;

    ptr /= "a/b";
    ptr /= "";
    ptr /= "m~n";

    std::cout << "(1) " << ptr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& token : ptr)
    {
        std::cout << token << "\n";
    }

    std::cout << "\n";
}
```
Output:
```
(1) /a~1b//m~0n

(2)
a/b

m~n
```

#### Concatentate two JSONPointers

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jsonpointer = jsoncons::jsonpointer;

int main()
{
    jsonpointer::json_pointer ptr("/a~1b");

    ptr += jsonpointer::json_pointer("//m~0n");

    std::cout << "(1) " << ptr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& token : ptr)
    {
        std::cout << token << "\n";
    }

    std::cout << "\n";
}
```
Output:
```
(1) /a~1b//m~0n

(2)
a/b

m~n
```

