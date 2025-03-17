### jsoncons::byte_string_view

```cpp
#include <jsoncons/utility/byte_string.hpp>

class byte_string_view;
```
A `byte_string_view` refers to a constant contiguous sequence of byte-like objects with the first element of the sequence at position zero.
It  holds two members: a pointer to constant `uint8_t*` and a size.

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`const_iterator`|
`iterator`|Same as `const_iterator`
`size_type`|`std::size_t`
`value_type`|`uint8_t`
`reference`|`uint8_t&`
`const_reference`|`const uint8_t&`
`difference_type`|`std::ptrdiff_t`
`pointer`|`uint8_t*`
`const_pointer`|`const uint8_t*`

#### Constructor

    constexpr byte_string_view() noexcept;

    constexpr byte_string_view(const uint8_t* data, std::size_t length) noexcept;

    template <typename Container>
    constexpr explicit byte_string_view(const Container& cont); 

    constexpr byte_string_view(const byte_string_view&) noexcept = default;

    constexpr byte_string_view(byte_string_view&& other) noexcept;

#### Assignment

    byte_string_view& operator=(const byte_string_view& s) noexcept = default;

    byte_string_view& operator=(byte_string_view&& s) noexcept;

#### Iterators

    constexpr const_iterator begin() const noexcept;

    constexpr const_iterator end() const noexcept;

    constexpr const_iterator cbegin() const noexcept;

    constexpr const_iterator cend() const noexcept;

#### Element access

    constexpr const uint8_t* data() const noexcept;

    constexpr uint8_t operator[](size_type pos) const; 

    constexpr byte_string_view substr(size_type pos) const;

    constexpr byte_string_view substr(size_type pos, size_type n) const;

#### Capacity

    constexpr size_t size() const noexcept;

#### Non-member functions

    bool operator==(const byte_string_view& lhs, const byte_string_view& rhs);

    bool operator!=(const byte_string_view& lhs, const byte_string_view& rhs);

    template <typename CharT>
    friend std::ostream& operator<<(std::ostream& os, const byte_string_view& o);

### Examples

#### Display bytes

```cpp
std::vector<uint8_t> v = {'H','e','l','l','o',' ','W','o','r','l','d'};

std::cout << "(1) " << byte_string_view(v) << "\n\n";

std::cout << "(2) " << byte_string_view(v).substr(0, 5) << "\n\n";
```

Output:
```
(1) 48,65,6c,6c,6f,20,57,6f,72,6c,64

(2) 48,65,6c,6c,6f
```

#### Display bytes from std::string

```cpp
std::string s = {'H','e','l','l','o',' ','W','o','r','l','d'};

std::cout << "(1) " << byte_string_view(s) << "\n\n";

std::cout << "(2) " << byte_string_view(s).substr(0, 5) << "\n\n";
```

Output:
```
(1) 48,65,6c,6c,6f,20,57,6f,72,6c,64

(2) 48,65,6c,6c,6f
```
