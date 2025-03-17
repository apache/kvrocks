### jsoncons::jsonpath::basic_path_element

```cpp
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

template <typename CharT,typename Allocator>
class basic_path_element   (since 0.172.0)
```

Two specializations for common character types are defined:

Type      |Definition
----------|------------------------------
path_element   |`basic_path_element<char>` 
wpath_element  |`basic_path_element<wchar_t>`

Objects of type `basic_path_element` represent an element (name or index) of a normalized path.

#### Member types
Type        |Definition
------------|------------------------------
char_type   | `CharT`
allocator_type | Allocator
char_allocator_type | Rebound Allocator for `char_type`
string_type | `std::basic_string<char_type,std::char_traits<char_type>,char_allocator_type>`

#### Constructors

    basic_path_element(const char_type* name, std::size_t length, 
        const Allocator& alloc = Allocator());                                      (1)

    explicit basic_path_element(const string_type& name);                           (2)

    explicit basic_path_element(string_type&& name);                                (3)

    basic_path_element(std::size_t index, 
        const Allocator& alloc = Allocator());                                      (4)           

    basic_path_element(const basic_path_element&);                                  (5)

    basic_path_element(basic_path_element&&) noexcept;                              (6)

(1)-(3) Constructs a `basic_path_element` from a name.

(4) Constructs a `basic_path_element` from an index.

#### operator=

    basic_path_element& operator=(const basic_path_element&);

    basic_path_element& operator=(basic_path_element&&);

#### Accessors

    bool has_name() const;
Checks if the element has a name

    bool has_index() const;
Checks if the element has an index

    const string_type& name() const
Returns the name 

    std::size_t index() const 
Returns the index

