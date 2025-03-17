### jsoncons::jsonpath::basic_path_node

```cpp
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

template <typename CharT>                           (since 0.172.0)
class basic_path_node
```

Two specializations for common character types are defined:

Type      |Definition
----------|------------------------------
path_node   |`basic_path_node<char>` 
wpath_node  |`basic_path_node<wchar_t>` 

Objects of type `basic_path_node` represent a normalized path as a
singly linked list where each node has a pointer to its (shared) parent
node.

#### Member types
Type        |Definition
------------|------------------------------
char_type   | `CharT`
string_view_type | `jsoncons::basic_string_view<char_type>`

#### Constructors

    basic_path_node();                                        (1)

    basic_path_node(const basic_path_node* parent, 
        const string_view_type& name);                        (2)

    basic_path_node(const basic_path_node* parent, 
        std::size_t index);                                   (3)

    basic_path_node(const basic_path_node&);                  (4)

(1) Constructs an empty `basic_path_node` with a null parent
representing the root of a path.

(2) Constructs a `basic_path_node` from a name.

(3) Constructs a `basic_path_node` from an index.

(4) Copy constructor

#### operator=

    basic_path_node& operator=(const basic_path_node&);

#### Accessors

    const basic_path_node* parent() const;
Returns the parent of a path node

    std::size_t size() const;
Returns the number of nodes in the path.

    path_node_kind node_kind() const;
Returns the kind of the node

    const string_view_type& name() const;

    std::size_t index() const;

#### Non-member functions

    template <typename CharT,typename Allocator = std::allocator<CharT>>
    std::basic_string<CharT, std::char_traits<CharT>, Allocator> to_basic_string(const basic_path_node<CharT>& location,
        const Allocator& alloc = Allocator())
Returns a normalized path

    std::string to_string(const path_node& path)

    std::wstring to_wstring(const wpath_node& path)

