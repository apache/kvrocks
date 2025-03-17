### jsoncons::basic_json

```cpp
#include <jsoncons/basic_json.hpp>

template< 
    class CharT,
    class Policy = sorted_policy,
    class Allocator = std::allocator<char>
> class basic_json;

namespace pmr {
    template <typename CharT,typename Policy>
    using basic_json = jsoncons::basic_json<CharT, Policy, std::pmr::polymorphic_allocator<char>>;
    using json = basic_json<char,sorted_policy>;                                                     (since 0.171.0)
    using wjson = basic_json<wchar_t,sorted_policy>;
    using ojson = basic_json<char, order_preserving_policy>;
    using wojson = basic_json<wchar_t, order_preserving_policy>;
}
```

A `basic_json` is a union type that can hold one of a number of possible data members, 
some that require an allocator (a long string, byte string, array, or object), 
and other trivially copyable ones that do not (an empty object, short string, number, boolean, or null). 
The data member may be tagged with a [semantic_tag](semantic_tag.md) that provides additional 
information about its value. The sizeof a `basic_json` regardless of its template parameters 
is normally 16 bytes.

A `basic_json` is allocator-aware, and supports allocator propagation to allocator-aware arrays
or objects. Every constructor has a version that accepts an allocator argument. 
A long string, byte string, array or object contains a pointer to underlying storage,
the allocator is used to allocate that storage, and it is retained in that storage.
For other data members the allocator argument is ignored. For more about allocators,
see <a href=json/allocators.md>Allocators</a>.

When assigned a new `basic_json` value, the old value is overwritten. The member data type of the new value may be different 
from the old value. 

A `basic_json` can support multiple readers concurrently, as long as it is not being modified.
If it is being modified, it must be by one writer with no concurrent readers.

Several aliases for common character types and policies for ordering an object's name/value pairs are provided:

Type                |Definition
--------------------|------------------------------
[jsoncons::json](json.md)     |`jsoncons::basic_json<char,jsoncons::sorted_policy,std::allocator<char>>`
[jsoncons::ojson](ojson.md)   |`jsoncons::basic_json<char,jsoncons::order_preserving_policy,std::allocator<char>>`
[jsoncons::wjson](wjson.md)   |`jsoncons::basic_json<wchar_t,jsoncons::jsoncons::sorted_policy,std::allocator<char>>`
[jsoncons::wojson](wojson.md) |`jsoncons::basic_json<wchar_t,jsoncons::order_preserving_policy,std::allocator<char>>`
`jsoncons::pmr::json` (0.171.0) |`jsoncons::pmr::basic_json<char,jsoncons::sorted_policy>`
`jsoncons::pmr::ojson` (0.171.0) |`jsoncons::pmr::basic_json<char,jsoncons::order_preserving_policy>`
`jsoncons::pmr::wjson` (0.171.0) |`jsoncons::pmr::basic_json<wchar_t,jsoncons::sorted_policy>`
`jsoncons::pmr::wojson` (0.171.0) |`jsoncons::pmr::basic_json<wchar_t,jsoncons::order_preserving_policy>`

#### Template parameters

<table border="0">
  <tr>
    <td>CharT</td>
    <td>Character type of text string</td> 
  </tr>
  <tr>
    <td>Policy</td>
    <td>Implementation policy for arrays and objects</td>
  </tr>
  <tr>
    <td>Allocator</td>
    <td>Allocator type for allocating internal storage for long strings, byte strings, arrays and objects.
    The allocator type may be a stateless allocator, a <a href=https://en.cppreference.com/w/cpp/memory/polymorphic_allocator>std::pmr::polymorphic_allocator</a>, 
or a <a href=https://en.cppreference.com/w/cpp/memory/scoped_allocator_adaptor>std::scoped_allocator_adaptor</a>.
Non-propagating stateful allocators, such as the <a href=https://www.boost.org/doc/libs/1_82_0/doc/html/interprocess/allocators_containers.html#interprocess.allocators_containers.allocator_introduction>Boost.Interprocess allocators</a>,
must be wrapped by a <code>std::scoped_allocator_adaptor</code>.</td>
  </tr>
</table>

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`char_type`|CharT
`policy_type`|Policy
`allocator_type` (until 0.171.0)|A stateless allocator, or a non-propagating stateful allocator
`allocator_type` (after 0.171.0)|A stateless allocator, [std::pmr::polymorphic_allocator](https://en.cppreference.com/w/cpp/memory/polymorphic_allocator), or [std::scoped_allocator_adaptor](https://en.cppreference.com/w/cpp/memory/scoped_allocator_adaptor)
`char_traits_type`|`std::char_traits<char_type>`
`char_allocator_type`|`allocator_type` rebound to `char_type`
`reference`|`basic_json&`
`const_reference`|`const basic_json&`
`pointer`|`basic_json*`
`const_pointer`|`const basic_json*`
`string_view_type`|`basic_string_view<char_type>`
`key_type`|A [ContiguousContainer](https://en.cppreference.com/w/cpp/named_req/ContiguousContainer) to `char_type`
`key_value_type`|`key_value<key_type,basic_json>`
`object_iterator`|A [RandomAccessIterator](http://en.cppreference.com/w/cpp/concept/RandomAccessIterator) to [key_value_type](json/key_value.md)
`const_object_iterator`|A const [RandomAccessIterator](http://en.cppreference.com/w/cpp/concept/RandomAccessIterator) to const [key_value_type](json/key_value.md)
`array_iterator`|A [RandomAccessIterator](http://en.cppreference.com/w/cpp/concept/RandomAccessIterator) to `basic_json`
`const_array_iterator`|A const [RandomAccessIterator](http://en.cppreference.com/w/cpp/concept/RandomAccessIterator) to `const basic_json`
`object_range_type`|range<object_iterator,const_array_iterator>                 (since 0.173.3)
`const_object_range_type`|range<const_object_iterator,const_array_iterator>     (since 0.173.3)
`array_range_type`|range<array_iterator,const_array_iterator>                   (since 0.173.3)
`const_array_range_type`|range<const_array_iterator,const_array_iterator>       (since 0.173.3)
`proxy_type`|proxy<basic_json>. The `proxy_type` class supports conversion to `basic_json&`. (removed in 1.0.0)

### Static member functions

<table border="0">
  <tr>
    <td><a href="json/parse.md">parse</a></td>
    <td>Parses JSON.</td> 
  </tr>
  <tr>
    <td><a href="json/make_array.md">make_array</a></td>
    <td>Makes a multidimensional basic_json array.</td> 
  </tr>
  <tr>
    <td><a>const basic_json& null()</a></td>
    <td>Returns a null value</td> 
  </tr>
</table>

### Member functions
<table border="0">
  <tr>
    <td><a href="json/constructor.md">(constructor)</a></td>
    <td>constructs the basic_json value</td> 
  </tr>
  <tr>
    <td><a href="json/destructor.md">(destructor)</a></td>
    <td>destructs the basic_json value</td> 
  </tr>
  <tr>
    <td><a href="json/operator=.md">operator=</a></td>
    <td>assigns values</td> 
  </tr>
</table>

    allocator_type get_allocator() const
For a long string, byte string, array, or object, returns the retained allocator used to allocate memory
for their storage, otherwise attempts to return a default constructed allocator. 

#### Ranges and Iterators

<table border="0">
  <tr>
    <td><a href="json/array_range.md">array_range</a></td>
    <td>Returns a range that supports a range-based for loop over the elements of a <code>basic_json</code> array.</td> 
  </tr>
  <tr>
    <td><a href="json/object_range.md">obect_range</a></td>
    <td>Returns a range that supports a range-based for loop over the key-value pairs of a <code>basic_json</code> object.</td> 
  </tr>
</table>

#### Capacity

<table border="0">
  <tr>
    <td><a>size_t size() const noexcept</a></td>
    <td>Returns the number of elements in a basic_json array, or the number of members in a <code><basic_json</code> object, or <code>zero</code></td> 
  </tr>
  <tr>
    <td><a>bool empty() const noexcept</a></td>
    <td>Returns <code>true</code> if a basic_json string, object or array has no elements, otherwise <code>false</code></td> 
  </tr>
  <tr>
    <td><a>size_t capacity() const</a></td>
    <td>Returns the size of the storage space currently allocated for a basic_json object or array</td> 
  </tr>
  <tr>
    <td><a>void reserve(std::size_t n)</a></td>
    <td>Increases the capacity of a basic_json object or array to allow at least <code>n</code> members or elements</td> 
  </tr>
  <tr>
    <td><a>void resize(std::size_t n)</a></td>
    <td>Resizes a basic_json array so that it contains <code>n</code> elements</td> 
  </tr>
  <tr>
    <td><a>void resize(std::size_t n, const basic_json& val)</a></td>
    <td>Resizes a basic_json array so that it contains <code>n</code> elements that are initialized to <code>val</code></td> 
  </tr>
  <tr>
    <td><a>void shrink_to_fit()</a></td>
    <td>Requests the removal of unused capacity</td> 
  </tr>
</table>

#### Accessors

<table border="0">
  <tr>
    <td>bool contains(const string_view_type& key) const noexcept</td>
    <td>Returns <code>true</code> if an object has a member with the given <code>key</code> , otherwise <code>false</code></td> 
  </tr>
  <tr>
    <td><a href="json/is.md">is</a></td>
    <td>Checks if a basic_json value matches a type.</td> 
  </tr>
  <tr>
    <td><a href="json/as.md">as</a></td>
    <td>Attempts to convert a basic_json value to a value of a type.</td> 
  </tr>
  <tr>
    <td><a href="json/operator_at.md">operator[]</a></td>
    <td>Access or insert specified element.</td> 
  </tr>
  <tr>
    <td><a href="json/at.md">at<br>at_or_null</a></td>
    <td>Return the specified value.</td> 
  </tr>
  <tr>
    <td><a href="json/get_value_or.md">get_value_or</a></td>
    <td>Return the specified value if available, otherwise a default value.</td> 
  </tr>
</table>

    semantic_tag tag() const
Returns the [semantic_tag](semantic_tag.md) associated with this value

    uint64_t ext_tag() const
If `tag()` == `semantic_tag::ext`, returns a format specific tag associated with a byte string value,
otherwise return 0. An example is a MessagePack `type` in the range 0-127 associated with the
MessagePack ext format family, or a CBOR tag preceeding a byte string. 

    json_type type() const
Returns the [json type](json_type.md) associated with this value
 
    object_iterator find(const string_view_type& name)
    const_object_iterator find(const string_view_type& name) const
Returns an object iterator to a member whose name compares equal to `name`. If there is no such member, returns `object_range.end()`.
Throws `std::domain_error` if not an object.  

#### Modifiers

<table border="0">
  <tr>
    <td><a>void clear()</a></td>
    <td>Remove all elements from an array or members from an object, otherwise do nothing</td> 
  </tr>
  <tr>
    <td><a href="json/erase.md">erase</a></td>
    <td>Erases array elements and object members</td> 
  </tr>
  <tr>
    <td><a href="json/push_back.md">push_back</a></td>
    <td>Adds a value to the end of a basic_json array</td> 
  </tr>
  <tr>
    <td><a href="json/insert.md">insert</a></td>
    <td>Inserts elements</td> 
  </tr>
  <tr>
    <td><a href="json/emplace_back.md">emplace_back</a></td>
    <td>Constructs a value in place at the end of a basic_json array</td> 
  </tr>
  <tr>
    <td><a href="json/emplace.md">emplace</a></td>
    <td>Constructs a value in place before a specified position in a basic_json array</td> 
  </tr>
  <tr>
    <td><a href="json/try_emplace.md">try_emplace</a></td>
    <td>Constructs a key-value pair in place in a basic_json object if the key does not exist, does nothing if the key exists</td> 
  </tr>
  <tr>
    <td><a href="json/insert_or_assign.md">insert_or_assign</a></td>
    <td>Inserts a key-value pair in a basic_json object if the key does not exist, or assigns a new value if the key already exists</td> 
  </tr>
  <tr>
    <td><a href="json/merge.md">merge</a></td>
    <td>Inserts another basic_json object's key-value pairs into a basic_json object, if they don't already exist.</td>
  </tr>
  <tr>
    <td><a href="json/merge_or_update.md">merge_or_update</a></td>
    <td>Inserts another basic_json object's key-value pairs into a basic_json object, or assigns them if they already exist.</td>
  </tr>
  <tr>
    <td><a>void swap(basic_json& val) noexcept</a></td>
    <td>Exchanges the content of the <code>basic_json</code> value with the content of <code>val</code>, which is another <code>basic_json</code> value</td>
  </tr>
</table>

#### Serialization

<table border="0">
  <tr>
    <td><a href="json/dump.md"</a>dump</td>
    <td>Serializes basic_json value to a string, stream, or <a href="./basic_json_visitor.md">basic_json_visitor</a>.</td> 
  </tr>
</table>

#### Non member functions

    bool operator==(const basic_json& lhs, const basic_json& rhs)
Returns `true` if two basic_json objects compare equal, `false` otherwise. 

    bool operator!=(const basic_json& lhs, const basic_json& rhs)
Returns `true` if two basic_json objects do not compare equal, `false` otherwise. 

    bool operator<(const basic_json& lhs, const basic_json& rhs)
Compares the contents of lhs and rhs lexicographically. 

    bool operator<=(const basic_json& lhs, const basic_json& rhs)
Compares the contents of lhs and rhs lexicographically. 

    bool operator>(const basic_json& lhs, const basic_json& rhs)
Compares the contents of lhs and rhs lexicographically. 

    bool operator>=(const basic_json& lhs, const basic_json& rhs)
Compares the contents of lhs and rhs lexicographically. 

    std::basic_istream<char_type>& operator>>(std::basic_istream<char_type>& is, basic_json& o)
Reads a `basic_json` value from a stream.

    std::basic_ostream<char_type>& operator<<(std::basic_ostream<char_type>& os, const basic_json& o)
Inserts basic_json value into stream.

    std::basic_ostream<char_type>& print(const basic_json& val)  
    std::basic_ostream<char_type>& print(const basic_json& val, const basic_json_options<CharT>& options)  
Inserts basic_json value into stream using the specified [basic_json_options](basic_json_options.md) if supplied.

    std::basic_ostream<char_type>& pretty_print(const basic_json& val)  
    std::basic_ostream<char_type>& pretty_print(const basic_json& val, const basic_json_options<CharT>& options)  
Inserts basic_json value into stream using the specified [basic_json_options](basic_json_options.md) if supplied.

    void swap(basic_json& a, basic_json& b) noexcept
Exchanges the values of `a` and `b`

