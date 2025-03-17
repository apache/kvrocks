### Allocators

The class `basic_json` has an `Allocator` template parameter and an `allocator_type` member that indicates
that it is allocator aware. `Allocator` must be a Scoped Allocator, that is, an allocator 
that applies not only to a `basic_json`'s data member, but also to its data member's elements.
In particular, `Allocator` must be either a stateless allocator, 
a <a href=https://en.cppreference.com/w/cpp/memory/polymorphic_allocator>std::pmr::polymorphic_allocator</a>, 
or a <a href=https://en.cppreference.com/w/cpp/memory/scoped_allocator_adaptor>std::scoped_allocator_adaptor</a>. 
Non-propagating stateful allocators, such as the [Boost.Interprocess allocators](https://www.boost.org/doc/libs/1_82_0/doc/html/interprocess/allocators_containers.html#interprocess.allocators_containers.allocator_introduction),
must be wrapped by a [std::scoped_allocator_adaptor](https://en.cppreference.com/w/cpp/memory/scoped_allocator_adaptor).

Every constructor has a version that accepts an allocator argument, this is required for
allocator propogation. A long string, byte string, array or object makes use of the
allocator argument to allocate storage. For the other data members (short string, 
number, boolean, or null) the allocator argument is ignored.

A long string, byte string, array and object data member all contain a pointer `ptr` obtained
from an earlier call to `allocate`. `ptr` points to storage that contains both 
the data representing the long string, byte string, array or object, and the 
allocator used to allocate that storage. To later deallocate that storage, the allocator is
retrieved with `ptr->get_allocator()`.  

#### Propagation

The allocator applies not only to a `basic_json`'s data member, but also to its data member's elements. For example:

```cpp
char buffer[1024];
std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
std::pmr::polymorphic_allocator<char> alloc(&pool);

std::string key = "key too long for short string";
std::string value = "string too long for short string";

jsoncons::pmr::json j{ jsoncons::json_object_arg, alloc };
assert(j.get_allocator().resource() == &pool);

j.try_emplace(key, value);

auto it = std::search(std::begin(buffer), std::end(buffer), key.begin(), key.end());
assert(it != std::end(buffer));
it = std::search(std::begin(buffer), std::end(buffer), value.begin(), value.end());
assert(it != std::end(buffer));
```

#### Copy construction

The copy constructor 

```
Json j1(j);
```

constructs `j1` from the contents of `j`. If `j` holds a long string, bytes string, array or object,
copy construction applies allocator traits `select_on_container_copy_construction` to
the allocator from `j` (since 0.178.0) For example: 

```cpp
char buffer[1024];
std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
std::pmr::polymorphic_allocator<char> alloc(&pool);

jsoncons::pmr::json j{ "String too long for short string", alloc };
assert(j.is_string());
assert(j.get_allocator().resource() == &pool);

jsoncons::pmr::json j1(j);
assert(j1 == j);
assert(j1.get_allocator() == std::allocator_traits<std::pmr::polymorphic_allocator<char>>::
    select_on_container_copy_construction(j.get_allocator()));
assert(j1.get_allocator() == std::pmr::polymorphic_allocator<char>{}); // expected result for pmr allocators
```

#### Allocator-extended copy construction

The allocator-extended copy constructor 

```
Json j1(j, alloc);
```

constructs `j1` from the contents of `j` using `alloc` as the allocator. If `j` holds a long string, bytes string, array or object,
copy construction uses allocator `alloc` for allocating storage, otherwise `alloc` is ignored. For example: 

```cpp
char buffer[1024];
std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
std::pmr::polymorphic_allocator<char> alloc(&pool);

char buffer1[1024];
std::pmr::monotonic_buffer_resource pool1{ std::data(buffer1), std::size(buffer1) };
std::pmr::polymorphic_allocator<char> alloc1(&pool1);

jsoncons::pmr::json j{ "String too long for short string", alloc };
assert(j.is_string());
assert(j.get_allocator().resource() == &pool);

jsoncons::pmr::json j1(j, alloc1);
assert(j1 == j);
assert(j1.get_allocator().resource() == &pool1);
```

#### Move construction

The move constructor 

```
Json j1(std::move(j));
```

constructs `j1` by taking the contents of `j`, which has either a pointer or a trivially copyable value,
and replaces it with `null`. For example: 

```
char buffer[1024];
std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
std::pmr::polymorphic_allocator<char> alloc(&pool);

jsoncons::pmr::json j{ "String too long for short string", alloc};
assert(j.is_string());
assert(j.get_allocator().resource() == &pool);

jsoncons::pmr::json j1(std::move(j));
assert(j1.is_string());
assert(j1.get_allocator().resource() == &pool);
assert(j.is_null());
```

#### Allocator-extended move construction

The allocator-extended move constructor 

```
Json j1(std::move(j), alloc);
```

constructs `j1` with a copy of the data member `j`, using `alloc` as the allocator. For example:

```cpp
char buffer[1024];
std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
std::pmr::polymorphic_allocator<char> alloc(&pool);

char buffer1[1024];
std::pmr::monotonic_buffer_resource pool1{ std::data(buffer1), std::size(buffer1) };
std::pmr::polymorphic_allocator<char> alloc1(&pool1);

jsoncons::pmr::json j{ "String too long for short string", alloc };
assert(j.is_string());
assert(j.get_allocator().resource() == &pool);

jsoncons::pmr::json j1(std::move(j), alloc1);
assert(j1.is_string());
assert(j1.get_allocator().resource() == &pool1);
```

#### Copy assignment

If the left side is a long string, byte string, array or object, uses its allocator,
otherwise, if the right side is a long string, byte string, array or object,
constructs a copy of the right side as if using copy construction, i.e.
applies allocator traits `select_on_container_copy_construction` to the
right side allocator. For example:

```cpp
char buffer[1024];
std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
std::pmr::polymorphic_allocator<char> alloc(&pool);

char buffer1[1024];
std::pmr::monotonic_buffer_resource pool1{ std::data(buffer1), std::size(buffer1) };
std::pmr::polymorphic_allocator<char> alloc1(&pool1);

jsoncons::pmr::json j{ "String too long for short string", alloc };
assert(j.is_string());
assert(j.get_allocator().resource() == &pool);

// copy long string to number
jsoncons::pmr::json j1{10};
j1 = j;
assert(j1.is_string());
assert(j1.get_allocator() == std::allocator_traits<std::pmr::polymorphic_allocator<char>>::
    select_on_container_copy_construction(j.get_allocator()));
assert(j1.get_allocator() == std::pmr::polymorphic_allocator<char>{});

// copy long string to array
jsoncons::pmr::json j2{ jsoncons::json_array_arg, {1,2,3,4}, 
    jsoncons::semantic_tag::none, alloc1};
j2 = j;
assert(j2.is_string());
assert(j2.get_allocator().resource() == &pool1);
```

`basic_json` does not check allocator traits `propagate_on_container_copy_assignment`.
In the case of arrays and objects, `basic_json` leaves it to the implementing types (by
default `std::vector`) to conform to the traits.

#### Move assignment

If either `j` or `j1` are a long string, byte string, array, or object, `basic_json` move assignment 

```
j1 = std::move(j);
```

swaps the two data member values, such that two pointers are swapped or a pointer and a
trivially copyable value are swapped. Otherwise, move assignment copies `j`'s value to `j1`
and leaves `j` unchanged. For example:

```cpp
char buffer[1024];
std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
std::pmr::polymorphic_allocator<char> alloc(&pool);

jsoncons::pmr::json j{ "String too long for short string", alloc };
assert(j.is_string());
assert(j.get_allocator().resource() == &pool);

jsoncons::pmr::json j1{10};
assert(j1.is_number());

j1 = std::move(j);
assert(j1.is_string());
assert(j1.get_allocator().resource() == &pool);
assert(j.is_number());
```

`basic_json` does not check allocator traits `propagate_on_container_move_assignment`.
It simply swaps pointers, or a pointer and a trivial value, or copies a trivial value.

#### Fancy pointers

`basic_json` is compatible with boost [boost::interprocess::offset_ptr](https://www.boost.org/doc/libs/1_86_0/doc/html/interprocess/offset_ptr.html),
provided that the implementing containers for arrays and objects are also compatible. In the example below, boost containers are used. 
 
```cpp
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <cstdlib> //std::system
#include <jsoncons/json.hpp>
#include <scoped_allocator>

using shmem_allocator = boost::interprocess::allocator<int,
    boost::interprocess::managed_shared_memory::segment_manager>;

using cust_allocator = std::scoped_allocator_adaptor<shmem_allocator>;

struct boost_sorted_policy 
{
    template <typename KeyT,typename Json>
    using object = jsoncons::sorted_json_object<KeyT,Json,boost::interprocess::vector>;

    template <typename Json>
    using array = jsoncons::json_array<Json,boost::interprocess::vector>;

    template <typename CharT,typename CharTraits,typename Allocator>
    using member_key = boost::interprocess::basic_string<CharT, CharTraits, Allocator>;
};

using cust_json = jsoncons::basic_json<char,boost_sorted_policy, cust_allocator>;

int main(int argc, char *argv[])
{
   typedef std::pair<double, int> MyType;

   if (argc == 1) // Parent process
   {  
      //Remove shared memory on construction and destruction
      struct shm_remove
      {
         shm_remove() { boost::interprocess::shared_memory_object::remove("MySharedMemory"); }
         ~shm_remove() noexcept { boost::interprocess::shared_memory_object::remove("MySharedMemory"); }
      } remover;

      //Construct managed shared memory
      boost::interprocess::managed_shared_memory segment(boost::interprocess::create_only, 
                                                         "MySharedMemory", 65536);

      //Initialize shared memory STL-compatible allocator
      const shmem_allocator alloc(segment.get_segment_manager());

      // Create json value with all dynamic allocations in shared memory

      cust_json* j = segment.construct<cust_json>("MyJson")(jsoncons::json_array_arg, alloc);
      j->push_back(10);

      cust_json o(jsoncons::json_object_arg, alloc);
      o.try_emplace("category", "reference");
      o.try_emplace("author", "Nigel Rees");
      o.insert_or_assign("title", "Sayings of the Century");
      o.insert_or_assign("price", 8.95);

      j->push_back(o);

      cust_json a(jsoncons::json_array_arg, 2,  cust_json(jsoncons::json_object_arg, alloc), 
          jsoncons::semantic_tag::none, alloc);
      a[0]["first"] = 1;

      j->push_back(a);

      std::pair<cust_json*, boost::interprocess::managed_shared_memory::size_type> res;
      res = segment.find<cust_json>("MyJson");

      std::cout << "Parent process:\n";
      std::cout << pretty_print(*(res.first)) << "\n\n";

      //Launch child process
      std::string s(argv[0]); s += " child ";
      if (0 != std::system(s.c_str()))
         return 1;

      //Check child has destroyed all objects
      if (segment.find<MyType>("MyJson").first)
         return 1;
   }
   else // Child process
   {
      //Open managed shared memory
      boost::interprocess::managed_shared_memory segment(boost::interprocess::open_only, 
          "MySharedMemory");

      std::pair<cust_json*, boost::interprocess::managed_shared_memory::size_type> res;
      res = segment.find<cust_json>("MyJson");

      if (res.first != nullptr)
      {
          std::cout << "Child process:\n";
          std::cout << pretty_print(*(res.first)) << "\n";
      }
      else
      {
          std::cout << "Result is null\n";
      }

      //We're done, delete all the objects
      segment.destroy<cust_json>("MyJson");
   }
   return 0;
}
```

Output:

```
Parent process:
[
    10,
    {
        "author": "Nigel Rees",
        "category": "reference",
        "price": 8.95,
        "title": "Sayings of the Century"
    },
    [
        {
            "first": 1
        },
        {}
    ]
]

Child process:
[
    10,
    {
        "author": "Nigel Rees",
        "category": "reference",
        "price": 8.95,
        "title": "Sayings of the Century"
    },
    [
        {
            "first": 1
        },
        {}
    ]
]
```

#### References

[An allocator-aware variant type](https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2024/p3153r0.html)

[C++ named requirements: Allocator](https://en.cppreference.com/w/cpp/named_req/Allocator)

[std::allocator_traits](https://en.cppreference.com/w/cpp/memory/allocator_traits)

[std::pmr::polymorphic_allocator](https://en.cppreference.com/w/cpp/memory/polymorphic_allocator)

[std::scoped_allocator_adaptor](https://en.cppreference.com/w/cpp/memory/scoped_allocator_adaptor)

[Towards meaningful fancy pointers](https://quuxplusone.github.io/draft/fancy-pointers.html)


