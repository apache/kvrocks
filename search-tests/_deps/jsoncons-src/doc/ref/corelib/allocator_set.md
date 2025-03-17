### jsoncons::allocator_set

```cpp
#include <jsoncons/allocator_set.hpp>

template< 
    class Allocator,class TempAllocator
> allocator_set;
```

Member type                         |Definition
------------------------------------|------------------------------
`allocator_type`|`Allocator`
`temp_allocator_type`|`TempAllocator`

#### Constructors

    allocator_set(const Allocator& alloc=Allocator(), const TempAllocator& temp_alloc=TempAllocator())
Constructs an `allocator_set` with an allocator for result data and a
second allocator for temporary allocations. 

    allocator_set(const allocator_set& other)
Copy constructor. 

    allocator_set(allocator_set&& other)

Move constructor. 

#### Accessors

    Allocator get_allocator() const;
Returns an allocator object for result data

    TempAllocator get_temp_allocator() const;
Returns an allocator object for for temporary allocations

#### Helper functions

    template <typename Allocator,class TempAllocator>
    allocator_set<Allocator,TempAllocator> combine_allocators(
        const Allocator& alloc, const TempAllocator& temp_alloc);

Combines an allocator for result data and an allocator for temporary allocations into an `allocator_set` object,
deducing the allocator types from the types of the arguments.

    allocator_set<std::allocator<char>,std::allocator<char>> combine_allocators()
Creates an `allocator_set<std::allocator<char>,std::allocator<char>>` object with default allocators for result data
and temporary allocations.

    template <typename Allocator>
    allocator_set<Allocator,std::allocator<char>> combine_allocators(
        const Allocator& alloc);

Creates an `allocator_set` with the provided allocator for result data and
defaulting to a `std::allocator<char>` for temporary allocations. 

    template <typename TempAllocator>
    allocator_set<std::allocator<char>,TempAllocator> temp_allocator_only(const TempAllocator& temp_alloc)

Creates a `allocator_set` object, defaulting the result allocator type to `std::allocator<char>`
and deducing the temp allocator type from the type of the `temp_alloc` argument.


