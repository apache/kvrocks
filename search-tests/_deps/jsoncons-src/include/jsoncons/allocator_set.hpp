// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_ALLOCATOR_SET_HPP
#define JSONCONS_ALLOCATOR_SET_HPP

#include <memory>

#include <jsoncons/tag_type.hpp>

namespace jsoncons {

template <typename Allocator,typename TempAllocator >
class allocator_set
{
    Allocator result_alloc_;
    TempAllocator temp_alloc_;
public:
    using allocator_type = Allocator;
    using temp_allocator_type = TempAllocator;

    allocator_set(const Allocator& alloc=Allocator(), 
        const TempAllocator& temp_alloc=TempAllocator())
        : result_alloc_(alloc), temp_alloc_(temp_alloc)
    {
    }

    allocator_set(const allocator_set&)  = default;
    allocator_set(allocator_set&&)  = default;
    allocator_set& operator=(const allocator_set&)  = delete;
    allocator_set& operator=(allocator_set&&)  = delete;
    ~allocator_set() = default;

    Allocator get_allocator() const {return result_alloc_;}
    TempAllocator get_temp_allocator() const {return temp_alloc_;}
};

inline
allocator_set<std::allocator<char>,std::allocator<char>> combine_allocators()
{
    return allocator_set<std::allocator<char>,std::allocator<char>>(std::allocator<char>(), std::allocator<char>());
}

template <typename Allocator>
allocator_set<Allocator,std::allocator<char>> combine_allocators(const Allocator& alloc)
{
    return allocator_set<Allocator,std::allocator<char>>(alloc, std::allocator<char>());
}

template <typename Allocator,typename TempAllocator >
allocator_set<Allocator,TempAllocator> combine_allocators(const Allocator& alloc, const TempAllocator& temp_alloc)
{
    return allocator_set<Allocator,TempAllocator>(alloc, temp_alloc);
}

template <typename TempAllocator >
allocator_set<std::allocator<char>,TempAllocator> temp_allocator_only(const TempAllocator& temp_alloc)
{
    return allocator_set<std::allocator<char>,TempAllocator>(std::allocator<char>(), temp_alloc);
}

} // namespace jsoncons

#endif // JSONCONS_ALLOCATOR_SET_HPP
