// Range v3 library
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <cstddef>
#include <iterator>
#include <range/v3/iterator.hpp>

struct X { };

namespace std {
    template<> struct iterator_traits<X> { };
}

struct Y {
    using difference_type = std::ptrdiff_t;
    using value_type = int;
    using pointer = int*;
    using reference = int&;
    using iterator_category = std::forward_iterator_tag;
};

static_assert(ranges::detail::is_std_iterator_traits_specialized_v<X>, "");
static_assert(!ranges::detail::is_std_iterator_traits_specialized_v<Y>, "");
static_assert(!ranges::detail::is_std_iterator_traits_specialized_v<int*>, "");

int main()
{
}
