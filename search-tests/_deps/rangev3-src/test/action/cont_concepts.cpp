// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

#include <array>
#include <vector>
#include <memory>
#include <range/v3/core.hpp>
#include <range/v3/action/concepts.hpp>
#include <range/v3/view/ref.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    int rgi[6];
    CPP_assert(range<decltype(rgi)>);
    CPP_assert(!semi_container<decltype(rgi)>);

    std::array<int, 6> a;
    CPP_assert(semi_container<decltype(a)>);
    CPP_assert(!container<decltype(a)>);

    std::vector<int> v;
    CPP_assert(container<decltype(v)>);

    std::vector<std::unique_ptr<int>> v2;
    CPP_assert(container<decltype(v2)>);

    CPP_assert(lvalue_container_like<decltype((v2))>);
    CPP_assert(!lvalue_container_like<decltype(std::move(v2))>);

    CPP_assert(lvalue_container_like<decltype(views::ref(v2))>);

    return ::test_result();
}
