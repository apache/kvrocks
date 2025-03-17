// Range v3 library
//
//  Copyright Andrew Sutton 2014
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/core.hpp>
#include <range/v3/algorithm/count.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

struct S
{
    int i;
};

int main()
{
    using namespace ranges;

    int ia[] = {0, 1, 2, 2, 0, 1, 2, 3};
    constexpr auto cia = size(ia);

    CHECK(count(InputIterator<const int*>(ia),
                Sentinel<const int*>(ia + cia), 2) == 3);
    CHECK(count(InputIterator<const int*>(ia),
                Sentinel<const int*>(ia + cia), 7) == 0);
    CHECK(count(InputIterator<const int*>(ia),
                Sentinel<const int*>(ia), 2) == 0);

    CHECK(count(make_subrange(InputIterator<const int*>(ia),
                      Sentinel<const int*>(ia + cia)), 2) == 3);
    CHECK(count(make_subrange(InputIterator<const int*>(ia),
                      Sentinel<const int*>(ia + cia)), 7) == 0);
    CHECK(count(make_subrange(InputIterator<const int*>(ia),
                      Sentinel<const int*>(ia)), 2) == 0);

    S sa[] = {{0}, {1}, {2}, {2}, {0}, {1}, {2}, {3}};
    constexpr auto csa = size(ia);

    CHECK(count(InputIterator<const S*>(sa),
                Sentinel<const S*>(sa + csa), 2, &S::i) == 3);
    CHECK(count(InputIterator<const S*>(sa),
                Sentinel<const S*>(sa + csa), 7, &S::i) == 0);
    CHECK(count(InputIterator<const S*>(sa),
                Sentinel<const S*>(sa), 2, &S::i) == 0);

    CHECK(count(make_subrange(InputIterator<const S*>(sa),
                      Sentinel<const S*>(sa + csa)), 2, &S::i) == 3);
    CHECK(count(make_subrange(InputIterator<const S*>(sa),
                      Sentinel<const S*>(sa + csa)), 7, &S::i) == 0);
    CHECK(count(make_subrange(InputIterator<const S*>(sa),
                      Sentinel<const S*>(sa)), 2, &S::i) == 0);

    {
        using IL = std::initializer_list<int>;
        STATIC_CHECK(ranges::count(IL{0, 1, 2, 1, 3, 1, 4}, 1) == 3);
        STATIC_CHECK(ranges::count(IL{0, 1, 2, 1, 3, 1, 4}, 5) == 0);
    }

    return ::test_result();
}
