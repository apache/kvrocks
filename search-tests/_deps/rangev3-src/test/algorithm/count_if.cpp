// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/core.hpp>
#include <range/v3/algorithm/count_if.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

struct S
{
    int i;
};

struct T
{
    bool b;
    bool m() { return b; }
};

constexpr bool even(int i)
{
    return i % 2 == 0;
}

int main()
{
    using namespace ranges;
    auto equals = [](int i){ return std::bind(equal_to{}, i, std::placeholders::_1); };

    int ia[] = {0, 1, 2, 2, 0, 1, 2, 3};
    constexpr auto cia = size(ia);

    CHECK(count_if(InputIterator<const int*>(ia),
                   Sentinel<const int*>(ia + cia), equals(2)) == 3);
    CHECK(count_if(InputIterator<const int*>(ia),
                   Sentinel<const int*>(ia + cia), equals(7)) == 0);
    CHECK(count_if(InputIterator<const int*>(ia),
                   Sentinel<const int*>(ia), equals(2)) == 0);

    CHECK(count_if(make_subrange(InputIterator<const int*>(ia),
                         Sentinel<const int*>(ia + cia)), equals(2)) == 3);
    CHECK(count_if(make_subrange(InputIterator<const int*>(ia),
                         Sentinel<const int*>(ia + cia)), equals(7)) == 0);
    CHECK(count_if(make_subrange(InputIterator<const int*>(ia),
                         Sentinel<const int*>(ia)), equals(2)) == 0);

    S sa[] = {{0}, {1}, {2}, {2}, {0}, {1}, {2}, {3}};
    constexpr auto csa = size(ia);

    CHECK(count_if(InputIterator<const S*>(sa),
                   Sentinel<const S*>(sa + csa), equals(2), &S::i) == 3);
    CHECK(count_if(InputIterator<const S*>(sa),
                   Sentinel<const S*>(sa + csa), equals(7), &S::i) == 0);
    CHECK(count_if(InputIterator<const S*>(sa),
                   Sentinel<const S*>(sa), equals(2), &S::i) == 0);

    CHECK(count_if(make_subrange(InputIterator<const S*>(sa),
                         Sentinel<const S*>(sa + csa)), equals(2), &S::i) == 3);
    CHECK(count_if(make_subrange(InputIterator<const S*>(sa),
                         Sentinel<const S*>(sa + csa)), equals(7), &S::i) == 0);
    CHECK(count_if(make_subrange(InputIterator<const S*>(sa),
                         Sentinel<const S*>(sa)), equals(2), &S::i) == 0);

    T ta[] = {{true}, {false}, {true}, {false}, {false}, {true}, {false}, {false}, {true}, {false}};
    CHECK(count_if(InputIterator<T*>(ta),
                   Sentinel<T*>(ta + size(ta)), &T::m) == 4);
    CHECK(count_if(InputIterator<T*>(ta),
                   Sentinel<T*>(ta + size(ta)), &T::b) == 4);
    CHECK(count_if(make_subrange(InputIterator<T*>(ta),
                         Sentinel<T*>(ta + size(ta))), &T::m) == 4);
    CHECK(count_if(make_subrange(InputIterator<T*>(ta),
                         Sentinel<T*>(ta + size(ta))), &T::b) == 4);

    {
        using IL = std::initializer_list<int>;
        STATIC_CHECK(ranges::count_if(IL{0, 1, 2, 1, 3, 1, 4}, even) == 3);
        STATIC_CHECK(ranges::count_if(IL{1, 1, 3, 1}, even) == 0);
    }

    return ::test_result();
}
