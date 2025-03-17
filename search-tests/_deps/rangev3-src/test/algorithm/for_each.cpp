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

#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/for_each.hpp>

#include "../array.hpp"
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

struct S
{
    void p() const { *p_ += i_; }
    int *p_;
    int i_;
};

constexpr int void_f(int const &)
{
    return 3;
}

int main()
{
    int sum = 0;
    auto fun = [&](int i){sum += i; };
    std::vector<int> v1 { 0, 2, 4, 6 };
    CHECK(ranges::for_each(v1.begin(), v1.end(), fun).in == v1.end());
    CHECK(ranges::for_each(v1, fun).in == v1.end());
    CHECK(sum == 24);

    sum = 0;
    auto rfun = [&](int & i){sum += i; };
    CHECK(ranges::for_each(v1.begin(), v1.end(), rfun).in == v1.end());
    CHECK(ranges::for_each(v1, rfun).in == v1.end());
    CHECK(sum == 24);

    sum = 0;
    std::vector<S> v2{{&sum, 0}, {&sum, 2}, {&sum, 4}, {&sum, 6}};
    CHECK(ranges::for_each(v2.begin(), v2.end(), &S::p).in == v2.end());
    CHECK(ranges::for_each(v2, &S::p).in == v2.end());
    CHECK(sum == 24);

    sum = 0;
    CHECK(::is_dangling(ranges::for_each(::MakeTestRange(v1.begin(), v1.end()), fun).in));
    CHECK(sum == 12);

    {
        constexpr auto rng = test::array<int, 4>{{0, 2, 4, 6}};
        STATIC_CHECK(ranges::for_each(rng, void_f).in == ranges::end(rng));
    }

    return ::test_result();
}
