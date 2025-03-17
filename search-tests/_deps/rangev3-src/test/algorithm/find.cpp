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

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/find.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

struct S
{
    int i_;
};

template<class Rng, class T>
constexpr T ret_val(Rng r, T val)
{
    auto rng = r;
    auto pi = ranges::find(rng, val);
    return *pi;
}
template<class Rng, class T>
constexpr bool found(Rng r, T val)
{
    auto rng = r;
    auto pi = ranges::find(rng, val);
    return pi != ranges::end(rng);
}

int main()
{
    using namespace ranges;

    int ia[] = {0, 1, 2, 3, 4, 5};
    constexpr auto s = size(ia);

    {
        InputIterator<const int*> r = find(InputIterator<const int*>(ia),
                                            InputIterator<const int*>(ia+s), 3);
        CHECK(*r == 3);
        r = find(InputIterator<const int*>(ia),
                 InputIterator<const int*>(ia+s), 10);
        CHECK(r == InputIterator<const int*>(ia+s));

        r = find(InputIterator<const int*>(ia),
                 Sentinel<const int*>(ia+s), 3);
        CHECK(*r == 3);
        r = find(InputIterator<const int*>(ia),
                 Sentinel<const int*>(ia+s), 10);
        CHECK(r == InputIterator<const int*>(ia+s));
    }

    {
        int *pi = find(ia, 3);
        CHECK(*pi == 3);
        pi = find(ia, 10);
        CHECK(pi == ia+s);
    }

    {
#ifndef RANGES_WORKAROUND_MSVC_573728
        auto pj0 = find(std::move(ia), 3);
        CHECK(::is_dangling(pj0));
#endif // RANGES_WORKAROUND_MSVC_573728
        std::vector<int> vec(begin(ia), end(ia));
        auto pj1 = find(std::move(vec), 3);
        CHECK(::is_dangling(pj1));
        auto pj2 = find(views::all(ia), 10);
        CHECK(pj2 == ia+s);
    }

    {
        S sa[] = {{0}, {1}, {2}, {3}, {4}, {5}};
        S *ps = find(sa, 3, &S::i_);
        CHECK(ps->i_ == 3);
        ps = find(sa, 10, &S::i_);
        CHECK(ps == end(sa));
    }

    {
        // https://github.com/Microsoft/Range-V3-VS2015/issues/9
        auto vec = std::vector<std::string>{{"a"}, {"b"}, {"c"}};
        auto it = ranges::find(vec, "b");
        CHECK(it == vec.begin() + 1);
    }

    {
        using IL = std::initializer_list<int>;
        STATIC_CHECK(ret_val(IL{1, 2}, 2) == 2);
        STATIC_CHECK(found(IL{1, 3, 4}, 4));
        STATIC_CHECK(!found(IL{1, 3, 4}, 5));
    }

    return ::test_result();
}
