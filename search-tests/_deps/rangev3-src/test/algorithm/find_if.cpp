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
#include <range/v3/algorithm/find_if.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

struct S
{
    int i_;
};

constexpr bool is_three(int i)
{
    return i == 3;
}

template<class Rng>
constexpr bool contains_three(Rng r)
{
    auto it = ranges::find_if(r, is_three);
    return it != ranges::end(r);
}

int main()
{
    using namespace ranges;

    int ia[] = {0, 1, 2, 3, 4, 5};
    constexpr auto s = size(ia);

    {
        InputIterator<const int*> r = find_if(InputIterator<const int*>(ia),
                                              InputIterator<const int*>(ia + s),
                                              [](int i){return i == 3;});
        CHECK(*r == 3);
        r = find_if(InputIterator<const int*>(ia),
                    InputIterator<const int*>(ia+s),
                    [](int i){return i == 10;});
        CHECK(r == InputIterator<const int*>(ia+s));

        r = find_if(InputIterator<const int*>(ia),
                    Sentinel<const int*>(ia+s),
                    [](int i){return i == 3;});
        CHECK(*r == 3);
        r = find_if(InputIterator<const int*>(ia),
                    Sentinel<const int*>(ia+s),
                    [](int i){return i == 10;});
        CHECK(r == InputIterator<const int*>(ia+s));
    }

    {
        int *pi = find_if(ia, [](int i){return i == 3;});
        CHECK(*pi == 3);
        pi = find_if(ia, [](int i){return i == 10;});
        CHECK(pi == ia+s);
    }

#ifndef RANGES_WORKAROUND_MSVC_573728
    {
        auto pj0 = find_if(std::move(ia), [](int i){return i == 3;});
        CHECK(::is_dangling(pj0));
        auto pj1 = find_if(std::move(ia), [](int i){return i == 10;});
        CHECK(::is_dangling(pj1));
    }
#endif // RANGES_WORKAROUND_MSVC_573728

    {
        std::vector<int> const vec(begin(ia), end(ia));
        auto pj0 = find_if(std::move(vec), [](int i){return i == 3;});
        CHECK(::is_dangling(pj0));
        auto pj1 = find_if(std::move(vec), [](int i){return i == 10;});
        CHECK(::is_dangling(pj1));
    }

    {
        auto* ignore = find_if(ranges::views::all(ia), [](int i){return i == 10;});
        (void)ignore;
    }

    {
        S sa[] = {{0}, {1}, {2}, {3}, {4}, {5}};
        S *ps = find_if(sa, [](int i){return i == 3;}, &S::i_);
        CHECK(ps->i_ == 3);
        ps = find_if(sa, [](int i){return i == 10;}, &S::i_);
        CHECK(ps == end(sa));
    }

    {
        using IL = std::initializer_list<int>;
        STATIC_CHECK(contains_three(IL{0, 1, 2, 3}));
        STATIC_CHECK(!contains_three(IL{0, 1, 2}));
    }

    return ::test_result();
}
