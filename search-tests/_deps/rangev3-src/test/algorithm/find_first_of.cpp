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

#include <range/v3/core.hpp>
#include <range/v3/algorithm/find_first_of.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"
#include "../test_utils.hpp"

namespace rng = ranges;

void test_iter()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 0, 1, 2, 3};
    static constexpr auto sa = size(ia);
    int ib[] = {1, 3, 5, 7};
    static constexpr auto sb = size(ib);
    CHECK(rng::find_first_of(InputIterator<const int*>(ia),
                             Sentinel<const int*>(ia + sa),
                             ForwardIterator<const int*>(ib),
                             Sentinel<const int*>(ib + sb)) ==
                             InputIterator<const int*>(ia+1));
    int ic[] = {7};
    CHECK(rng::find_first_of(InputIterator<const int*>(ia),
                             Sentinel<const int*>(ia + sa),
                             ForwardIterator<const int*>(ic),
                             Sentinel<const int*>(ic + 1)) ==
                             InputIterator<const int*>(ia+sa));
    CHECK(rng::find_first_of(InputIterator<const int*>(ia),
                             Sentinel<const int*>(ia + sa),
                             ForwardIterator<const int*>(ic),
                             Sentinel<const int*>(ic)) ==
                             InputIterator<const int*>(ia+sa));
    CHECK(rng::find_first_of(InputIterator<const int*>(ia),
                             Sentinel<const int*>(ia),
                             ForwardIterator<const int*>(ic),
                             Sentinel<const int*>(ic+1)) ==
                             InputIterator<const int*>(ia));
}

void test_iter_pred()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 0, 1, 2, 3};
    static constexpr auto sa = size(ia);
    int ib[] = {1, 3, 5, 7};
    static constexpr auto sb = size(ib);
    CHECK(rng::find_first_of(InputIterator<const int*>(ia),
                             Sentinel<const int*>(ia + sa),
                             ForwardIterator<const int*>(ib),
                             Sentinel<const int*>(ib + sb),
                             std::equal_to<int>()) ==
                             InputIterator<const int*>(ia+1));
    int ic[] = {7};
    CHECK(rng::find_first_of(InputIterator<const int*>(ia),
                             Sentinel<const int*>(ia + sa),
                             ForwardIterator<const int*>(ic),
                             Sentinel<const int*>(ic + 1),
                             std::equal_to<int>()) ==
                             InputIterator<const int*>(ia+sa));
    CHECK(rng::find_first_of(InputIterator<const int*>(ia),
                             Sentinel<const int*>(ia + sa),
                             ForwardIterator<const int*>(ic),
                             Sentinel<const int*>(ic),
                             std::equal_to<int>()) ==
                             InputIterator<const int*>(ia+sa));
    CHECK(rng::find_first_of(InputIterator<const int*>(ia),
                             Sentinel<const int*>(ia),
                             ForwardIterator<const int*>(ic),
                             Sentinel<const int*>(ic+1),
                             std::equal_to<int>()) ==
                             InputIterator<const int*>(ia));
}

void test_rng()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 0, 1, 2, 3};
    static constexpr auto sa = size(ia);
    int ib[] = {1, 3, 5, 7};
    static constexpr auto sb = size(ib);
    CHECK(rng::find_first_of(make_subrange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia + sa)),
                             make_subrange(ForwardIterator<const int*>(ib),
                             ForwardIterator<const int*>(ib + sb))) ==
                             InputIterator<const int*>(ia+1));
    CHECK(::is_dangling(rng::find_first_of(::MakeTestRange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia + sa)),
                             make_subrange(ForwardIterator<const int*>(ib),
                             ForwardIterator<const int*>(ib + sb)))));
    int ic[] = {7};
    CHECK(rng::find_first_of(make_subrange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia + sa)),
                             make_subrange(ForwardIterator<const int*>(ic),
                             ForwardIterator<const int*>(ic + 1))) ==
                             InputIterator<const int*>(ia+sa));
    CHECK(rng::find_first_of(make_subrange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia + sa)),
                             make_subrange(ForwardIterator<const int*>(ic),
                             ForwardIterator<const int*>(ic))) ==
                             InputIterator<const int*>(ia+sa));
    CHECK(rng::find_first_of(make_subrange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia)),
                             make_subrange(ForwardIterator<const int*>(ic),
                             ForwardIterator<const int*>(ic+1))) ==
                             InputIterator<const int*>(ia));
    CHECK(::is_dangling(rng::find_first_of(::MakeTestRange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia + sa)),
                             make_subrange(ForwardIterator<const int*>(ic),
                             ForwardIterator<const int*>(ic + 1)))));
    CHECK(::is_dangling(rng::find_first_of(::MakeTestRange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia + sa)),
                             make_subrange(ForwardIterator<const int*>(ic),
                             ForwardIterator<const int*>(ic)))));
    CHECK(::is_dangling(rng::find_first_of(::MakeTestRange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia)),
                             make_subrange(ForwardIterator<const int*>(ic),
                             ForwardIterator<const int*>(ic+1)))));
}

void test_rng_pred()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 0, 1, 2, 3};
    static constexpr auto sa = size(ia);
    int ib[] = {1, 3, 5, 7};
    static constexpr auto sb = size(ib);
    CHECK(rng::find_first_of(make_subrange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia + sa)),
                             make_subrange(ForwardIterator<const int*>(ib),
                             ForwardIterator<const int*>(ib + sb)),
                             std::equal_to<int>()) ==
                             InputIterator<const int*>(ia+1));
    int ic[] = {7};
    CHECK(rng::find_first_of(make_subrange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia + sa)),
                             make_subrange(ForwardIterator<const int*>(ic),
                             ForwardIterator<const int*>(ic + 1)),
                             std::equal_to<int>()) ==
                             InputIterator<const int*>(ia+sa));
    CHECK(rng::find_first_of(make_subrange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia + sa)),
                             make_subrange(ForwardIterator<const int*>(ic),
                             ForwardIterator<const int*>(ic)),
                             std::equal_to<int>()) ==
                             InputIterator<const int*>(ia+sa));
    CHECK(rng::find_first_of(make_subrange(InputIterator<const int*>(ia),
                             InputIterator<const int*>(ia)),
                             make_subrange(ForwardIterator<const int*>(ic),
                             ForwardIterator<const int*>(ic+1)),
                             std::equal_to<int>()) ==
                             InputIterator<const int*>(ia));
}

struct S
{
    int i;
};

void test_rng_pred_proj()
{
    using namespace ranges;
    S ia[] = {S{0}, S{1}, S{2}, S{3}, S{0}, S{1}, S{2}, S{3}};
    static constexpr auto sa = size(ia);
    S ib[] = {S{1}, S{3}, S{5}, S{7}};
    static constexpr auto sb = size(ib);
    CHECK(rng::find_first_of(make_subrange(InputIterator<const S*>(ia),
                             InputIterator<const S*>(ia + sa)),
                             make_subrange(ForwardIterator<const S*>(ib),
                             ForwardIterator<const S*>(ib + sb)),
                             std::equal_to<int>(), &S::i, &S::i) ==
                             InputIterator<const S*>(ia+1));
    S ic[] = {S{7}};
    CHECK(rng::find_first_of(make_subrange(InputIterator<const S*>(ia),
                             InputIterator<const S*>(ia + sa)),
                             make_subrange(ForwardIterator<const S*>(ic),
                             ForwardIterator<const S*>(ic + 1)),
                             std::equal_to<int>(), &S::i, &S::i) ==
                             InputIterator<const S*>(ia+sa));
    CHECK(rng::find_first_of(make_subrange(InputIterator<const S*>(ia),
                             InputIterator<const S*>(ia + sa)),
                             make_subrange(ForwardIterator<const S*>(ic),
                             ForwardIterator<const S*>(ic)),
                             std::equal_to<int>(), &S::i, &S::i) ==
                             InputIterator<const S*>(ia+sa));
    CHECK(rng::find_first_of(make_subrange(InputIterator<const S*>(ia),
                             InputIterator<const S*>(ia)),
                             make_subrange(ForwardIterator<const S*>(ic),
                             ForwardIterator<const S*>(ic+1)),
                             std::equal_to<int>(), &S::i, &S::i) ==
                             InputIterator<const S*>(ia));
}

void test_constexpr()
{
    using namespace ranges;
    constexpr int ia[] = {0, 1, 2, 3, 0, 1, 2, 3};
    constexpr auto sa = size(ia);
    constexpr int ib[] = {1, 3, 5, 7};
    constexpr auto sb = size(ib);
    STATIC_CHECK(
        rng::find_first_of(as_lvalue(make_subrange(InputIterator<const int *>(ia),
                                                   InputIterator<const int *>(ia + sa))),
                           make_subrange(ForwardIterator<const int *>(ib),
                                         ForwardIterator<const int *>(ib + sb)),
                           equal_to{}) == InputIterator<const int *>(ia + 1));
    constexpr int ic[] = {7};
    STATIC_CHECK(
        rng::find_first_of(as_lvalue(make_subrange(InputIterator<const int *>(ia),
                                                   InputIterator<const int *>(ia + sa))),
                           make_subrange(ForwardIterator<const int *>(ic),
                                         ForwardIterator<const int *>(ic + 1)),
                           equal_to{}) == InputIterator<const int *>(ia + sa));
    STATIC_CHECK(
        rng::find_first_of(as_lvalue(make_subrange(InputIterator<const int *>(ia),
                                                   InputIterator<const int *>(ia + sa))),
                           make_subrange(ForwardIterator<const int *>(ic),
                                         ForwardIterator<const int *>(ic)),
                           equal_to{}) == InputIterator<const int *>(ia + sa));
    STATIC_CHECK(
        rng::find_first_of(as_lvalue(make_subrange(InputIterator<const int *>(ia),
                                                   InputIterator<const int *>(ia))),
                           make_subrange(ForwardIterator<const int *>(ic),
                                         ForwardIterator<const int *>(ic + 1)),
                           equal_to{}) == InputIterator<const int *>(ia));
}

int main()
{
    ::test_iter();
    ::test_iter_pred();
    ::test_rng();
    ::test_rng_pred();
    ::test_rng_pred_proj();

    return ::test_result();
}
