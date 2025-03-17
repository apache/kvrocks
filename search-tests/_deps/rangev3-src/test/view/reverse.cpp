// Range v3 library
//
//  Copyright Eric Niebler 2015-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <list>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/take_exactly.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/counted.hpp>
#include <range/v3/view/delimit.hpp>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/c_str.hpp>
#include <range/v3/view/zip.hpp>
#include <range/v3/utility/copy.hpp>
#include <range/v3/algorithm/find.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    // Reverse a random-access, common, sized range
    std::vector<int> rgv{0,1,2,3,4,5,6,7,8,9};
    auto const rng0 = rgv | views::reverse;
    CPP_assert(view_<std::remove_const_t<decltype(rng0)>>);
    CPP_assert(random_access_range<decltype(rng0)>);
    CPP_assert(common_range<decltype(rng0)>);
    CPP_assert(sized_range<decltype(rng0)>);
    CHECK(rng0.size() == 10u);
    ::check_equal(rng0, {9,8,7,6,5,4,3,2,1,0});
    ::check_equal(rng0 | views::reverse, {0,1,2,3,4,5,6,7,8,9});
    ::check_equal(rng0 | views::reverse | views::reverse, {9,8,7,6,5,4,3,2,1,0});
    ::check_equal(rng0 | views::reverse | views::reverse | views::reverse, {0,1,2,3,4,5,6,7,8,9});

    {
        auto z = views::zip(rgv);
        auto rz = z | views::reverse;
        CPP_assert(same_as<range_value_t<decltype(z)>, range_value_t<decltype(rz)>>);
    }

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
#if defined(__clang__) && __clang_major__ < 6
    // Workaround https://bugs.llvm.org/show_bug.cgi?id=33314
    RANGES_DIAGNOSTIC_PUSH
    RANGES_DIAGNOSTIC_IGNORE_UNDEFINED_FUNC_TEMPLATE
#endif
    {
        ranges::reverse_view dg0{rgv};
        ::check_equal(dg0, {9, 8, 7, 6, 5, 4, 3, 2, 1, 0});
        ranges::reverse_view dg1{dg0};
#ifdef RANGES_WORKAROUND_MSVC_934330
        ::check_equal(dg1, {9, 8, 7, 6, 5, 4, 3, 2, 1, 0});
#else // ^^^ "workaround" / no "workaround" vvv
        ::check_equal(dg1, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
#endif // RANGES_WORKAROUND_MSVC_934330
    }
#if defined(__clang__) && __clang_major__ < 6
    RANGES_DIAGNOSTIC_POP
#endif // clang bug workaround
#endif // use deduction guides

    // Reverse another random-access, non-common, sized range
    auto cnt = counted_view<std::vector<int>::iterator>(rgv.begin(), 10);
    CPP_assert(!common_range<decltype(cnt)>);
    auto const rng1 = rgv | views::reverse;
    CPP_assert(view_<std::remove_const_t<decltype(rng1)>>);
    CPP_assert(random_access_range<decltype(rng1)>);
    CPP_assert(common_range<decltype(rng1)>);
    CPP_assert(sized_range<decltype(rng1)>);
    CHECK(rng1.size() == 10u);
    ::check_equal(rng1, {9,8,7,6,5,4,3,2,1,0});
    ::check_equal(rng1 | views::reverse, {0,1,2,3,4,5,6,7,8,9});

    // Reverse a random-access, non-common, non-sized range
    auto sz = views::c_str((char const*)"hello");
    auto rng2 = sz | views::reverse;
    CPP_assert(view_<decltype(rng2)>);
    CPP_assert(random_access_range<decltype(rng2)>);
    CPP_assert(common_range<decltype(rng2)>);
    CPP_assert(!sized_range<decltype(detail::as_const(rng2))>);
    CPP_assert(sized_range<decltype(rng2)>);
    auto const & crng2 = rng2;
    CPP_assert(!range<decltype(crng2)>);
    ::check_equal(rng2, {'o','l','l','e','h'});
    ::check_equal(rng2 | views::reverse, {'h','e','l','l','o'});

    // Reverse a bidirectional, common, sized range
    std::list<int> rgl{0,1,2,3,4,5,6,7,8,9};
    auto const rng3 = rgl | views::reverse;
    CPP_assert(view_<std::remove_const_t<decltype(rng3)>>);
    CPP_assert(bidirectional_range<decltype(rng3)>);
    CPP_assert(!random_access_range<decltype(rng3)>);
    CPP_assert(common_range<decltype(rng3)>);
    CPP_assert(sized_range<decltype(rng3)>);
    CHECK(rng3.size() == 10u);
    ::check_equal(rng3, {9,8,7,6,5,4,3,2,1,0});
    ::check_equal(rng3 | views::reverse, {0,1,2,3,4,5,6,7,8,9});

    // Reverse a bidirectional, weak, sized range
    auto cnt2 = views::counted(rgl.begin(), 10);
    auto rng4 = cnt2 | views::reverse;
    CPP_assert(view_<decltype(rng4)>);
    CPP_assert(bidirectional_range<decltype(rng4)>);
    CPP_assert(!random_access_range<decltype(rng4)>);
    CPP_assert(common_range<decltype(rng4)>);
    CPP_assert(sized_range<decltype(rng4)>);
    CHECK(rng4.size() == 10u);
    auto const & crng4 = rng4;
    CPP_assert(!range<decltype(crng4)>);
    ::check_equal(rng4, {9,8,7,6,5,4,3,2,1,0});
    ::check_equal(rng4 | views::reverse, {0,1,2,3,4,5,6,7,8,9});

    // Reverse a bidirectional, weak, non-sized range
    auto dlm = views::delimit(rgl.begin(), 9);
    CPP_assert(!common_range<decltype(dlm)>);
    auto rng5 = dlm | views::reverse;
    CPP_assert(view_<decltype(rng5)>);
    CPP_assert(bidirectional_range<decltype(rng5)>);
    CPP_assert(!random_access_range<decltype(rng5)>);
    CPP_assert(common_range<decltype(rng5)>);
    CPP_assert(!sized_range<decltype(rng5)>);
    auto const & crng5 = rng5;
    CPP_assert(!range<decltype(crng5)>);
    ::check_equal(rng5, {8,7,6,5,4,3,2,1,0});
    ::check_equal(rng5 | views::reverse, {0,1,2,3,4,5,6,7,8});

    // Reverse a bidirectional, weak, non-sized range
    auto dlm2 = views::delimit(rgl, 10);
    CPP_assert(!common_range<decltype(dlm2)>);
    auto rng6 = dlm2 | views::reverse;
    CPP_assert(view_<decltype(rng6)>);
    CPP_assert(bidirectional_range<decltype(rng6)>);
    CPP_assert(!random_access_range<decltype(rng6)>);
    CPP_assert(common_range<decltype(rng6)>);
    CPP_assert(!sized_range<decltype(rng6)>);
    auto const & crng6 = rng6;
    CPP_assert(!range<decltype(crng6)>);
    ::check_equal(rng6, {9,8,7,6,5,4,3,2,1,0});
    ::check_equal(rng6 | views::reverse, {0,1,2,3,4,5,6,7,8,9});

    {
        std::vector<int> v = {1, 2, 3, 4, 5};
        auto b = find(v, 2);
        auto e = find(v | views::reverse, 4).base();
        ::check_equal(make_subrange(b, e), {2, 3, 4});

        auto e2 = find(v | views::filter([](int i){ return i%2 == 0;})
                         | views::reverse, 4);
        CHECK(::is_dangling(e2));
    }

    return test_result();
}
