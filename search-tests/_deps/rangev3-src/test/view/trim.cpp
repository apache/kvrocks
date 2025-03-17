// Range v3 library
//
//  Copyright Johel Guerrero 2019
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#include <concepts/concepts.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/range/operations.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range_fwd.hpp>
#include <range/v3/utility/copy.hpp>
#include <range/v3/view/addressof.hpp>
#include <range/v3/view/drop.hpp>
#include <range/v3/view/drop_while.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/subrange.hpp>
#include <range/v3/view/tail.hpp>
#include <range/v3/view/trim.hpp>

#include "../test_iterators.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 4, 3, 2, 1, 2, 3, 4, 3, 2, 1, 0};
    int ib[] =             {4, 3, 2, 1, 2, 3, 4};
    constexpr auto bs = distance(ib);
    auto p = [](int i) { return i < 4; };

    auto rng = views::trim(ia, p);
    static_assert(same_as<iterator_t<decltype(rng)>, iterator_t<decltype(ia)>>, "");
    CPP_assert(view_<decltype(rng)>);
    CPP_assert(contiguous_range<decltype(rng)>);
    CPP_assert(common_range<decltype(rng)>);
    CPP_assert(!sized_range<decltype(rng)>);
    check_equal(rng, ib);

    auto && rng2 = views::trim(ib, p);
    check_equal(views::addressof(rng2), views::addressof(ib));

    auto rng3 = ia | views::drop(4) | views::trim(p);
    static_assert(same_as<iterator_t<decltype(rng3)>, iterator_t<decltype(rng3.base())>>, "");
    CPP_assert(view_<decltype(rng3)>);
    CPP_assert(contiguous_range<decltype(rng3)>);
    CPP_assert(common_range<decltype(rng3)>);
    CPP_assert(!sized_range<decltype(rng3)>);
    check_equal(rng3, ib);

    auto rng4 = ia | views::reverse | views::drop(4) | views::trim(p);
    static_assert(same_as<iterator_t<decltype(rng4)>, iterator_t<decltype(rng4.base())>>, "");
    CPP_assert(view_<decltype(rng4)>);
    CPP_assert(random_access_range<decltype(rng4)>);
    CPP_assert(common_range<decltype(rng4)>);
    CPP_assert(!sized_range<decltype(rng4)>);
    check_equal(rng4, ib);

    check_equal(views::trim(ia, p), ia | views::drop_while(p) | views::reverse | views::drop_while(p));

    auto rng5 = make_subrange(ib + 1, ib + bs - 1) | views::trim(p);
    static_assert(same_as<iterator_t<decltype(rng5)>, iterator_t<decltype(rng5.base())>>, "");
    CPP_assert(view_<decltype(rng5)>);
    CPP_assert(contiguous_range<decltype(rng5)>);
    CPP_assert(common_range<decltype(rng5)>);
    CPP_assert(!sized_range<decltype(rng5)>);
    CHECK(empty(rng5));

    auto rng6 = make_subrange(ib, ib + bs - 1) | views::trim(p);
    CHECK(distance(rng6) == 1);
    check_equal(&front(rng6), ib);

    auto rng7 = ib | views::tail | views::trim(p);
    static_assert(same_as<iterator_t<decltype(rng7)>, iterator_t<decltype(rng7.base())>>, "");
    CPP_assert(view_<decltype(rng7)>);
    CPP_assert(contiguous_range<decltype(rng7)>);
    CPP_assert(common_range<decltype(rng7)>);
    CPP_assert(!sized_range<decltype(rng7)>);
    CHECK(distance(rng7) == 1);
    check_equal(&front(rng7), ib + bs - 1);

    auto rng8 = make_subrange(BidirectionalIterator<const int*>(ia),
        BidirectionalIterator<const int*>(ia + distance(ia))) | views::trim(p);
    static_assert(same_as<iterator_t<decltype(rng8)>, iterator_t<decltype(rng8.base())>>, "");
    CPP_assert(view_<decltype(rng8)>);
    CPP_assert(bidirectional_range<decltype(rng8)>);
    CPP_assert(common_range<decltype(rng8)>);
    CPP_assert(!sized_range<decltype(rng8)>);

    return test_result();
}
