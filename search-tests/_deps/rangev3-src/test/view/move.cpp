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

#include <cstring>
#include <string>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/view/move.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/utility/copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;
    static const char * const data[] = {"'allo", "'allo", "???"};
    std::vector<MoveOnlyString> vs(begin(data), end(data));
    auto x = vs | views::move;

    {
        CPP_assert(common_range<decltype(x)>);
        CPP_assert(sized_range<decltype(x)>);
        CPP_assert(view_<decltype(x)>);
        CPP_assert(common_range<decltype(x)>);
        CPP_assert(sized_range<decltype(x)>);
        CPP_assert(random_access_iterator<decltype(x.begin())>);
        using I = decltype(x.begin());
        CPP_assert(same_as<iterator_tag_of<I>, std::random_access_iterator_tag>);
        CPP_assert(same_as<
            typename std::iterator_traits<I>::iterator_category,
            std::random_access_iterator_tag>);

        CHECK(bool(*x.begin() == "'allo"));
    }

    {
        std::vector<MoveOnlyString> vs2(x.begin(), x.end());
        static_assert(std::is_same<MoveOnlyString&&, decltype(*x.begin())>::value, "");
        ::check_equal(vs2, {"'allo", "'allo", "???"});
        ::check_equal(vs, {"", "", ""});
    }

    {
        MoveOnlyString rgs[] = {"can", "you", "hear", "me", "now?"};
        auto rng = debug_input_view<MoveOnlyString>{rgs} | views::move;
        MoveOnlyString target[sizeof(rgs) / sizeof(rgs[0])];
        copy(rng, target);
        ::check_equal(rgs, {"", "", "", "", ""});
        ::check_equal(target, {"can", "you", "hear", "me", "now?"});
    }

    return test_result();
}
