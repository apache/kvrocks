// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Gonzalo Brito Gadeschi 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <vector>
#include <range/v3/algorithm/equal.hpp>
#include <range/v3/view/c_str.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/core.hpp>
#include "../simple_test.hpp"

int main()
{
    {
        std::vector<int> vi{1,2,3,4};
        CHECK(ranges::index(vi, 0) == 1);
        CHECK(ranges::index(vi, 1) == 2);
        CHECK(ranges::index(vi, 2) == 3);
        CHECK(ranges::index(vi, 3) == 4);

        CHECK(ranges::at(vi, 0) == 1);
        CHECK(ranges::at(vi, 1) == 2);
        CHECK(ranges::at(vi, 2) == 3);
        CHECK(ranges::at(vi, 3) == 4);

        try
        {
            ranges::at(vi, 4);
            CHECK(false);
        }
        catch(std::out_of_range const& e)
        {
            CHECK(ranges::equal(ranges::views::c_str(e.what()),
                                ranges::views::c_str("ranges::at")));
        }

        try
        {
            ranges::at(vi, -1);
            CHECK(false);
        }
        catch(std::out_of_range const& e)
        {
            CHECK(ranges::equal(ranges::views::c_str(e.what()),
                                ranges::views::c_str("ranges::at")));
        }

        auto viv = ranges::make_subrange(vi.begin(), vi.end());
        CHECK(viv.at(0) == 1);
        CHECK(viv.at(1) == 2);
        CHECK(viv.at(2) == 3);
        CHECK(viv.at(3) == 4);

        try
        {
            viv.at(4);
            CHECK(false);
        }
        catch(std::out_of_range const& e)
        {
            CHECK(ranges::equal(ranges::views::c_str(e.what()),
                                ranges::views::c_str("view_interface::at")));
        }

        try
        {
            viv.at(-1);
            CHECK(false);
        }
        catch(std::out_of_range const& e)
        {
            CHECK(ranges::equal(ranges::views::c_str(e.what()),
                                ranges::views::c_str("view_interface::at")));
        }

        const auto cviv = viv;
        CHECK(cviv.at(0) == 1);
        CHECK(cviv.at(1) == 2);
        CHECK(cviv.at(2) == 3);
        CHECK(cviv.at(3) == 4);

        try
        {
            cviv.at(4);
            CHECK(false);
        }
        catch(std::out_of_range const& e)
        {
            CHECK(ranges::equal(ranges::views::c_str(e.what()),
                                ranges::views::c_str("view_interface::at")));
        }

        try
        {
            cviv.at(-1);
            CHECK(false);
        }
        catch(std::out_of_range const& e)
        {
            CHECK(ranges::equal(ranges::views::c_str(e.what()),
                                ranges::views::c_str("view_interface::at")));
        }
    }

    {
        auto rng = ranges::views::ints(std::int64_t{0}, std::numeric_limits<std::int64_t>::max());
        CHECK(ranges::index(rng, std::numeric_limits<std::int64_t>::max() - 1) ==
              std::numeric_limits<std::int64_t>::max() - 1);
        CHECK(ranges::at(rng, std::numeric_limits<std::int64_t>::max() - 1) ==
              std::numeric_limits<std::int64_t>::max() - 1);
    }

    #if RANGES_CXX_CONSTEXPR >= RANGES_CXX_CONSTEXPR_14
    {
        constexpr int vi[4] = {1, 2, 3, 4};
        constexpr int vi0 = ranges::index(vi, 0);
        static_assert(vi0 == 1, "");
        constexpr int vi1 = ranges::at(vi, 1);
        static_assert(vi1 == 2, "");
    }
    #endif

    return ::test_result();
}
