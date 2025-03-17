// Range v3 library
//
//  Copyright Eric Niebler 2017-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/utility/scope_exit.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

RANGES_DIAGNOSTIC_IGNORE_UNNEEDED_MEMBER

namespace
{
    int i = 0;

    struct NoexceptFalse
    {
        NoexceptFalse() {}
        NoexceptFalse(NoexceptFalse const &) noexcept(false)
        {}

        NoexceptFalse(NoexceptFalse &&) noexcept(false)
        {
            CHECK(false);
        }

        void operator()() const
        {
            ++i;
        }
    };

    struct ThrowingCopy
    {
        ThrowingCopy() {}
        [[noreturn]] ThrowingCopy(ThrowingCopy const &) noexcept(false)
        {
            throw 42;
        }

        ThrowingCopy(ThrowingCopy &&) noexcept(false)
        {
            CHECK(false);
        }

        void operator()() const
        {
            ++i;
        }
    };
}

int main()
{
    std::cout << "\nTesting scope_exit\n";

    {
        auto guard = ranges::make_scope_exit([&]{++i;});
        CHECK(i == 0);
    }
    CHECK(i == 1);

    {
        auto guard = ranges::make_scope_exit(NoexceptFalse{});
        CHECK(i == 1);
    }
    CHECK(i == 2);

    try
    {
        auto guard = ranges::make_scope_exit(ThrowingCopy{});
        CHECK(false);
    }
    catch(int)
    {}
    CHECK(i == 3);

    return ::test_result();
}
