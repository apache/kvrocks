/// \file
// Range v3 library
//
//  Copyright Andrey Diduh 2019
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#include <sstream>
#include <vector>

#include <range/v3/core.hpp>
#include <range/v3/view/addressof.hpp>
#include <range/v3/view/facade.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/take.hpp>

#include "../simple_test.hpp"
#include "../test_utils.hpp"

using namespace ranges;

void simple_test()
{
    std::vector<int> list = {1,2,3};

    auto out = list | views::addressof;

    check_equal(out, {&list[0], &list[1], &list[2]});
}

struct test_istream_range
  : view_facade<test_istream_range, unknown>
{
private:
    friend range_access;

    std::vector<int> *list;

    struct cursor
    {
    private:
        std::size_t i = 0;
        std::vector<int> *list = nullptr;
    public:
        cursor() = default;
        explicit cursor(std::vector<int> &list_)
          : list(&list_)
        {}
        void next()
        {
            ++i;
        }
        int &read() const noexcept
        {
            return (*list)[i];
        }
        bool equal(default_sentinel_t) const
        {
            return i == list->size();
        }
    };

    cursor begin_cursor()
    {
        return cursor{*list};
    }
public:
    test_istream_range() = default;
    explicit test_istream_range(std::vector<int> &list_)
      : list(&list_)
    {}
};

void test_input_range()
{
    // It is implementation dependent,
    // for how long returned reference remains valid.
    // It should be valid at least until next read.
    // For test purposes we use custom input range.

    std::vector<int> list{1, 2, 3};
    auto rng = test_istream_range(list);
    CPP_assert(input_range<decltype(rng)>);

    auto out = rng | views::addressof;
    check_equal(out, {&list[0], &list[1], &list[2]});
}

struct test_xvalue_range
  : view_facade<test_xvalue_range, unknown>
{
private:
    friend range_access;

    struct cursor
    {
        cursor() = default;
        void next();
        int &&read() const noexcept;
        bool equal(default_sentinel_t) const;
    };

    cursor begin_cursor();
};

template<typename, typename = void>
constexpr bool can_view = false;
template<typename R>
constexpr bool can_view<R,
    meta::void_<decltype(views::addressof(std::declval<R>()))>> = true;

// prvalue ranges cannot be passed to views::addressof
CPP_assert(!can_view<decltype(views::iota(0, 3))>);
// xvalue ranges cannot be passed to views::addressof
CPP_assert(!can_view<test_xvalue_range>);

int main()
{
    simple_test();
    test_input_range();

    return test_result();
}
