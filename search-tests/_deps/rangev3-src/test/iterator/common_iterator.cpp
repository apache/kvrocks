// Range v3 library
//
//  Copyright Eric Niebler 2015-present
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#include <algorithm>
#include <numeric>
#include <range/v3/iterator/common_iterator.hpp>
#include <range/v3/iterator/unreachable_sentinel.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_UNNEEDED_MEMBER

namespace {
    struct silly_arrow_cursor {
        int read() const { return 0; }
        void next() {}
        int arrow() const { return 42; }
    };

    int forty_two = 42;
    struct lvalue_iterator {
        using difference_type = int;
        using value_type = int;
        int& operator*() const { return forty_two; }
        lvalue_iterator& operator++() & { return *this; }
        lvalue_iterator operator++(int) & { return *this; }
    };
    struct xvalue_iterator : lvalue_iterator {
        int&& operator*() const { return std::move(forty_two); }
        xvalue_iterator& operator++() & { return *this; }
        xvalue_iterator operator++(int) & { return *this; }
    };

    struct proxy_cursor {
        int read() const { return 42; }
        void next() {}
    };

    void test_operator_arrow() {
        // I is a pointer type
        {
            int i = 42;
            auto ci = ranges::common_iterator<int*, ranges::unreachable_sentinel_t>{&i};
            CPP_assert(ranges::same_as<int*, decltype(ci.operator->())>);
            CHECK(ci.operator->() == &i);
        }
        // the expression i.operator->() is well-formed
        {
            using I = ranges::basic_iterator<silly_arrow_cursor>;
            auto ci = ranges::common_iterator<I, ranges::unreachable_sentinel_t>{};
            CPP_assert(ranges::same_as<I, decltype(ci.operator->())>);
            CHECK(ci.operator->().operator->() == 42);
        }
        // the expression *i is a glvalue [lvalue case]
        {
            auto ci = ranges::common_iterator<lvalue_iterator, ranges::unreachable_sentinel_t>{};
            CPP_assert(ranges::same_as<int*, decltype(ci.operator->())>);
            CHECK(ci.operator->() == &forty_two);
        }
        // the expression *i is a glvalue [xvalue case]
        {
            auto ci = ranges::common_iterator<xvalue_iterator, ranges::unreachable_sentinel_t>{};
            CPP_assert(ranges::same_as<int*, decltype(ci.operator->())>);
            CHECK(ci.operator->() == &forty_two);
        }
        // Otherwise, returns a proxy object
        {
            using I = ranges::basic_iterator<proxy_cursor>;
            auto ci = ranges::common_iterator<I, ranges::unreachable_sentinel_t>{};
            using A = decltype(ci.operator->());
            CPP_assert(std::is_class<A>::value);
            CPP_assert(!std::is_same<I, A>::value);
            CHECK(*ci.operator->().operator->() == 42);
        }
    }
}

int main() {
    {
        CPP_assert(
            ranges::forward_iterator<
                ranges::common_iterator<
                    BidirectionalIterator<const char *>,
                    Sentinel<const char *>>>);
        CPP_assert(
            !ranges::bidirectional_iterator<
                ranges::common_iterator<
                    BidirectionalIterator<const char *>,
                    Sentinel<const char *>>>);
        CPP_assert(
            std::is_same<
                ranges::common_reference<
                    ranges::common_iterator<
                        BidirectionalIterator<const char *>,
                        Sentinel<const char *>
                    >&,
                    ranges::common_iterator<
                        BidirectionalIterator<const char *>,
                        Sentinel<const char *>
                    >
                >::type,
                ranges::common_iterator<
                    BidirectionalIterator<const char *>,
                    Sentinel<const char *>
                >
            >::value);
        // Sized iterator range tests
        CPP_assert(
            !ranges::sized_sentinel_for<
                ranges::common_iterator<
                    ForwardIterator<int*>,
                    Sentinel<int*, true> >,
                ranges::common_iterator<
                    ForwardIterator<int*>,
                    Sentinel<int*, true> > >);
        CPP_assert(
            ranges::sized_sentinel_for<
                ranges::common_iterator<
                    RandomAccessIterator<int*>,
                    Sentinel<int*, true> >,
                ranges::common_iterator<
                    RandomAccessIterator<int*>,
                    Sentinel<int*, true> > >);
        CPP_assert(
            !ranges::sized_sentinel_for<
                ranges::common_iterator<
                    RandomAccessIterator<int*>,
                    Sentinel<int*, false> >,
                ranges::common_iterator<
                    RandomAccessIterator<int*>,
                    Sentinel<int*, false> > >);
    }
    {
        int rgi[] {0,1,2,3,4,5,6,7,8,9};
        using CI = ranges::common_iterator<
            RandomAccessIterator<int*>,
            Sentinel<int*>>;
        CI first{RandomAccessIterator<int*>{rgi}};
        CI last{Sentinel<int*>{rgi+10}};
        CHECK(std::accumulate(first, last, 0, std::plus<int>{}) == 45);
    }

    test_operator_arrow();

    return test_result();
}
