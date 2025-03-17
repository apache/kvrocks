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

#include <list>
#include <range/v3/core.hpp>
#include <range/v3/view/counted.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

struct fortytwo_erator {
    using difference_type = int;
    using value_type = int;
    fortytwo_erator() = default;
    int operator*() const { return 42; }
    fortytwo_erator& operator++() { return *this; }
    void operator++(int) {}
};

int main()
{
    using namespace ranges;
    std::cout << "\nTesting counted\n";

    {
        int rgi[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        auto rng = views::counted(ForwardIterator<int*>{rgi}, 10);
        rng.size();
        CPP_assert(sized_range<decltype(rng)> && view_<decltype(rng)>);
        auto i = rng.begin();
        auto b = i.base();
        auto c = i.count();
        decltype(i) j{b, c};
        ::check_equal(rng, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        static_assert(std::is_same<decltype(i), counted_iterator<ForwardIterator<int*>>>::value, "");
    }

    {
        std::list<int> l;
        counted_iterator<std::list<int>::iterator> a(l.begin(), 0);
        counted_iterator<std::list<int>::const_iterator> b(l.begin(), 0);

        detail::ignore_unused(
            a-a,
            b-b,
            a-b,
            b-a);

        counted_iterator<char*> c(nullptr, 0);
        counted_iterator<char const*> d(nullptr, 0);
        detail::ignore_unused(
            c-c,
            d-d,
            c-d,
            d-c);
    }

    {
        // Regression test: ensure that we can post-increment a counted_iterator<I>
        // when decltype(declval<I &>()++) is void.
        CPP_assert(ranges::input_iterator<fortytwo_erator>);
        ranges::counted_iterator<fortytwo_erator> c{{}, 42};
        c++;
    }

    return ::test_result();
}
