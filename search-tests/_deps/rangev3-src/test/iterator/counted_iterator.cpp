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

#include <range/v3/iterator/counted_iterator.hpp>

#include "../simple_test.hpp"

// iterator that models input_iterator (has void-returning postfix increment operator)
struct Iterator
{
    using value_type = int;
    using difference_type = int;

    int counter = 0;

    int operator*() const { return counter; }
    Iterator& operator++() { ++counter; return *this; }
    void operator++(int) { ++counter; }
    bool operator==(const Iterator& rhs) const { return counter == rhs.counter; }
    bool operator!=(const Iterator& rhs) const { return !(*this == rhs); }
};

int main()
{
    CPP_assert(ranges::input_iterator<Iterator>);
    auto cnt = ranges::counted_iterator<Iterator>(Iterator(), 1);
    CHECK(*cnt == 0);
    cnt++;
    CHECK(cnt == ranges::default_sentinel);

    return test_result();
}
