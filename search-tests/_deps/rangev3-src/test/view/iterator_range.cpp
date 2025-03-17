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
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/utility/copy.hpp>
#include <range/v3/iterator/unreachable_sentinel.hpp>
#include <range/v3/iterator_range.hpp>
#include <range/v3/view/all.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

struct empty
{};

int main()
{
    std::vector<int> vi{1,2,3,4};

    using namespace ranges;
    iterator_range<std::vector<int>::iterator> r0 {vi.begin(), vi.end()};
    CPP_assert(view_<decltype(r0)>);
    CPP_assert(sized_range<decltype(r0)>);
    CHECK(r0.size() == 4u);
    CHECK(r0.begin() == vi.begin());
    CHECK(r0.end() == vi.end());
    CHECK(ranges::get<0>(r0) == vi.begin());
    CHECK(ranges::get<1>(r0) == vi.end());
    ++r0.begin();
    CHECK(r0.size() == 3u);

    std::pair<std::vector<int>::iterator, std::vector<int>::iterator> p0 = r0;
    CHECK(p0.first == vi.begin()+1);
    CHECK(p0.second == vi.end());

    iterator_range<std::vector<int>::iterator, unreachable_sentinel_t> r1 { r0.begin(), {} };
    CPP_assert(view_<decltype(r1)>);
    CPP_assert(!sized_range<decltype(r1)>);
    CHECK(r1.begin() == vi.begin()+1);
    r1.end() = unreachable;

    ++r0.begin();
    CHECK(r0.begin() == vi.begin()+2);
    CHECK(r0.size() == 2u);
    --r0.end();
    CHECK(r0.end() == vi.end()-1);
    CHECK(r0.size() == 1u);
    CHECK(r0.front() == 3);
    CHECK(r0.back() == 3);

    std::pair<std::vector<int>::iterator, unreachable_sentinel_t> p1 = r1;
    CHECK(p1.first == vi.begin()+1);

    iterator_range<std::vector<int>::iterator, unreachable_sentinel_t> r2 { p1 };
    CHECK(r1.begin() == vi.begin()+1);

    std::list<int> li{1,2,3,4};
    sized_iterator_range<std::list<int>::iterator> l0 {li.begin(), li.end(), li.size()};
    CPP_assert(view_<decltype(l0)>);
    CPP_assert(sized_range<decltype(l0)>);
    CHECK(l0.begin() == li.begin());
    CHECK(l0.end() == li.end());
    CHECK(l0.size() == li.size());

    return ::test_result();
}
