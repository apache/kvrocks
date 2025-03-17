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

#include <initializer_list>
#include <range/v3/algorithm/ends_with.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/view/subrange.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

int comparison_count = 0;

template<typename T>
bool counting_equals(const T &a, const T &b)
{
    ++comparison_count;
    return a == b;
}

int main()
{
    using namespace ranges;
    int ia[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    constexpr auto as = distance(ia);
    int ib[] = {5, 6, 7, 8, 9};
    constexpr auto bs = distance(ib);
    CHECK(ends_with(RandomAccessIterator<const int*>(ia),
                     RandomAccessIterator<const int*>(ia + as),
                     RandomAccessIterator<const int*>(ib),
                     RandomAccessIterator<const int*>(ib + bs)));
    CHECK(!ends_with(InputIterator<const int*>(ia),
                      InputIterator<const int*, true>(ia + as),
                      InputIterator<const int*>(ib),
                      InputIterator<const int*, true>(ib + bs - 1)));
    CHECK(!ends_with(ForwardIterator<const int*>(ia),
                      Sentinel<const int*>(ia + as),
                      ForwardIterator<const int*>(ib),
                      Sentinel<const int*>(ib + bs - 1)));
    CHECK(!ends_with(make_subrange(RandomAccessIterator<const int*>(ia),
                      RandomAccessIterator<const int*>(ia + as)),
                      make_subrange(RandomAccessIterator<const int*>(ib),
                      RandomAccessIterator<const int*>(ib + bs - 1))));
    CHECK(ends_with(make_subrange(InputIterator<const int*>(ia),
                     InputIterator<const int*, true>(ia + as)),
                     make_subrange(InputIterator<const int*>(ib),
                     InputIterator<const int*, true>(ib + bs))));
    CHECK(ends_with(make_subrange(ForwardIterator<const int*>(ia),
                     Sentinel<const int*>(ia + as)),
                     make_subrange(ForwardIterator<const int*>(ib),
                     Sentinel<const int*>(ib + bs))));
    comparison_count = 0;
    CHECK(!ends_with(RandomAccessIterator<const int*>(ib),
                      RandomAccessIterator<const int*>(ib + bs),
                      RandomAccessIterator<const int*>(ia),
                      RandomAccessIterator<const int*>(ia + as),
                      counting_equals<int>));
    CHECK(comparison_count == 0);
    comparison_count = 0;
    CHECK(ends_with(InputIterator<const int*>(ia),
                     InputIterator<const int*, true>(ia + as),
                     InputIterator<const int*>(ib),
                     InputIterator<const int*, true>(ib + bs),
                     counting_equals<int>));
    CHECK(comparison_count > 0);
    comparison_count = 0;
    CHECK(ends_with(ForwardIterator<const int*>(ia),
                     Sentinel<const int*>(ia + as),
                     ForwardIterator<const int*>(ib),
                     Sentinel<const int*>(ib + bs),
                     counting_equals<int>));
    CHECK(comparison_count > 0);
    comparison_count = 0;
    CHECK(ends_with(make_subrange(RandomAccessIterator<const int*>(ia),
                     RandomAccessIterator<const int*>(ia + as)),
                     make_subrange(RandomAccessIterator<const int*>(ib),
                     RandomAccessIterator<const int*>(ib + bs)),
                     counting_equals<int>));
    CHECK(comparison_count > 0);
    comparison_count = 0;
    CHECK(!ends_with(make_subrange(InputIterator<const int*>(ib),
                      InputIterator<const int*, true>(ib + bs - 1)),
                      make_subrange(InputIterator<const int*>(ib),
                      InputIterator<const int*, true>(ib + bs)),
                      counting_equals<int>));
    CHECK(comparison_count == 0);
    comparison_count = 0;
    CHECK(!ends_with(make_subrange(ForwardIterator<const int*>(ia),
                      Sentinel<const int*>(ia)),
                      make_subrange(ForwardIterator<const int*>(ib),
                      Sentinel<const int*>(ib + bs)),
                      counting_equals<int>));
    CHECK(comparison_count == 0);

#if RANGES_CXX_CONSTEXPR >= RANGES_CXX_CONSTEXPR_14 && RANGES_CONSTEXPR_INVOKE
    using IL = std::initializer_list<int>;
    static_assert(ends_with(IL{0, 1, 2, 3, 4}, IL{3, 4}), "");
    static_assert(!ends_with(IL{0, 1, 2, 3, 4}, IL{2, 3}), "");
    static_assert(ends_with(IL{}, IL{}), "");
#endif

    return ::test_result();
}
