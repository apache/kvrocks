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

#include <list>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/ref.hpp>
#include <range/v3/view/subrange.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

CPP_template(class Rng)(
    requires ranges::range<Rng>)
ranges::borrowed_subrange_t<Rng> algorithm(Rng &&rng);

struct Base {};
struct Derived : Base {};

int main()
{
    using namespace ranges;

    std::vector<int> vi{1,2,3,4};

    ////////////////////////////////////////////////////////////////////////////
    // borrowed_subrange_t tests:

    // lvalues are ReferenceableRanges and do not dangle:
    CPP_assert(same_as<subrange<int*>,
        decltype(::algorithm(std::declval<int(&)[42]>()))>);
    CPP_assert(same_as<subrange<std::vector<int>::iterator>,
        decltype(::algorithm(vi))>);

    // subrange and ref_view are ReferenceableRanges and do not dangle:
    CPP_assert(same_as<subrange<int*>,
        decltype(::algorithm(std::declval<subrange<int*>>()))>);
    CPP_assert(same_as<subrange<int*>,
        decltype(::algorithm(std::declval<ref_view<int[42]>>()))>);

    // non-ReferenceableRange rvalue ranges dangle:
    CPP_assert(same_as<dangling,
        decltype(::algorithm(std::declval<std::vector<int>>()))>);
    CPP_assert(same_as<dangling,
        decltype(::algorithm(std::move(vi)))>);

    // Test that slicing conversions are not allowed.
    CPP_assert(constructible_from<subrange<Base*, Base*>, Base*, Base*>);
    CPP_assert(!constructible_from<subrange<Base*, Base*>, Derived*, Derived*>);
    CPP_assert(constructible_from<subrange<const Base*, const Base*>, Base*, Base*>);
    CPP_assert(!constructible_from<subrange<const Base*, const Base*>, Derived*, Derived*>);
    CPP_assert(!constructible_from<subrange<Base*, Base*>, subrange<Derived*, Derived*>>);

    CPP_assert(constructible_from<subrange<Base*, unreachable_sentinel_t>, Base*, unreachable_sentinel_t>);
    CPP_assert(!constructible_from<subrange<Base*, unreachable_sentinel_t>, Derived*, unreachable_sentinel_t>);
    CPP_assert(constructible_from<subrange<const Base*, unreachable_sentinel_t>, Base*, unreachable_sentinel_t>);
    CPP_assert(!constructible_from<subrange<const Base*, unreachable_sentinel_t>, Derived*, unreachable_sentinel_t>);
    CPP_assert(!constructible_from<subrange<Base*, unreachable_sentinel_t>, subrange<Derived*, unreachable_sentinel_t>>);

    CPP_assert(constructible_from<subrange<Base*, Base*, subrange_kind::sized>, Base*, Base*, std::size_t>);
    CPP_assert(!constructible_from<subrange<Base*, Base*, subrange_kind::sized>, Derived*, Base*, std::size_t>);
    CPP_assert(constructible_from<subrange<const Base*, const Base*, subrange_kind::sized>, Base*, const Base*, std::size_t>);
    CPP_assert(!constructible_from<subrange<const Base*, const Base*, subrange_kind::sized>, Derived*, const Base*, std::size_t>);
    CPP_assert(!constructible_from<subrange<Base*, Base*, subrange_kind::sized>, subrange<Derived*, Base*>, std::size_t>);

    CPP_assert(convertible_to<subrange<Base*, Base*>, std::pair<const Base*, const Base*>>);
    CPP_assert(!convertible_to<subrange<Derived*, Derived*>, std::pair<Base*, Base*>>);

    subrange<std::vector<int>::iterator> r0 {vi.begin(), vi.end()};
    static_assert(std::tuple_size<decltype(r0)>::value == 2, "");
    CPP_assert(same_as<std::vector<int>::iterator,
        std::tuple_element<0, decltype(r0)>::type>);
    CPP_assert(same_as<std::vector<int>::iterator,
        std::tuple_element<1, decltype(r0)>::type>);
    CPP_assert(sized_range<decltype(r0)>);
    CHECK(r0.size() == 4u);
    CHECK(r0.begin() == vi.begin());
    CHECK(get<0>(r0) == vi.begin());
    CHECK(r0.end() == vi.end());
    CHECK(get<1>(r0) == vi.end());
    r0 = r0.next();
    CHECK(r0.size() == 3u);

    {
        subrange<std::vector<int>::iterator> rng {vi.begin(), vi.end(), ranges::size(vi)};
        CHECK(rng.size() == 4u);
        CHECK(rng.begin() == vi.begin());
        CHECK(rng.end() == vi.end());
    }

    std::pair<std::vector<int>::iterator, std::vector<int>::iterator> p0 = r0;
    CHECK(p0.first == vi.begin()+1);
    CHECK(p0.second == vi.end());

    subrange<std::vector<int>::iterator, unreachable_sentinel_t> r1 { r0.begin(), {} };
    static_assert(std::tuple_size<decltype(r1)>::value == 2, "");
    CPP_assert(same_as<std::vector<int>::iterator,
        std::tuple_element<0, decltype(r1)>::type>);
    CPP_assert(same_as<unreachable_sentinel_t,
        std::tuple_element<1, decltype(r1)>::type>);
    CPP_assert(view_<decltype(r1)>);
    CPP_assert(!sized_range<decltype(r1)>);
    CHECK(r1.begin() == vi.begin()+1);
    r1.end() = unreachable;

    r0 = r0.next();
    ++r0.begin();
    CHECK(r0.begin() == vi.begin()+2);
    CHECK(r0.size() == 2u);
    r0 = {r0.begin(), --r0.end()}; // --r0.end();
    CHECK(r0.end() == vi.end()-1);
    CHECK(r0.size() == 1u);
    CHECK(r0.front() == 3);
    CHECK(r0.back() == 3);

    std::pair<std::vector<int>::iterator, unreachable_sentinel_t> p1 = r1;
    CHECK(p1.first == vi.begin()+1);

    std::list<int> li{1,2,3,4};
    using LI = std::list<int>::iterator;
    subrange<LI, LI, subrange_kind::sized> l0 {li.begin(), li.end(), li.size()};
    CPP_assert(view_<decltype(l0)> && sized_range<decltype(l0)>);
    CHECK(l0.begin() == li.begin());
    CHECK(l0.end() == li.end());
    CHECK(l0.size() == li.size());
    l0 = l0.next();
    CHECK(l0.begin() == next(li.begin()));
    CHECK(l0.end() == li.end());
    CHECK(l0.size() == li.size() - 1u);

    l0 = views::all(li);

    subrange<std::list<int>::iterator> l1 = l0;
    CPP_assert(!sized_range<decltype(l1)>);
    CHECK(l1.begin() == li.begin());
    CHECK(l1.end() == li.end());

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
#if defined(__clang__) && __clang_major__ < 6
// Workaround https://bugs.llvm.org/show_bug.cgi?id=33314
RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_UNDEFINED_FUNC_TEMPLATE
#endif
    {
        subrange s0{vi.begin(), vi.end()};
        subrange s1{li.begin(), li.end()};
        CPP_assert(same_as<decltype(r0), decltype(s0)>);
        CPP_assert(same_as<decltype(l1), decltype(s1)>);
    }
    {
        subrange s0{vi.begin(), vi.end(), ranges::size(vi)};
        subrange s1{li.begin(), li.end(), ranges::size(li)};
        CPP_assert(same_as<decltype(r0), decltype(s0)>);
        CPP_assert(same_as<decltype(l0), decltype(s1)>);
    }
    {
        subrange s0{vi};
        subrange s1{li};
        subrange s2{views::all(vi)};
        subrange s3{views::all(li)};
        CPP_assert(same_as<decltype(r0), decltype(s0)>);
        CPP_assert(same_as<decltype(l0), decltype(s1)>);
        CPP_assert(same_as<decltype(r0), decltype(s2)>);
        CPP_assert(same_as<decltype(l0), decltype(s3)>);
    }
    {
        subrange s0{r0};
        subrange s1{l0};
        subrange s2{l1};
        CPP_assert(same_as<decltype(r0), decltype(s0)>);
        CPP_assert(same_as<decltype(l0), decltype(s1)>);
        CPP_assert(same_as<decltype(l1), decltype(s2)>);
    }
    {
        subrange s0{vi, ranges::size(vi)};
        subrange s1{li, ranges::size(li)};
        subrange s2{views::all(vi), ranges::size(vi)};
        subrange s3{views::all(li), ranges::size(li)};
        CPP_assert(same_as<decltype(r0), decltype(s0)>);
        CPP_assert(same_as<decltype(l0), decltype(s1)>);
        CPP_assert(same_as<decltype(r0), decltype(s2)>);
        CPP_assert(same_as<decltype(l0), decltype(s3)>);
    }
    {
        subrange s0{r0, size(r0)};
        subrange s1{l0, size(l0)};
        subrange s2{l1, size(l0)};
        CPP_assert(same_as<decltype(r0), decltype(s0)>);
        CPP_assert(same_as<decltype(l0), decltype(s1)>);
        CPP_assert(same_as<decltype(l0), decltype(s2)>);
    }
#if defined(__clang__) && __clang_major__ < 6
RANGES_DIAGNOSTIC_POP
#endif // clang bug workaround
#endif // use deduction guides

    return ::test_result();
}
