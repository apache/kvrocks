// Range v3 library
//
//  Copyright Eric Niebler 2014, 2016
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <list>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>
#include <meta/meta.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/move_iterators.hpp>
#include <range/v3/iterator/insert_iterators.hpp>
#include <range/v3/iterator/stream_iterators.hpp>
#include <range/v3/algorithm/copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

using namespace ranges;

struct MoveOnlyReadable
{
    using value_type = std::unique_ptr<int>;
    value_type operator*() const;
};

CPP_assert(indirectly_readable<MoveOnlyReadable>);

void test_insert_iterator()
{
    CPP_assert(output_iterator<insert_iterator<std::vector<int>>, int&&>);
    CPP_assert(!equality_comparable<insert_iterator<std::vector<int>>>);
    std::vector<int> vi{5,6,7,8};
    copy(std::initializer_list<int>{1,2,3,4}, inserter(vi, vi.begin()+2));
    ::check_equal(vi, {5,6,1,2,3,4,7,8});
}

void test_ostream_joiner()
{
    std::ostringstream oss;
    std::vector<int> vi{};
    copy(vi, make_ostream_joiner(oss, ","));
    ::check_equal(oss.str(), std::string{""});
    vi = {1,2,3,4};
    copy(vi, make_ostream_joiner(oss, ","));
    ::check_equal(oss.str(), std::string{"1,2,3,4"});
}

void test_move_iterator()
{
    std::vector<MoveOnlyString> in;
    in.emplace_back("this");
    in.emplace_back("is");
    in.emplace_back("his");
    in.emplace_back("face");
    std::vector<MoveOnlyString> out;
    auto first = ranges::make_move_iterator(in.begin());
    using I = decltype(first);
    CPP_assert(input_iterator<I>);
    CPP_assert(!forward_iterator<I>);
    CPP_assert(same_as<I, ranges::move_iterator<std::vector<MoveOnlyString>::iterator>>);
    auto last = ranges::make_move_sentinel(in.end());
    using S = decltype(last);
    CPP_assert(sentinel_for<S, I>);
    CPP_assert(sized_sentinel_for<I, I>);
    CHECK((first - first) == 0);
    CPP_assert(sized_sentinel_for<S, I>);
    CHECK(static_cast<std::size_t>(last - first) == in.size());
    ranges::copy(first, last, ranges::back_inserter(out));
    ::check_equal(in, {"","","",""});
    ::check_equal(out, {"this","is","his","face"});
}

template<class I>
using RI = std::reverse_iterator<I>;

void issue_420_regression()
{
    // Verify that sized_sentinel_for<std::reverse_iterator<S>, std::reverse_iterator<I>>
    // properly requires sized_sentinel_for<I, S>
    CPP_assert(sized_sentinel_for<RI<int*>, RI<int*>>);
    CPP_assert(!sized_sentinel_for<RI<int*>, RI<float*>>);
    using BI = BidirectionalIterator<int*>;
    CPP_assert(!sized_sentinel_for<RI<BI>, RI<BI>>);
}

struct value_type_tester_thingy {};

namespace ranges
{
    template<>
    struct indirectly_readable_traits<::value_type_tester_thingy>
    {
        using value_type = int;
    };
}

template<typename T>
struct with_value_type { using value_type = T; };
template<typename T>
struct with_element_type { using element_type = T; };

// arrays of known bound
CPP_assert(same_as<int, ranges::indirectly_readable_traits<int[4]>::value_type>);
CPP_assert(same_as<int, ranges::indirectly_readable_traits<const int[4]>::value_type>);
CPP_assert(same_as<int*, ranges::indirectly_readable_traits<int*[4]>::value_type>);
CPP_assert(same_as<with_value_type<int>, ranges::indirectly_readable_traits<with_value_type<int>[4]>::value_type>);

#if !defined(__GNUC__) || defined(__clang__)
// arrays of unknown bound
CPP_assert(same_as<int, ranges::indirectly_readable_traits<int[]>::value_type>);
CPP_assert(same_as<int, ranges::indirectly_readable_traits<const int[]>::value_type>);
#endif

template<typename T>
using readable_traits_value_type_t = typename ranges::indirectly_readable_traits<T>::value_type;
template<typename T>
using readable_traits_value_type = meta::defer<readable_traits_value_type_t, T>;

// object pointer types
CPP_assert(same_as<int, ranges::indirectly_readable_traits<int*>::value_type>);
CPP_assert(same_as<int, ranges::indirectly_readable_traits<int*const>::value_type>);
CPP_assert(same_as<int, ranges::indirectly_readable_traits<int const*>::value_type>);
CPP_assert(same_as<int, ranges::indirectly_readable_traits<int const*const>::value_type>);
CPP_assert(same_as<int[4], ranges::indirectly_readable_traits<int(*)[4]>::value_type>);
CPP_assert(same_as<int[4], ranges::indirectly_readable_traits<const int(*)[4]>::value_type>);
struct incomplete;
CPP_assert(same_as<incomplete, ranges::indirectly_readable_traits<incomplete*>::value_type>);
static_assert(!meta::is_trait<readable_traits_value_type<void*>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<void const*>>::value, "");

// class types with member value_type
CPP_assert(same_as<int, ranges::indirectly_readable_traits<with_value_type<int>>::value_type>);
CPP_assert(same_as<int, ranges::indirectly_readable_traits<with_value_type<int> const>::value_type>);
CPP_assert(same_as<int, ranges::indirectly_readable_traits<value_type_tester_thingy>::value_type>);
CPP_assert(same_as<int, ranges::indirectly_readable_traits<value_type_tester_thingy const>::value_type>);
CPP_assert(same_as<int[4], ranges::indirectly_readable_traits<with_value_type<int[4]>>::value_type>);
CPP_assert(same_as<int[4], ranges::indirectly_readable_traits<with_value_type<int[4]> const>::value_type>);
static_assert(!meta::is_trait<readable_traits_value_type<with_value_type<void>>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<with_value_type<int(int)>>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<with_value_type<int&>>>::value, "");

// class types with member element_type
CPP_assert(same_as<int, ranges::indirectly_readable_traits<with_element_type<int>>::value_type>);
CPP_assert(same_as<int, ranges::indirectly_readable_traits<with_element_type<int> const>::value_type>);
CPP_assert(same_as<int, ranges::indirectly_readable_traits<with_element_type<int const>>::value_type>);
CPP_assert(same_as<int[4], ranges::indirectly_readable_traits<with_element_type<int[4]>>::value_type>);
CPP_assert(same_as<int[4], ranges::indirectly_readable_traits<with_element_type<int[4]> const>::value_type>);
CPP_assert(same_as<int[4], ranges::indirectly_readable_traits<with_element_type<int const[4]>>::value_type>);
static_assert(!meta::is_trait<readable_traits_value_type<with_element_type<void>>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<with_element_type<void const>>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<with_element_type<void> const>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<with_element_type<int(int)>>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<with_element_type<int&>>>::value, "");

// cv-void
static_assert(!meta::is_trait<readable_traits_value_type<void>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<void const>>::value, "");
// reference types
static_assert(!meta::is_trait<readable_traits_value_type<int&>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<int&&>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<int*&>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<int*&&>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<int(&)(int)>>::value, "");
static_assert(!meta::is_trait<readable_traits_value_type<std::ostream&>>::value, "");

CPP_assert(indirectly_swappable<int *, int *>);
CPP_assert(indirectly_movable<int const *, int *>);
CPP_assert(!indirectly_swappable<int const *, int const *>);
CPP_assert(!indirectly_movable<int const *, int const *>);

namespace Boost
{
    struct S {}; // just to have a type from Boost namespace
    template<typename I, typename D>
    void advance(I&, D)
    {}
}

// Regression test for https://github.com/ericniebler/range-v3/issues/845
void test_845()
{
    std::list<std::pair<Boost::S, int>> v = { {Boost::S{}, 0} };
    auto itr = v.begin();
    ranges::advance(itr, 1); // Should not create ambiguity
}

// Test for https://github.com/ericniebler/range-v3/issues/1110
void test_1110()
{
    // this should not trigger assertation error
    std::vector<int> v = {1,2,3};
    auto e = ranges::end(v);
    ranges::advance(e, 0, ranges::begin(v));
}

// Test the deep integration with the STL
#if defined(RANGES_DEEP_STL_INTEGRATION) && RANGES_DEEP_STL_INTEGRATION

struct X
{
    int& operator*() const;
    X & operator++();
    struct proxy { operator int() const; };
    proxy operator++(int);
};

namespace std
{
    template <>
    struct iterator_traits<::X>
    {
        using value_type = int;
        using reference = int&;
        using pointer = int*;
        using difference_type = ptrdiff_t;
        using iterator_category = std::input_iterator_tag;
    };
}

static_assert(ranges::input_iterator<X>, "");

struct Y
{
    using value_type = int;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::bidirectional_iterator_tag;
    using reference = int&;
    using pointer = int*;
    int& operator*() const noexcept;
};

static_assert(std::is_same<std::add_pointer_t<int&>, int*>::value, "");

struct Z
{
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::bidirectional_iterator_tag;
    int& operator*() const noexcept;
    Z& operator++();
    Z operator++(int);
    bool operator==(Z) const;
    bool operator!=(Z) const;
};

namespace ranges
{
    template <>
    struct indirectly_readable_traits<::Z>
    {
        using value_type = int;
    };
}

// Looks like an STL2 forward iterator, but the conformance beyond
// input is "accidental".
struct WouldBeFwd
{
    using value_type = struct S{ };
    using difference_type = std::ptrdiff_t;
    S & operator*() const;
    WouldBeFwd& operator++();
    WouldBeFwd operator++(int);
    //S* operator->() const;
    bool operator==(WouldBeFwd) const;
    bool operator!=(WouldBeFwd) const;
};

namespace std
{
    template <>
    struct iterator_traits<::WouldBeFwd>
    {
        using value_type = ::WouldBeFwd::value_type;
        using difference_type = ::WouldBeFwd::difference_type;
        using reference = iter_reference_t<::WouldBeFwd>;
        using pointer = add_pointer_t<reference>;
        // Explicit opt-out of stl2's forward_iterator concept:
        using iterator_category = std::input_iterator_tag; // STL1-style iterator category
    };
}

// Looks like an STL2 bidirectional iterator, but the conformance beyond
// forward is "accidental".
struct WouldBeBidi
{
    using value_type = struct S{ };
    using difference_type = std::ptrdiff_t;
    // using iterator_category = std::input_iterator_tag;
    // using iterator_concept = std::forward_iterator_tag;
    S operator*() const; // by value!
    WouldBeBidi& operator++();
    WouldBeBidi operator++(int);
    WouldBeBidi& operator--();
    WouldBeBidi operator--(int);
    //S* operator->() const;
    bool operator==(WouldBeBidi) const;
    bool operator!=(WouldBeBidi) const;
};

namespace std
{
    template <>
    struct iterator_traits<::WouldBeBidi>
    {
        using value_type = ::WouldBeBidi::value_type;
        using difference_type = ::WouldBeBidi::difference_type;
        using reference = value_type;
        using pointer = value_type*;
        using iterator_category = std::input_iterator_tag; // STL1-style iterator category
        // Explicit opt-out of stl2's bidirectional_iterator concept:
        using iterator_concept = std::forward_iterator_tag; // STL2-style iterator category
    };
}

struct OutIter
{
    using difference_type = std::ptrdiff_t;
    OutIter& operator=(int);
    OutIter& operator*();
    OutIter& operator++();
    OutIter& operator++(int);
};

// proxy iterator
struct bool_iterator
{
    using value_type = bool;
    struct reference
    {
        operator bool() const { return true; }
        reference();
        reference(reference const &);
        reference& operator=(reference);
        reference& operator=(bool);
    };
    using difference_type = std::ptrdiff_t;
    reference operator*() const;
    bool_iterator& operator++();
    bool_iterator operator++(int);
    bool operator==(bool_iterator) const;
    bool operator!=(bool_iterator) const;
    friend reference iter_move(bool_iterator i) { return *i; }
    friend void iter_swap(bool_iterator, bool_iterator) { }
};

void deep_integration_test()
{
    using std::is_same;
    using std::iterator_traits;
    using ranges::iter_value_t;
    using ranges::iter_difference_t;
    static_assert(is_same<iter_difference_t<std::int_least16_t>, int>::value, "");
    static_assert(is_same<iter_difference_t<std::uint_least16_t>, int>::value, "");
    static_assert(is_same<iter_difference_t<std::int_least32_t>, std::int_least32_t>::value, "");
    static_assert(is_same<iter_difference_t<std::uint_least32_t>, meta::_t<std::make_signed<std::uint_least32_t>>>::value, "");
    static_assert(is_same<iter_difference_t<std::uint_least64_t>, meta::_t<std::make_signed<std::uint_least64_t>>>::value, "");

    static_assert(is_same<iter_value_t<const int*>, int>::value, "");
    static_assert(is_same<iter_difference_t<const int*>, ptrdiff_t>::value, "");
    static_assert(is_same<iter_difference_t<int* const>, ptrdiff_t>::value, "");

    static_assert(detail::is_std_iterator_traits_specialized_v<X>, "");
    static_assert(is_same<iterator_traits<X>::value_type, int>::value, "");
    static_assert(is_same<iter_value_t<X>, int>::value, "");

    static_assert(!detail::is_std_iterator_traits_specialized_v<Y>, "");
    static_assert(is_same<iterator_traits<Y>::value_type, int>::value, "");
    static_assert(is_same<iter_value_t<Y>, int>::value, "");

    // libc++ has a broken std::iterator_traits primary template
    // https://bugs.llvm.org/show_bug.cgi?id=39619
#ifndef _LIBCPP_VERSION
    // iterator_traits uses specializations of ranges::indirectly_readable_traits:
    static_assert(!detail::is_std_iterator_traits_specialized_v<Z>, "");
    static_assert(is_same<iterator_traits<Z>::value_type, int>::value, "");
    static_assert(is_same<iter_value_t<Z>, int>::value, "");
    static_assert(is_same<iterator_traits<Z>::iterator_category,
                          std::bidirectional_iterator_tag>::value, "");
#endif

    static_assert(ranges::input_iterator<WouldBeFwd>, "");
    static_assert(!ranges::forward_iterator<WouldBeFwd>, "");
    static_assert(is_same<iterator_traits<WouldBeFwd>::iterator_category,
                           std::input_iterator_tag>::value, "");

    static_assert(ranges::forward_iterator<WouldBeBidi>, "");
    static_assert(!ranges::bidirectional_iterator<WouldBeBidi>, "");
    static_assert(is_same<iterator_traits<WouldBeBidi>::iterator_category,
                          std::input_iterator_tag>::value, "");

    static_assert(ranges::input_or_output_iterator<OutIter>, "");
    static_assert(!ranges::input_iterator<OutIter>, "");
    static_assert(is_same<iterator_traits<OutIter>::difference_type,
                          std::ptrdiff_t>::value, "");
    static_assert(is_same<iterator_traits<OutIter>::iterator_category,
                          std::output_iterator_tag>::value, "");

    static_assert(ranges::contiguous_iterator<int volatile *>, "");

    static_assert(ranges::forward_iterator<bool_iterator>, "");
    static_assert(is_same<iterator_traits<bool_iterator>::iterator_category,
                          std::input_iterator_tag>::value, "");
    // static_assert(_Cpp98InputIterator<int volatile*>);
    // static_assert(_Cpp98InputIterator<bool_iterator>);

    // // Test subsumption:
    // test(WouldBeFwd{});
    // test(WouldBeBidi{});
    // test(meta::detail::nullptr_v<int>);

    // // Test subsumption:
    // test2(OutIter{});
    // test2(meta::detail::nullptr_v<int>);

    // // Test subsumption:
    // test3(WouldBeFwd{}, WouldBeFwd{});
    // test3(meta::detail::nullptr_v<int>, meta::detail::nullptr_v<int>);
}

#endif

int main()
{
    test_insert_iterator();
    test_move_iterator();
    test_ostream_joiner();
    issue_420_regression();
    test_1110();

    {
        struct S { using value_type = int; };
        CPP_assert(same_as<int, ranges::indirectly_readable_traits<S const>::value_type>);
    }

    return ::test_result();
}
