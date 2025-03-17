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
//

#include <range/v3/core.hpp>
#include <range/v3/utility/copy.hpp>
#include <range/v3/view/c_str.hpp>
#include <range/v3/view/indices.hpp>
#include <range/v3/view/indirect.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/drop_exactly.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

struct Int
{
    using difference_type = int;
    int i = 0;
    Int() = default;
    explicit Int(int j) : i(j) {}
    Int & operator++() {++i; CHECK(i <= 10); return *this;}
    Int operator++(int) {auto tmp = *this; ++*this; return tmp;}
    bool operator==(Int j) const { return i == j.i; }
    bool operator!=(Int j) const { return i != j.i; }
};

CPP_template(typename I)(
    requires ranges::integral<I>)
void test_iota_distance()
{
    using namespace ranges;
    using D = iter_difference_t<I>;
    I max = std::numeric_limits<I>::max();

    CHECK(detail::iota_distance_(I(0), I(0)) == D(0));
    CHECK(detail::iota_distance_(I(1), I(0)) == D(-1));
    CHECK(detail::iota_distance_(I(0), I(1)) ==  D(1));
    CHECK(detail::iota_distance_(I(1), I(1)) == D(0));

    CHECK(detail::iota_distance_(I(max - I(1)), I(max - I(1))) == D(0));
    CHECK(detail::iota_distance_(I(max), I(max - I(1))) == D(-1));
    CHECK(detail::iota_distance_(I(max - I(1)), I(max)) == D(1));
    CHECK(detail::iota_distance_(I(max), I(max)) == D(0));
}

int main()
{
    using namespace ranges;

    {
        CPP_assert(random_access_range<decltype(views::iota((unsigned short)0))>);
        CPP_assert(random_access_range<decltype(views::iota(0))>);
        static_assert(is_infinite<decltype(views::iota(0))>::value, "");
        static_assert(is_infinite<decltype(views::drop_exactly(views::iota(0),0))>::value, "");
        static_assert(!sized_range<decltype(views::iota(0))>, "");
    }

    {
        char const *sz = "hello world";
        ::check_equal(views::iota(ForwardIterator<char const*>(sz)) | views::take(10) | views::indirect,
            {'h','e','l','l','o',' ','w','o','r','l'});

        ::check_equal(views::ints | views::take(10), {0,1,2,3,4,5,6,7,8,9});
        ::check_equal(views::ints(0,unreachable) | views::take(10), {0,1,2,3,4,5,6,7,8,9});
        ::check_equal(views::ints(0,9), {0,1,2,3,4,5,6,7,8});
        ::check_equal(views::closed_indices(0,9), {0,1,2,3,4,5,6,7,8,9});
        ::check_equal(views::ints(1,10), {1,2,3,4,5,6,7,8,9});
        ::check_equal(views::closed_indices(1,10), {1,2,3,4,5,6,7,8,9,10});
    }

    {
        auto chars = views::ints(std::numeric_limits<signed char>::min(),
                                std::numeric_limits<signed char>::max());
        CPP_assert(random_access_range<decltype(chars)>);
        CPP_assert(same_as<int, range_difference_t<decltype(chars)>>);
        CPP_assert(view_<decltype(chars)>);
        CPP_assert(random_access_range<decltype(chars)>);
        CPP_assert(common_range<decltype(chars)>);
        CHECK(distance(chars.begin(), chars.end()) == (long) CHAR_MAX - (long) CHAR_MIN);
        CHECK(chars.size() == (unsigned)((long) CHAR_MAX - (long) CHAR_MIN));
    }

    {
        auto ushorts = views::ints(std::numeric_limits<unsigned short>::min(),
                                std::numeric_limits<unsigned short>::max());
        CPP_assert(view_<decltype(ushorts)>);
        CPP_assert(common_range<decltype(ushorts)>);
        CPP_assert(same_as<int, range_difference_t<decltype(ushorts)>>);
        CPP_assert(same_as<unsigned int, range_size_t<decltype(ushorts)>>);
        CHECK(distance(ushorts.begin(), ushorts.end()) == (int) USHRT_MAX);
        CHECK(ushorts.size() == (unsigned) USHRT_MAX);
    }

    {
        auto uints = views::closed_indices(
            std::numeric_limits<std::uint_least32_t>::min(),
            std::numeric_limits<std::uint_least32_t>::max() - 1);
        CPP_assert(view_<decltype(uints)>);
        CPP_assert(common_range<decltype(uints)>);
        CPP_assert(same_as<std::int_fast64_t, range_difference_t<decltype(uints)>>);
        CPP_assert(same_as<std::uint_fast64_t, range_size_t<decltype(uints)>>);
        CHECK(uints.size() == std::numeric_limits<std::uint32_t>::max());
    }

    {
        auto is = views::closed_indices(
            std::numeric_limits<std::int_least32_t>::min(),
            std::numeric_limits<std::int_least32_t>::max() - 1);
        CPP_assert(same_as<std::int_fast64_t, range_difference_t<decltype(is)>>);
        CPP_assert(same_as<std::uint_fast64_t, range_size_t<decltype(is)>>);
        CHECK(is.size() == std::numeric_limits<std::uint32_t>::max());
    }

    {
        auto sints = views::ints(std::numeric_limits<int>::min(),
                                std::numeric_limits<int>::max());
        CPP_assert(random_access_range<decltype(sints)>);
        CPP_assert(same_as<std::int_fast64_t, range_difference_t<decltype(sints)>>);
        CPP_assert(view_<decltype(sints)>);
        CPP_assert(random_access_range<decltype(sints)>);
        CPP_assert(common_range<decltype(sints)>);
        CHECK(distance(sints.begin(), sints.end()) == (std::int_fast64_t) INT_MAX - (std::int_fast64_t) INT_MIN);
        CHECK(sints.size() == (std::uint_fast64_t)((std::int_fast64_t) INT_MAX - (std::int_fast64_t) INT_MIN));
    }

    {
        auto is = views::closed_iota(Int{0}, Int{10});
        ::check_equal(is, {Int{0},Int{1},Int{2},Int{3},Int{4},Int{5},Int{6},Int{7},Int{8},Int{9},Int{10}});
        CPP_assert(view_<decltype(is)>);
        CPP_assert(common_range<decltype(is)>);
        CPP_assert(!sized_range<decltype(is)>);
        CPP_assert(forward_range<decltype(is)>);
        CPP_assert(!bidirectional_range<decltype(is)>);
    }

    {
        auto is = views::closed_iota(0, 10);
        ::check_equal(is, {0,1,2,3,4,5,6,7,8,9,10});
        CPP_assert(view_<decltype(is)>);
        CPP_assert(common_range<decltype(is)>);
        CPP_assert(sized_range<decltype(is)>);
        CPP_assert(random_access_range<decltype(is)>);
        CHECK(size(is) == 11u);
        auto it = is.begin(), e = is.end(), be = e;
        --be;
        using D = range_difference_t<decltype(is)>;
        // CHECK op++ and op-
        for(D i = 0; ; ++i)
        {
            CHECK((e - it) == (11 - i));
            CHECK((it - e) == -(11 - i));
            CHECK((be - it) == (10 - i));
            CHECK((it - be) == -(10 - i));
            if(i == 11) break;
            ++it;
        }
        // CHECK op-- and op-
        for(D i = 11; ; --i)
        {
            CHECK((e - it) == (11 - i));
            CHECK((it - e) == -(11 - i));
            CHECK((be - it) == (10 - i));
            CHECK((it - be) == -(10 - i));
            if(i == 0) break;
            --it;
        }
        // CHECK op+= and op-
        for(D i = 0; ; ++i)
        {
            it = next(is.begin(), i);
            CHECK((e - it) == (11 - i));
            CHECK((it - e) == -(11 - i));
            CHECK((be - it) == (10 - i));
            CHECK((it - be) == -(10 - i));
            if(i == 11) break;
        }
        // CHECK op-
        CHECK((e - 0) == e);
        CHECK((be - 0) == be);
        CHECK((e - 1) == be);
        CHECK((be - 1) == is.begin() + 9);
    }

    {  // iota distance tests
        test_iota_distance<int8_t>();
        test_iota_distance<int16_t>();
        test_iota_distance<int32_t>();
        test_iota_distance<int64_t>();

        test_iota_distance<uint8_t>();
        test_iota_distance<uint16_t>();
        test_iota_distance<uint32_t>();
        test_iota_distance<uint64_t>();
    }

    {
        // https://github.com/ericniebler/range-v3/issues/506
        auto cstr = views::c_str((const char*)"hello world");
        auto cstr2 = views::iota(cstr.begin(), cstr.end()) | views::indirect;
        ::check_equal(cstr2, std::string("hello world"));
        auto i = cstr2.begin();
        i += 4;
        CHECK(*i == 'o');
        CHECK((i - cstr2.begin()) == 4);
    }

    {  // test views::indices/closed_indices
        ::check_equal(views::indices | views::take(10), std::initializer_list<std::size_t>{0,1,2,3,4,5,6,7,8,9});

        ::check_equal(views::indices(0, 10), {0,1,2,3,4,5,6,7,8,9});
        ::check_equal(views::closed_indices(0, 10), {0,1,2,3,4,5,6,7,8,9,10});

        ::check_equal(views::indices(10), {0,1,2,3,4,5,6,7,8,9});
        ::check_equal(views::closed_indices(10), {0,1,2,3,4,5,6,7,8,9,10});
    }

    return ::test_result();
}
