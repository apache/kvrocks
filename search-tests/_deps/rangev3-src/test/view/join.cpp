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

#include <iterator>
#include <forward_list>
#include <functional>
#include <vector>

#include <range/v3/core.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/split.hpp>
#include <range/v3/view/generate_n.hpp>
#include <range/v3/view/repeat_n.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/concat.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/single.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/filter.hpp>

#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_GLOBAL_CONSTRUCTORS

namespace
{
    template<typename T, std::size_t N>
    struct input_array
    {
        T elements_[N];

        InputIterator<T*> begin() { return InputIterator<T*>{elements_ + 0}; }
        InputIterator<T*> end() { return InputIterator<T*>{elements_ + N}; }
        constexpr std::size_t size() const { return N; }
    };

    static int N = 0;
    auto make_input_rng()
    {
        using ranges::views::generate_n;
        return generate_n([](){
            return generate_n([](){
                return N++;
            },3);
        },3);
    }

    template<typename T>
    constexpr auto twice(T t)
    {
        return ranges::views::concat(
            ranges::views::single(t),
            ranges::views::single(t));
    }

#ifdef __clang__
RANGES_DIAGNOSTIC_IGNORE_PRAGMAS
RANGES_DIAGNOSTIC_IGNORE("-Wunneeded-member-function")
RANGES_DIAGNOSTIC_IGNORE("-Wunused-member-function")
#endif

    // https://github.com/ericniebler/range-v3/issues/283
    void test_issue_283()
    {
        const std::vector<std::vector<int>> nums =
        {
            { 1, 2, 3 },
            { 4, 5, 6 }
        };
        const auto flat_nums = ranges::views::join( nums ) | ranges::to<std::vector>();
        ::check_equal(flat_nums, {1,2,3,4,5,6});
    }

    // https://github.com/ericniebler/range-v3/issues/1414
    void test_issue_1414()
    {
        std::forward_list<char> u2; // this can also be a vector
        std::vector<char> i2;
        auto v2 = u2 | ranges::views::chunk(3) | ranges::views::join(i2);
        CPP_assert(ranges::input_range<decltype(v2)>);
    }
}

int main()
{
    using namespace ranges;

    // Test that we can join an input range of input ranges:
    {
        auto rng0 = make_input_rng() | views::join;
        static_assert(range_cardinality<decltype(rng0)>::value == ranges::finite, "");
        CPP_assert(input_range<decltype(rng0)>);
        CPP_assert(!forward_range<decltype(rng0)>);
        CPP_assert(!common_range<decltype(rng0)>);
        CPP_assert(!sized_range<decltype(rng0)>);
        check_equal(rng0, {0,1,2,3,4,5,6,7,8});
    }

    // Joining with a value
    {
        N = 0;
        auto rng1 = make_input_rng() | views::join(42);
        static_assert(range_cardinality<decltype(rng1)>::value == ranges::finite, "");
        CPP_assert(input_range<decltype(rng1)>);
        CPP_assert(!forward_range<decltype(rng1)>);
        CPP_assert(!common_range<decltype(rng1)>);
        CPP_assert(!sized_range<decltype(rng1)>);
        check_equal(rng1, {0,1,2,42,3,4,5,42,6,7,8});
    }

    // Joining with a range
    {
        N = 0;
        int rgi[] = {42,43};
        auto rng2 = make_input_rng() | views::join(rgi);
        static_assert(range_cardinality<decltype(rng2)>::value == ranges::finite, "");
        CPP_assert(input_range<decltype(rng2)>);
        CPP_assert(!forward_range<decltype(rng2)>);
        CPP_assert(!common_range<decltype(rng2)>);
        CPP_assert(!sized_range<decltype(rng2)>);
        check_equal(rng2, {0,1,2,42,43,3,4,5,42,43,6,7,8});
    }

    // Just for fun:
    {
        std::string str = "Now,is,the,time,for,all,good,men,to,come,to,the,aid,of,their,country";
        auto res = str | views::split(',') | views::join(' ') | to<std::string>();
        CHECK(res == "Now is the time for all good men to come to the aid of their country");
        static_assert(range_cardinality<decltype(res)>::value == ranges::finite, "");
    }

    {
        std::vector<std::string> vs{"This","is","his","face"};
        auto rng3 = views::join(vs);
        static_assert(range_cardinality<decltype(rng3)>::value == ranges::finite, "");
        CPP_assert(!sized_range<decltype(rng3)>);
        CPP_assert(!sized_sentinel_for<decltype(end(rng3)), decltype(begin(rng3))>);
        CHECK(to<std::string>(rng3) == "Thisishisface");

        auto rng4 = views::join(vs, ' ');
        static_assert(range_cardinality<decltype(rng3)>::value == ranges::finite, "");
        CPP_assert(!sized_range<decltype(rng4)>);
        CPP_assert(!sized_sentinel_for<decltype(end(rng4)), decltype(begin(rng4))>);
        CHECK(to<std::string>(rng4) == "This is his face");
    }

    {
        auto rng5 = views::join(twice(twice(42)));
        static_assert(range_cardinality<decltype(rng5)>::value == 4, "");
        CPP_assert(sized_range<decltype(rng5)>);
        CHECK(rng5.size() == 4u);
        check_equal(rng5, {42,42,42,42});
    }

    {
        auto rng6 = views::join(twice(views::repeat_n(42, 2)));
        static_assert(range_cardinality<decltype(rng6)>::value == ranges::finite, "");
        CPP_assert(sized_range<decltype(rng6)>);
        CHECK(rng6.size() == 4u);
        check_equal(rng6, {42,42,42,42});
    }

    {
        input_array<std::string, 4> some_strings = {{"This","is","his","face"}};
        CPP_assert(input_range<decltype(some_strings)>);
        CPP_assert(sized_range<decltype(some_strings)>);
        CPP_assert(!sized_range<decltype(some_strings | views::join)>);
    }

    {
        int const some_int_pairs[3][2] = {{0,1},{2,3},{4,5}};
        auto rng = debug_input_view<int const[2]>{some_int_pairs} | views::join;
        check_equal(rng, {0,1,2,3,4,5});
    }

    {
        std::vector<std::string> vs{"this","is","his","face"};
        join_view<ref_view<std::vector<std::string>>> jv{vs};
        check_equal(jv, {'t','h','i','s','i','s','h','i','s','f','a','c','e'});
        CPP_assert(bidirectional_range<decltype(jv)>);
        CPP_assert(bidirectional_range<const decltype(jv)>);
        CPP_assert(common_range<decltype(jv)>);
        CPP_assert(common_range<const decltype(jv)>);
    }

    {
        auto rng = views::iota(0,4)
            | views::transform([](int i) {return views::iota(0,i);})
            | views::join;
        check_equal(rng, {0,0,1,0,1,2});
        CPP_assert(input_range<decltype(rng)>);
        CPP_assert(!range<const decltype(rng)>);
        CPP_assert(!forward_range<decltype(rng)>);
        CPP_assert(!common_range<decltype(rng)>);
    }

    {
        auto rng = views::iota(0,4)
            | views::transform([](int i) {return views::iota(0,i);})
            | views::filter([](auto){ return true; })
            | views::join;
        check_equal(rng, {0,0,1,0,1,2});
        CPP_assert(input_range<decltype(rng)>);
        CPP_assert(!range<const decltype(rng)>);
        CPP_assert(!forward_range<decltype(rng)>);
        CPP_assert(!common_range<decltype(rng)>);
    }

    {
        auto rng = views::iota(0,4)
            | views::transform([](int i) {return std::string((std::size_t) i, char('a'+i));})
            | views::join;
        check_equal(rng, {'b','c','c','d','d','d'});
        CPP_assert(input_range<decltype(rng)>);
        CPP_assert(!range<const decltype(rng)>);
        CPP_assert(!forward_range<decltype(rng)>);
        CPP_assert(!common_range<decltype(rng)>);
    }

    {
        auto rng = views::iota(0,4)
            | views::transform([](int i) {return std::string((std::size_t) i, char('a'+i));})
            | views::join('-');
        check_equal(rng, {'-','b','-','c','c','-','d','d','d'});
        CPP_assert(input_range<decltype(rng)>);
        CPP_assert(!range<const decltype(rng)>);
        CPP_assert(!forward_range<decltype(rng)>);
        CPP_assert(!common_range<decltype(rng)>);
    }

    // https://github.com/ericniebler/range-v3/issues/1320
    {
        auto op = [](auto & input, int i, auto & ins)
        {
            return input | ranges::views::chunk(i)
                         | ranges::views::join(ins);
        };
        std::string input{"foobarbaxbat"};
        std::string insert{"X"};
        auto rng = op(input, 2, insert);
        std::cout << rng << '\n';
        ::check_equal(rng, {'f','o','X','o','b','X','a','r','X','b','a','X','x','b','X',
            'a','t'});
    }

    {
        auto op = [](auto & input, int i, auto & ins)
        {
            return input | ranges::views::chunk(i)
                         | ranges::views::join(ins);
        };
        std::vector<std::string> input{"foo","bar","bax","bat"};
        std::string insert{"XX"};
        auto rng = op(input, 2, insert);
        std::cout << rng << '\n';
        ::check_equal(rng, {"foo","bar","XX","bax","bat"});
    }

    test_issue_283();
    test_issue_1414();

    return ::test_result();
}
