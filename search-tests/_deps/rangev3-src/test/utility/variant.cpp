// Range v3 library
//
//  Copyright Eric Niebler 2015-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <vector>
#include <sstream>
#include <iostream>
#include <range/v3/functional/overload.hpp>
#include <range/v3/numeric/accumulate.hpp>
#include <range/v3/utility/variant.hpp>
#include <range/v3/view/concat.hpp>
#include <range/v3/view/partial_sum.hpp>
#include <range/v3/view/transform.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

void bug_1217()
{
    std::vector<int> vec;

    if(auto tx = vec | ranges::views::transform( [](int){ return 0; } ))
    {
        auto positions_visited = ranges::views::concat( tx, tx ) | ranges::views::partial_sum;
        ranges::accumulate( positions_visited, 0 );
    }
}
 
int main()
{
    using namespace ranges;

    // Simple variant and access.
    {
        variant<int, short> v;
        CHECK(v.index() == 0u);
        auto v2 = v;
        CHECK(v2.index() == 0u);
        v.emplace<1>((short)2);
        CHECK(v.index() == 1u);
        CHECK(get<1>(v) == (short)2);
        try
        {
            get<0>(v);
            CHECK(false);
        }
        catch(const bad_variant_access&)
        {}
        catch(...)
        {
            CHECK(!(bool)"unknown exception");
        }
        v = v2;
        CHECK(v.index() == 0u);
    }

    // variant of void
    {
        variant<void, void> v;
        CHECK(v.index() == 0u);
        v.emplace<0>();
        CHECK(v.index() == 0u);
        try
        {
            // Will only compile if get returns void
            v.index() == 0 ? void() : get<0>(v);
        }
        catch(...)
        {
            CHECK(false);
        }
        v.emplace<1>();
        CHECK(v.index() == 1u);
        try
        {
            get<0>(v);
            CHECK(false);
        }
        catch(const bad_variant_access&)
        {}
        catch(...)
        {
            CHECK(!(bool)"unknown exception");
        }
    }

    // variant of references
    {
        int i = 42;
        std::string s = "hello world";
        variant<int&, std::string&> v{emplaced_index<0>, i};
        CPP_assert(!default_constructible<variant<int&, std::string&>>);
        CHECK(v.index() == 0u);
        CHECK(get<0>(v) == 42);
        CHECK(&get<0>(v) == &i);
        auto const & cv = v;
        get<0>(cv) = 24;
        CHECK(i == 24);
        v.emplace<1>(s);
        CHECK(v.index() == 1u);
        CHECK(get<1>(v) == "hello world");
        CHECK(&get<1>(v) == &s);
        get<1>(cv) = "goodbye";
        CHECK(s == "goodbye");
    }

    // Move test 1
    {
        variant<int, MoveOnlyString> v{emplaced_index<1>, "hello world"};
        CHECK(get<1>(v) == "hello world");
        MoveOnlyString s = get<1>(std::move(v));
        CHECK(s == "hello world");
        CHECK(get<1>(v) == "");
        v.emplace<1>("goodbye");
        CHECK(get<1>(v) == "goodbye");
        auto v2 = std::move(v);
        CHECK(get<1>(v2) == "goodbye");
        CHECK(get<1>(v) == "");
        v = std::move(v2);
        CHECK(get<1>(v) == "goodbye");
        CHECK(get<1>(v2) == "");
    }

    // Move test 2
    {
        MoveOnlyString s = "hello world";
        variant<MoveOnlyString&> v{emplaced_index<0>, s};
        CHECK(get<0>(v) == "hello world");
        MoveOnlyString &s2 = get<0>(std::move(v));
        CHECK(&s2 == &s);
    }

    // Apply test 1
    {
        std::stringstream sout;
        variant<int, std::string> v{emplaced_index<1>, "hello"};
        auto fun = overload(
            [&sout](int&) {sout << "int";},
            [&sout](std::string&)->int {sout << "string"; return 42;});
        variant<void, int> x = v.visit(fun);
        CHECK(sout.str() == "string");
        CHECK(x.index() == 1u);
        CHECK(get<1>(x) == 42);
    }

    // Apply test 2
    {
        std::stringstream sout;
        std::string s = "hello";
        variant<int, std::string&> const v{emplaced_index<1>, s};
        auto fun = overload(
            [&sout](int const&) {sout << "int";},
            [&sout](std::string&)->int {sout << "string"; return 42;});
        variant<void, int> x = v.visit(fun);
        CHECK(sout.str() == "string");
        CHECK(x.index() == 1u);
        CHECK(get<1>(x) == 42);
    }

    // constexpr variant
    {
        constexpr variant<int, short> v{emplaced_index<1>, (short)2};
        static_assert(v.index() == 1,"");
        static_assert(v.valid(),"");
    }

    // Variant and arrays
    {
        variant<int[5], std::vector<int>> v{emplaced_index<0>, {1,2,3,4,5}};
        int (&rgi)[5] = get<0>(v);
        check_equal(rgi, {1,2,3,4,5});

        variant<int[5], std::vector<int>> v2{emplaced_index<0>, {}};
        int (&rgi2)[5] = get<0>(v2);
        check_equal(rgi2, {0,0,0,0,0});

        v2 = v;
        check_equal(rgi2, {1,2,3,4,5});

        struct T
        {
            T() = delete;
            T(int) {}
            T(T const &) = default;
            T &operator=(T const &) = default;
        };

        // Should compile and not assert at runtime.
        variant<T[5]> vrgt{emplaced_index<0>, {T{42},T{42},T{42},T{42},T{42}}};
        (void) vrgt;
    }

    return ::test_result();
}
