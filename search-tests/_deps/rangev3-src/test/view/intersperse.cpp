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

#include <sstream>
#include <range/v3/core.hpp>
#include <range/v3/view/intersperse.hpp>
#include <range/v3/view/delimit.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/range/conversion.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

#ifdef RANGES_WORKAROUND_MSVC_790554
template<std::size_t N>
auto c_str(char const (&sz)[N])
{
    return ranges::subrange<char const*>{&sz[0], &sz[N-1]};
}
#else // ^^^ workaround / no workaround vvv
template<std::size_t N>
ranges::subrange<char const*> c_str(char const (&sz)[N])
{
    return {&sz[0], &sz[N-1]};
}
#endif // RANGES_WORKAROUND_MSVC_790554

ranges::delimit_view<ranges::subrange<char const *, ranges::unreachable_sentinel_t>, char>
c_str_(char const *sz)
{
    return ranges::views::delimit(sz, '\0');
}

int main()
{
    using namespace ranges;

    {
        auto r0 = views::intersperse(c_str("abcde"), ',');
        CPP_assert(common_range<decltype(r0)>);
        CHECK((r0.end() - r0.begin()) == 9);
        CHECK(to<std::string>(r0) == "a,b,c,d,e");
        CHECK(r0.size() == 9u);

        auto r1 = views::intersperse(c_str(""), ',');
        CPP_assert(common_range<decltype(r1)>);
        CHECK(to<std::string>(r1) == "");
        CHECK(r1.size() == 0u);

        auto r2 = views::intersperse(c_str("a"), ',');
        CPP_assert(common_range<decltype(r2)>);
        CHECK(to<std::string>(r2) == "a");
        CHECK(r2.size() == 1u);

        auto r3 = views::intersperse(c_str("ab"), ',');
        CPP_assert(common_range<decltype(r3)>);
        CHECK(to<std::string>(r3) == "a,b");
        CHECK(r3.size() == 3u);
    }

    {
        auto r0 = views::intersperse(c_str("abcde"), ',') | views::reverse;
        CPP_assert(common_range<decltype(r0)>);
        CHECK(to<std::string>(r0) == "e,d,c,b,a");

        auto r1 = views::intersperse(c_str(""), ',') | views::reverse;
        CPP_assert(common_range<decltype(r1)>);
        CHECK(to<std::string>(r1) == "");

        auto r2 = views::intersperse(c_str("a"), ',') | views::reverse;
        CPP_assert(common_range<decltype(r2)>);
        CHECK(to<std::string>(r2) == "a");

        auto r3 = views::intersperse(c_str("ab"), ',') | views::reverse;
        CPP_assert(common_range<decltype(r3)>);
        CHECK(to<std::string>(r3) == "b,a");
    }

    {
        auto r0 = views::intersperse(c_str_("abcde"), ',');
        CPP_assert(!common_range<decltype(r0)>);
        CHECK(to<std::string>(r0) == "a,b,c,d,e");

        auto r1 = views::intersperse(c_str_(""), ',');
        CPP_assert(!common_range<decltype(r1)>);
        CHECK(to<std::string>(r1) == "");

        auto r2 = views::intersperse(c_str_("a"), ',');
        CPP_assert(!common_range<decltype(r2)>);
        CHECK(to<std::string>(r2) == "a");

        auto r3 = views::intersperse(c_str_("ab"), ',');
        CPP_assert(!common_range<decltype(r3)>);
        CHECK(to<std::string>(r3) == "a,b");
    }

    {
        auto r0 = views::intersperse(c_str("abcde"), ',');
        auto it = r0.begin();
        CHECK(*(it+0) == 'a');
        CHECK(*(it+1) == ',');
        CHECK(*(it+2) == 'b');
        CHECK(*(it+3) == ',');
        CHECK(*(it+4) == 'c');
        CHECK(*(it+5) == ',');
        CHECK(*(it+6) == 'd');
        CHECK(*(it+7) == ',');
        CHECK(*(it+8) == 'e');
        CHECK((it+9) == r0.end());

        it = r0.end();
        CHECK(*(it-9) == 'a');
        CHECK(*(it-8) == ',');
        CHECK(*(it-7) == 'b');
        CHECK(*(it-6) == ',');
        CHECK(*(it-5) == 'c');
        CHECK(*(it-4) == ',');
        CHECK(*(it-3) == 'd');
        CHECK(*(it-2) == ',');
        CHECK(*(it-1) == 'e');

        it = r0.begin();
        CHECK(((it+0)-it) == 0);
        CHECK(((it+1)-it) == 1);
        CHECK(((it+2)-it) == 2);
        CHECK(((it+3)-it) == 3);
        CHECK(((it+4)-it) == 4);
        CHECK(((it+5)-it) == 5);
        CHECK(((it+6)-it) == 6);
        CHECK(((it+7)-it) == 7);
        CHECK(((it+8)-it) == 8);
        CHECK(((it+9)-it) == 9);

        CHECK((it-(it+0)) == 0);
        CHECK((it-(it+1)) == -1);
        CHECK((it-(it+2)) == -2);
        CHECK((it-(it+3)) == -3);
        CHECK((it-(it+4)) == -4);
        CHECK((it-(it+5)) == -5);
        CHECK((it-(it+6)) == -6);
        CHECK((it-(it+7)) == -7);
        CHECK((it-(it+8)) == -8);
        CHECK((it-(it+9)) == -9);
    }

    {
        std::stringstream str{"1 2 3 4 5"};
        auto r0 = istream<int>(str) | views::intersperse(42);
        check_equal(r0, {1,42,2,42,3,42,4,42,5});
    }

    {
        int const some_ints[] = {1,2,3,4,5};
        auto rng = debug_input_view<int const>{some_ints} | views::intersperse(42);
        check_equal(rng, {1,42,2,42,3,42,4,42,5});
    }

    return test_result();
}
