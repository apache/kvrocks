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

#include <cctype>
#include <string>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/view/delimit.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/unique.hpp>
#include <range/v3/utility/copy.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/insert_iterators.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

using std::toupper;

// from http://stackoverflow.com/a/2886589/195873
struct ci_char_traits : std::char_traits<char>
{
    static bool eq(char c1, char c2) { return toupper(c1) == toupper(c2); }
    static bool ne(char c1, char c2) { return toupper(c1) != toupper(c2); }
    static bool lt(char c1, char c2) { return toupper(c1) <  toupper(c2); }
    static int compare(const char* s1, const char* s2, std::size_t n)
    {
        for(; n != 0; ++s1, ++s2, --n)
        {
            if(toupper(*s1) < toupper(*s2))
                return -1;
            if(toupper(*s1) > toupper(*s2))
                return 1;
        }
        return 0;
    }
    static const char* find(const char* s, int n, char a)
    {
        for(; n-- > 0; ++s)
            if(toupper(*s) == toupper(a))
                break;
        return s;
    }
};

using ci_string = std::basic_string<char, ci_char_traits>;

int main()
{
    using namespace ranges;

    {
        int rgi[] = {1, 1, 1, 2, 3, 4, 4};
        std::vector<int> out;

        auto rng = rgi | views::unique;
        has_type<int &>(*begin(rng));
        CPP_assert(view_<decltype(rng)>);
        CPP_assert(bidirectional_range<decltype(rng)>);
        CPP_assert(!random_access_range<decltype(rng)>);
        CPP_assert(common_range<decltype(rng)>);
        CPP_assert(!sized_range<decltype(rng)>);
        CPP_assert(range<decltype(rng) const>);
        copy(rng, ranges::back_inserter(out));
        ::check_equal(out, {1, 2, 3, 4});
        ::check_equal(views::reverse(out), {4, 3, 2, 1});
    }

    {
        std::vector<ci_string> rgs{"hello", "HELLO", "bye", "Bye", "BYE"};
        auto rng = rgs | views::unique;
        has_type<ci_string &>(*begin(rng));
        CPP_assert(view_<decltype(rng)>);
        CPP_assert(bidirectional_range<decltype(rng)>);
        CPP_assert(!random_access_range<decltype(rng)>);
        CPP_assert(common_range<decltype(rng)>);
        CPP_assert(!sized_range<decltype(rng)>);
        CPP_assert(range<decltype(rng) const>);
        auto fs = rng | views::transform([](ci_string s){return std::string(s.data(), s.size());});
        CPP_assert(view_<decltype(fs)>);
        CPP_assert(bidirectional_range<decltype(fs)>);
        ::check_equal(fs, {"hello","bye"});
        ::check_equal(views::reverse(fs), {"bye","hello"});
    }

    {
        int const rgi[] = {1, 1, 1, 2, 3, 4, 4, 42, 7};
        auto rng0 = views::delimit(rgi, 42) | views::reverse;
        // rng0 is mutable-only...
        CPP_assert(forward_range<decltype(rng0)>);
        CPP_assert(!forward_range<decltype(rng0) const>);
        // ...and composable
        auto rng = rng0 | views::unique(equal_to{});
        CPP_assert(view_<decltype(rng)>);
        CPP_assert(bidirectional_range<decltype(rng)>);
        CPP_assert(!random_access_range<decltype(rng)>);
        CPP_assert(common_range<decltype(rng)>);
        CPP_assert(!sized_range<decltype(rng)>);
        ::check_equal(rng, {4, 3, 2, 1});
    }

    {
        auto const caseInsensitiveCompare = [](const std::string& s1, const std::string& s2){
            if (s1.size() != s2.size())
                return false;
            for (unsigned i = 0; i < s1.size(); i++)
                if (toupper(s1[i]) != toupper(s2[i]))
                    return false;

            return true;
        };

        std::vector<std::string> rgs{"hello", "HELLO", "bye", "Bye", "BYE"};
        auto rng = rgs | views::unique(caseInsensitiveCompare);
        has_type<std::string &>(*begin(rng));
        CPP_assert(view_<decltype(rng)>);
        CPP_assert(bidirectional_range<decltype(rng)>);
        CPP_assert(!random_access_range<decltype(rng)>);
        CPP_assert(common_range<decltype(rng)>);
        CPP_assert(!sized_range<decltype(rng)>);
        CPP_assert(range<decltype(rng) const>);
        CPP_assert(view_<decltype(rng)>);
        CPP_assert(bidirectional_range<decltype(rng)>);
        ::check_equal(rng, {"hello","bye"});
        ::check_equal(views::reverse(rng), {"bye","hello"});
    }

    {
        int const rgi[] = {-1, 1, -1, 2, 3, 4, -4, 42, 7};
        auto rng0 = views::delimit(rgi, 42) | views::reverse;
        // rng0 is mutable-only...
        CPP_assert(forward_range<decltype(rng0)>);
        CPP_assert(!forward_range<decltype(rng0) const>);
        // ...and composable
        auto rng = rng0 | views::unique([](const int& n1, const int& n2){ return n1==n2 || n1==-n2; })
                        | views::transform([](const int& n){ return n > 0 ? n: -n;});
        CPP_assert(view_<decltype(rng)>);
        CPP_assert(bidirectional_range<decltype(rng)>);
        CPP_assert(!random_access_range<decltype(rng)>);
        CPP_assert(common_range<decltype(rng)>);
        CPP_assert(!sized_range<decltype(rng)>);
        ::check_equal(rng, {4, 3, 2, 1});
    }

    return test_result();
}
