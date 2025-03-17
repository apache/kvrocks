// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

#include <vector>
#include <string>
#include <range/v3/core.hpp>
#include <range/v3/action/join.hpp>
#include <range/v3/algorithm/move.hpp>
#include <range/v3/algorithm/equal.hpp>
#include <range/v3/view/transform.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    std::vector<std::string> v {"hello"," ","world"};
    auto s = v | move | actions::join;
    static_assert(std::is_same<decltype(s), std::string>::value, "");
    CHECK(s == "hello world");

    auto s2 = v | views::transform(views::all) | actions::join;
    static_assert(std::is_same<decltype(s2), std::vector<char>>::value, "");
    CHECK(std::string(s2.begin(), s2.end()) == "hello world");

    return ::test_result();
}
