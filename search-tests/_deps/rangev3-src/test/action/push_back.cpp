// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

#include <list>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/for_each.hpp>
#include <range/v3/action/push_back.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

int main()
{
    using namespace ranges;

    {
        std::vector<int> v;
        push_back(v, {1,2,3});
        ::check_equal(v, {1,2,3});

        push_back(v, views::iota(10) | views::take(3));
        ::check_equal(v, {1,2,3,10,11,12});

        push_back(v, views::iota(10) | views::take(3));
        ::check_equal(v, {1,2,3,10,11,12,10,11,12});

        int rg[] = {9,8,7};
        push_back(v, rg);
        ::check_equal(v, {1,2,3,10,11,12,10,11,12,9,8,7});
        push_back(v, rg);
        ::check_equal(v, {1,2,3,10,11,12,10,11,12,9,8,7,9,8,7});

        std::list<int> s;
        push_back(s,
            views::ints|views::take(10)|views::for_each([](int i){return yield_if(i%2==0,i);}));
        ::check_equal(s, {0,2,4,6,8});
        push_back(s, 10);
        ::check_equal(s, {0,2,4,6,8,10});
    }

    {
        std::vector<int> v;
        v = std::move(v) | push_back({1,2,3});
        ::check_equal(v, {1,2,3});

        v = std::move(v) | push_back(views::iota(10) | views::take(3));
        ::check_equal(v, {1,2,3,10,11,12});

        v = std::move(v) | push_back(views::iota(10) | views::take(3));
        ::check_equal(v, {1,2,3,10,11,12,10,11,12});

        int rg[] = {9,8,7};
        v = std::move(v) | push_back(rg);
        ::check_equal(v, {1,2,3,10,11,12,10,11,12,9,8,7});
        v = std::move(v) | push_back(rg);
        ::check_equal(v, {1,2,3,10,11,12,10,11,12,9,8,7,9,8,7});

        std::list<int> s;
        s = std::move(s) | push_back(
            views::ints|views::take(10)|views::for_each([](int i){return yield_if(i%2==0,i);}));
        ::check_equal(s, {0,2,4,6,8});
        s = std::move(s) | push_back(10);
        ::check_equal(s, {0,2,4,6,8,10});
    }

    {
        std::vector<int> v;
        v |= push_back({1,2,3});
        ::check_equal(v, {1,2,3});

        v |= push_back(views::iota(10) | views::take(3));
        ::check_equal(v, {1,2,3,10,11,12});

        v |= push_back(views::iota(10) | views::take(3));
        ::check_equal(v, {1,2,3,10,11,12,10,11,12});

        int rg[] = {9,8,7};
        v |= push_back(rg);
        ::check_equal(v, {1,2,3,10,11,12,10,11,12,9,8,7});
        v |= push_back(rg);
        ::check_equal(v, {1,2,3,10,11,12,10,11,12,9,8,7,9,8,7});

        std::list<int> s;
        s |= push_back(
            views::ints|views::take(10)|views::for_each([](int i){return yield_if(i%2==0,i);}));
        ::check_equal(s, {0,2,4,6,8});
        s |= push_back(10);
        ::check_equal(s, {0,2,4,6,8,10});
    }

    return ::test_result();
}
