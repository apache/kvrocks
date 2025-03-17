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

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <random>
#include <vector>
#include <algorithm>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/sort.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/view/for_each.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/repeat_n.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/zip.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/range/conversion.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"
#include "../test_iterators.hpp"

RANGES_DIAGNOSTIC_IGNORE_GLOBAL_CONSTRUCTORS
RANGES_DIAGNOSTIC_IGNORE_SIGN_CONVERSION
RANGES_DIAGNOSTIC_IGNORE_UNNEEDED_INTERNAL

// BUGBUG
namespace std
{
    template<typename F, typename S>
    std::ostream & operator<<(std::ostream &sout, std::pair<F,S> const & p)
    {
        return sout << '[' << p.first << ',' << p.second << ']';
    }
}

namespace
{
    std::mt19937 gen;

    struct indirect_less
    {
        template<class P>
        bool operator()(const P& x, const P& y)
            {return *x < *y;}
    };

    template<class RI>
    void
    test_sort_helper(RI f, RI l)
    {
        using value_type = ranges::iter_value_t<RI>;
        auto sort = make_testable_1<false>(ranges::sort);
        if (f != l)
        {
            auto len = l - f;
            value_type* save(new value_type[len]);
            do
            {
                std::copy(f, l, save);
                sort(save, save+len).check([&](int *res)
                {
                    CHECK(res == save+len);
                    CHECK(std::is_sorted(save, save+len));
                    std::copy(f, l, save);
                });
                sort(save, save+len, std::greater<int>{}).check([&](int *res)
                {
                    CHECK(res == save+len);
                    CHECK(std::is_sorted(save, save+len, std::greater<int>{}));
                    std::copy(f, l, save);
                });
            } while (std::next_permutation(f, l));
            delete [] save;
        }
    }

    template<class RI>
    void
    test_sort_driver_driver(RI f, RI l, int start, RI real_last)
    {
        for (RI i = l; i > f + start;)
        {
            *--i = start;
            if (f == i)
            {
                test_sort_helper(f, real_last);
            }
            if (start > 0)
                test_sort_driver_driver(f, i, start-1, real_last);
        }
    }

    template<class RI>
    void
    test_sort_driver(RI f, RI l, int start)
    {
        test_sort_driver_driver(f, l, start, l);
    }

    template<int sa>
    void
    test_sort_()
    {
        int ia[sa];
        for (int i = 0; i < sa; ++i)
        {
            test_sort_driver(ia, ia+sa, i);
        }
    }

    void
    test_larger_sorts(int N, int M)
    {
        RANGES_ENSURE(N > 0);
        RANGES_ENSURE(M > 0);
        // create array length N filled with M different numbers
        int* array = new int[N];
        int x = 0;
        for (int i = 0; i < N; ++i)
        {
            array[i] = x;
            if (++x == M)
                x = 0;
        }

        // test saw tooth pattern
        CHECK(ranges::sort(array, array+N) == array+N);
        CHECK(std::is_sorted(array, array+N));
        // test random pattern
        std::shuffle(array, array+N, gen);
        CHECK(ranges::sort(array, array+N) == array+N);
        CHECK(std::is_sorted(array, array+N));
        // test sorted pattern
        CHECK(ranges::sort(array, array+N) == array+N);
        CHECK(std::is_sorted(array, array+N));
        // test reverse sorted pattern
        std::reverse(array, array+N);
        CHECK(ranges::sort(array, array+N) == array+N);
        CHECK(std::is_sorted(array, array+N));
        // test swap ranges 2 pattern
        std::swap_ranges(array, array+N/2, array+N/2);
        CHECK(ranges::sort(array, array+N) == array+N);
        CHECK(std::is_sorted(array, array+N));
        // test reverse swap ranges 2 pattern
        std::reverse(array, array+N);
        std::swap_ranges(array, array+N/2, array+N/2);
        CHECK(ranges::sort(array, array+N) == array+N);
        CHECK(std::is_sorted(array, array+N));
        delete [] array;
    }

    void
    test_larger_sorts(unsigned N)
    {
        test_larger_sorts(N, 1);
        test_larger_sorts(N, 2);
        test_larger_sorts(N, 3);
        test_larger_sorts(N, N/2-1);
        test_larger_sorts(N, N/2);
        test_larger_sorts(N, N/2+1);
        test_larger_sorts(N, N-2);
        test_larger_sorts(N, N-1);
        test_larger_sorts(N, N);
    }

    struct S
    {
        int i, j;
    };

    struct Int
    {
        using difference_type = int;
        int i_;
        Int(int i = 0) : i_(i) {}
        Int(Int && that) : i_(that.i_) { that.i_ = 0; }
        Int(Int const &) = delete;
        Int & operator=(Int && that)
        {
            i_ = that.i_;
            that.i_ = 0;
            return *this;
        }
        friend bool operator==(Int const &a, Int const &b)
        {
            return a.i_ == b.i_;
        }
        friend bool operator!=(Int const &a, Int const &b)
        {
            return !(a == b);
        }
        friend bool operator<(Int const &a, Int const &b)
        {
            return a.i_ < b.i_;
        }
        friend bool operator>(Int const &a, Int const &b)
        {
            return a.i_ > b.i_;
        }
        friend bool operator<=(Int const &a, Int const &b)
        {
            return a.i_ <= b.i_;
        }
        friend bool operator>=(Int const &a, Int const &b)
        {
            return a.i_ >= b.i_;
        }
    };
    CPP_assert(ranges::default_constructible<Int>);
    CPP_assert(ranges::movable<Int>);
    CPP_assert(ranges::totally_ordered<Int>);
}

int main()
{
    // test null range
    int d = 0;
    ranges::sort(&d, &d);
    // exhaustively test all possibilities up to length 8
    test_sort_<1>();
    test_sort_<2>();
    test_sort_<3>();
    test_sort_<4>();
    test_sort_<5>();
    test_sort_<6>();
    test_sort_<7>();
    test_sort_<8>();

    test_larger_sorts(15);
    test_larger_sorts(16);
    test_larger_sorts(17);
    test_larger_sorts(256);
    test_larger_sorts(257);
    test_larger_sorts(499);
    test_larger_sorts(500);
    test_larger_sorts(997);
    test_larger_sorts(1000);
    test_larger_sorts(1009);

    // Check move-only types
    {
        std::vector<std::unique_ptr<int> > v(1000);
        for(int i = 0; (std::size_t)i < v.size(); ++i)
            v[i].reset(new int((int)v.size() - i - 1));
        ranges::sort(v, indirect_less());
        for(int i = 0; (std::size_t)i < v.size(); ++i)
            CHECK(*v[i] == i);
    }

    // Check projections
    {
        std::vector<S> v(1000, S{});
        for(int i = 0; (std::size_t)i < v.size(); ++i)
        {
            v[i].i = (int)v.size() - i - 1;
            v[i].j = i;
        }
        ranges::sort(v, std::less<int>{}, &S::i);
        for(int i = 0; (std::size_t)i < v.size(); ++i)
        {
            CHECK(v[i].i == i);
            CHECK((std::size_t)v[i].j == v.size() - i - 1);
        }
    }

    // Check rvalue range
    {
        std::vector<S> v(1000, S{});
        for(int i = 0; (std::size_t)i < v.size(); ++i)
        {
            v[i].i = (int)v.size() - i - 1;
            v[i].j = i;
        }
        CHECK(ranges::sort(ranges::views::all(v), std::less<int>{}, &S::i) == v.end());
        for(int i = 0; (std::size_t)i < v.size(); ++i)
        {
            CHECK(v[i].i == i);
            CHECK((std::size_t)v[i].j == v.size() - i - 1);
        }
    }
    {
        std::vector<S> v(1000, S{});
        for(int i = 0; (std::size_t)i < v.size(); ++i)
        {
            v[i].i = (int)v.size() - i - 1;
            v[i].j = i;
        }
        CHECK(::is_dangling(ranges::sort(std::move(v), std::less<int>{}, &S::i)));
        for(int i = 0; (std::size_t)i < v.size(); ++i)
        {
            CHECK(v[i].i == i);
            CHECK((std::size_t)v[i].j == v.size() - i - 1);
        }
    }

    // Check sorting a zip view, which uses iter_move
    {
        using namespace ranges;
        auto v0 =
            views::for_each(views::ints(1,6) | views::reverse, [](int i){
                return ranges::yield_from(views::repeat_n(i,i));
            }) | to<std::vector>();
        auto v1 = ranges::to<std::vector<Int>>(
            {1,2,2,3,3,3,4,4,4,4,5,5,5,5,5});
        auto rng = views::zip(v0, v1);
        ::check_equal(v0,{5,5,5,5,5,4,4,4,4,3,3,3,2,2,1});
        ::check_equal(v1,{1,2,2,3,3,3,4,4,4,4,5,5,5,5,5});
        using Rng = decltype(rng);
        using CR = range_common_reference_t<Rng>;
        auto proj = [](CR r) { return r; };
        auto pred = [](CR r1, CR r2) { return r1 < r2; };
        sort(rng, pred, proj);
        ::check_equal(v0,{1,2,2,3,3,3,4,4,4,4,5,5,5,5,5});
        ::check_equal(v1,{5,5,5,4,5,5,3,4,4,4,1,2,2,3,3});

        // Check that this compiles, too:
        sort(rng);
    }

    return ::test_result();
}
