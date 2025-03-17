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

#include <list>
#include <map>
#include <vector>

#include <range/v3/action/sort.hpp>
#include <range/v3/core.hpp>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/indices.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/zip.hpp>
#include <range/v3/view/reverse.hpp>

#include "../simple_test.hpp"
#include "../test_utils.hpp"

template<typename T>
struct vector_like
{
private:
    std::vector<T> data_;
public:
    using size_type = std::size_t;
    using allocator_type = std::allocator<T>;

    vector_like() = default;
    template<typename I>
    vector_like(I first, I last)
      : data_(first, last)
    {}
    template<typename I>
    void assign(I first, I last)
    {
        data_.assign(first, last);
    }

    auto begin()
    {
        return data_.begin();
    }
    auto end()
    {
        return data_.end();
    }
    auto begin() const
    {
        return data_.begin();
    }
    auto end() const
    {
        return data_.end();
    }
    size_type size() const
    {
        return data_.size();
    }
    size_type capacity() const
    {
        return data_.capacity();
    }
    size_type max_size() const
    {
        return data_.max_size();
    }
    auto& operator[](size_type n)
    {
        return data_[n];
    }
    auto& operator[](size_type n) const
    {
        return data_[n];
    }

    size_type last_reservation{};
    size_type reservation_count{};

    void reserve(size_type n)
    {
        data_.reserve(n);
        last_reservation = n;
        ++reservation_count;
    }
};

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
template<typename I>
vector_like(I, I) -> vector_like<ranges::iter_value_t<I>>;

template<typename Rng, typename CI = ranges::range_common_iterator_t<Rng>,
         typename = decltype(std::map{CI{}, CI{}})>
void test_zip_to_map(Rng && rng, int)
{
    using namespace ranges;
#ifdef RANGES_WORKAROUND_MSVC_779708
    auto m = static_cast<Rng &&>(rng) | to<std::map>();
#else  // ^^^ workaround / no workaround vvv
    auto m = static_cast<Rng &&>(rng) | to<std::map>;
#endif // RANGES_WORKAROUND_MSVC_779708
    CPP_assert(same_as<decltype(m), std::map<int, int>>);
}
#endif
template<typename Rng>
void test_zip_to_map(Rng &&, long)
{}

template<typename K, typename V>
struct map_like : std::map<K, V>
{
    template<typename Iter>
    map_like(Iter f, Iter l)
      : std::map<K, V>(f, l)
    {}
};

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
template<typename Iter>
map_like(Iter, Iter) -> map_like<typename ranges::iter_value_t<Iter>::first_type,
                                 typename ranges::iter_value_t<Iter>::second_type>;
#endif

int main()
{
    using namespace ranges;

    {
        auto lst0 = views::ints | views::transform([](int i) { return i * i; }) |
                    views::take(10) | to<std::list>();
        CPP_assert(same_as<decltype(lst0), std::list<int>>);
        ::check_equal(lst0, {0, 1, 4, 9, 16, 25, 36, 49, 64, 81});
    }

#ifndef RANGES_WORKAROUND_MSVC_779708 // "workaround" is a misnomer; there's no
                                      // workaround.
    {
        auto lst1 = views::ints | views::transform([](int i) { return i * i; }) |
                    views::take(10) | to<std::list>;
        CPP_assert(same_as<decltype(lst1), std::list<int>>);
        ::check_equal(lst1, {0, 1, 4, 9, 16, 25, 36, 49, 64, 81});
    }
#endif // RANGES_WORKAROUND_MSVC_779708

    {
        auto vec0 = views::ints | views::transform([](int i) { return i * i; }) |
                    views::take(10) | to_vector | actions::sort(std::greater<int>{});
        CPP_assert(same_as<decltype(vec0), std::vector<int>>);
        ::check_equal(vec0, {81, 64, 49, 36, 25, 16, 9, 4, 1, 0});
    }

    {
        auto vec1 = views::ints | views::transform([](int i) { return i * i; }) |
                    views::take(10) | to<std::vector<long>>() |
                    actions::sort(std::greater<long>{});
        CPP_assert(same_as<decltype(vec1), std::vector<long>>);
        ::check_equal(vec1, {81, 64, 49, 36, 25, 16, 9, 4, 1, 0});
    }

#ifndef RANGES_WORKAROUND_MSVC_779708
    {
        auto vec2 = views::ints | views::transform([](int i) { return i * i; }) |
                    views::take(10) | to<std::vector<long>> |
                    actions::sort(std::greater<long>{});
        CPP_assert(same_as<decltype(vec2), std::vector<long>>);
        ::check_equal(vec2, {81, 64, 49, 36, 25, 16, 9, 4, 1, 0});
    }
#endif // RANGES_WORKAROUND_MSVC_779708

    {
        const std::size_t N = 4096;
        auto vl = views::iota(0, int{N}) | to<vector_like<int>>();
        CPP_assert(same_as<decltype(vl), vector_like<int>>);
        CHECK(vl.reservation_count == std::size_t{1});
        CHECK(vl.last_reservation == N);
    }

    // https://github.com/ericniebler/range-v3/issues/1145
    {
        auto r1 = views::indices(std::uintmax_t{100});
        auto r2 = views::zip(r1, r1);

#ifdef RANGES_WORKAROUND_MSVC_779708
        auto m = r2 | ranges::to<std::map<std::uintmax_t, std::uintmax_t>>();
#else // ^^^ workaround / no workaround vvv
        auto m = r2 | ranges::to<std::map<std::uintmax_t, std::uintmax_t>>;
#endif // RANGES_WORKAROUND_MSVC_779708
        CPP_assert(same_as<decltype(m), std::map<std::uintmax_t, std::uintmax_t>>);
    }

    // Transform a range-of-ranges into a container of containers
    {
        auto r = views::ints(1, 4) |
                 views::transform([](int i) { return views::ints(i, i + 3); });

        auto m = r | ranges::to<std::vector<std::vector<int>>>();
        CPP_assert(same_as<decltype(m), std::vector<std::vector<int>>>);
        CHECK(m.size() == 3u);
        check_equal(m[0], {1, 2, 3});
        check_equal(m[1], {2, 3, 4});
        check_equal(m[2], {3, 4, 5});
    }

    // Use ranges::to in a closure with an action
    {
#ifdef RANGES_WORKAROUND_MSVC_779708
        auto closure = ranges::to<std::vector>() | actions::sort;
#else // ^^^ workaround / no workaround vvv
        auto closure = ranges::to<std::vector> | actions::sort;
#endif // RANGES_WORKAROUND_MSVC_779708

        auto r = views::ints(1, 4) | views::reverse;
        auto m = r | closure;

        CPP_assert(same_as<decltype(m), std::vector<int>>);
        CHECK(m.size() == 3u);
        check_equal(m, {1, 2, 3});
    }

    test_zip_to_map(views::zip(views::ints, views::iota(0, 10)), 0);

    // https://github.com/ericniebler/range-v3/issues/1544
    {
        std::vector<std::vector<int>> d;
        auto m = views::transform(d, views::all);
        auto v = ranges::to<std::vector<std::vector<int>>>(m);
        check_equal(d, v);
    }

    {
        std::vector<std::pair<int, int>> v = {{1, 2}, {3, 4}};
        auto m1 = ranges::to<map_like<int, int>>(v);
        auto m2 = v | ranges::to<map_like<int, int>>();

        CPP_assert(same_as<decltype(m1), map_like<int, int>>);
        CPP_assert(same_as<decltype(m2), map_like<int, int>>);
        check_equal(m1, std::map<int, int>{{1, 2}, {3, 4}});
        check_equal(m1, m2);

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
        auto m3 = ranges::to<map_like>(v);
        auto m4 = v | ranges::to<map_like>();
        CPP_assert(same_as<decltype(m3), map_like<int, int>>);
        CPP_assert(same_as<decltype(m4), map_like<int, int>>);
        check_equal(m1, m3);
        check_equal(m1, m4);
#endif
    }

    return ::test_result();
}
