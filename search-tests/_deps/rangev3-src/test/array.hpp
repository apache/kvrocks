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
// The implementation of array has been adapted from libc++
// (http://libcxx.llvm.org).
//
//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#ifndef RANGES_V3_TEST_ARRAY_HPP
#define RANGES_V3_TEST_ARRAY_HPP

#include <stdexcept>
#include <range/v3/range_fwd.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/algorithm/fill_n.hpp>
#include <range/v3/algorithm/swap_ranges.hpp>
#include <range/v3/algorithm/equal.hpp>
#include <range/v3/algorithm/lexicographical_compare.hpp>
#include <range/v3/utility/swap.hpp>

namespace test {

    /// \addtogroup group-utility
    /// A std::array with constexpr support
    template<typename T, std::size_t N>
    struct array
    {
        using self = array;
        using value_type = T;
        using reference = value_type&;
        using const_reference = value_type const&;
        using iterator = value_type*;
        using const_iterator = value_type const*;
        using pointer = value_type*;
        using const_pointer = value_type const*;
        using size_type = std::size_t;
        using difference_type = std::ptrdiff_t;
        using reverse_iterator = ranges::reverse_iterator<iterator>;
        using const_reverse_iterator = ranges::reverse_iterator<const_iterator>;

        value_type elems_[N > 0 ? N : 1];

        constexpr /*c++14*/ void fill(const value_type& u)
        {
            ranges::fill_n(elems_, N, u);
        }
        constexpr /*c++14*/
        void swap(array& a) noexcept(ranges::is_nothrow_swappable<T>::value)
        {
            ranges::swap_ranges(elems_, elems_ + N, a.elems_);
        }
         // iterators:
        constexpr /*c++14*/
        iterator begin() noexcept
        {
            return iterator(elems_);
        }
        constexpr /*c++14*/
        const_iterator begin() const noexcept
        {
            return const_iterator(elems_);
        }
        constexpr /*c++14*/
        iterator end() noexcept
        {
            return iterator(elems_ + N);
        }
        constexpr /*c++14*/
        const_iterator end() const noexcept
        {
            return const_iterator(elems_ + N);
        }
        constexpr /*c++14*/
        reverse_iterator rbegin() noexcept
        {
            return reverse_iterator(end());
        }
        constexpr /*c++14*/
        const_reverse_iterator rbegin() const noexcept
        {
            return const_reverse_iterator(end());
        }
        constexpr /*c++14*/
        reverse_iterator rend() noexcept
        {
            return reverse_iterator(begin());
        }
        constexpr /*c++14*/
        const_reverse_iterator rend() const noexcept
        {
            return const_reverse_iterator(begin());
        }
        constexpr /*c++14*/
        const_iterator cbegin() const noexcept
        {
            return begin();
        }
        constexpr /*c++14*/
        const_iterator cend() const noexcept
        {
            return end();
        }
        constexpr /*c++14*/
        const_reverse_iterator crbegin() const noexcept
        {
            return rbegin();
        }
        constexpr /*c++14*/
        const_reverse_iterator crend() const noexcept
        {
            return rend();
        }
        // capacity:
        constexpr /*c++14*/
        size_type size() const noexcept
        {
            return N;
        }
        constexpr /*c++14*/
        size_type max_size() const noexcept
        {
            return N;
        }
        constexpr /*c++14*/
        bool empty() const noexcept
        {
            return N == 0;
        }
         // element access:
        constexpr /*c++14*/ reference operator[](size_type n)
        {
            return elems_[n];
        }
        constexpr /*c++14*/ const_reference operator[](size_type n) const
        {
            return elems_[n];
        }
        constexpr /*c++14*/ reference at(size_type n)
        {
            if (n >= N)
                throw std::out_of_range("array::at");
            return elems_[n];
        }
        constexpr /*c++14*/ const_reference at(size_type n) const
        {
            if (n >= N)
                throw std::out_of_range("array::at");
            return elems_[n];
        }
        constexpr /*c++14*/ reference front()
        {
            return elems_[0];
        }
        constexpr /*c++14*/ const_reference front() const
        {
            return elems_[0];
        }
        constexpr /*c++14*/ reference back()
        {
            return elems_[N > 0 ? N-1 : 0];
        }
        constexpr /*c++14*/ const_reference back() const
        {
            return elems_[N > 0 ? N-1 : 0];
        }
        constexpr /*c++14*/
        value_type* data() noexcept
        {
            return elems_;
        }
        constexpr /*c++14*/
        const value_type* data() const noexcept
        {
            return elems_;
        }
    };

    template<class T, size_t N>
    constexpr /*c++14*/
    bool
    operator==(const array<T, N>& x, const array<T, N>& y)
    {
        return ranges::equal(x.elems_, x.elems_ + N, y.elems_);
    }
    template<class T, size_t N>
    constexpr /*c++14*/
    bool
    operator!=(const array<T, N>& x, const array<T, N>& y)
    {
        return !(x == y);
    }
    template<class T, size_t N>
    constexpr /*c++14*/
    bool
    operator<(const array<T, N>& x, const array<T, N>& y)
    {
        return ranges::lexicographical_compare(x.elems_, x.elems_ + N, y.elems_, y.elems_ + N);
    }
    template<class T, size_t N>
    constexpr /*c++14*/
    bool
    operator>(const array<T, N>& x, const array<T, N>& y)
    {
        return y < x;
    }
    template<class T, size_t N>
    constexpr /*c++14*/
    bool
    operator<=(const array<T, N>& x, const array<T, N>& y)
    {
        return !(y < x);
    }

    template<class T, size_t N>
    constexpr /*c++14*/
    bool
    operator>=(const array<T, N>& x, const array<T, N>& y)
    {
         return !(x < y);
    }

    template<class T, size_t N>
    constexpr /*c++14*/
    auto swap(array<T, N>& x, array<T, N>& y)
    noexcept(ranges::is_nothrow_swappable<T>::value)
    -> typename std::enable_if<ranges::is_swappable<T>::value, void>::type
    {
        x.swap(y);
    }

    template<size_t I, class T, size_t N>
    constexpr /*c++14*/
    T& get(array<T, N>& a) noexcept
    {
        static_assert(I < N, "Index out of bounds in ranges::get<> (ranges::array)");
        return a.elems_[I];
    }

    template<size_t I, class T, size_t N>
    constexpr /*c++14*/
    const T& get(const array<T, N>& a) noexcept
    {
        static_assert(I < N, "Index out of bounds in ranges::get<> (const ranges::array)");
        return a.elems_[I];
    }

    template<size_t I, class T, size_t N>
    constexpr /*c++14*/
    T && get(array<T, N>&& a) noexcept
    {
        static_assert(I < N, "Index out of bounds in ranges::get<> (ranges::array &&)");
        return std::move(a.elems_[I]);
    }

    template<class T, std::size_t N>
    constexpr /*c++14*/ void swap(array<T, N>& a, array<T, N>& b) {
        for(std::size_t i = 0; i != N; ++i) {
            auto tmp = std::move(a[i]);
            a[i] = std::move(b[i]);
            b[i] = std::move(tmp);
        }
    }
}  // namespace test

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_MISMATCHED_TAGS

namespace std
{

template<class T, size_t N>
class tuple_size<test::array<T, N>>
    : public integral_constant<size_t, N> {};

template<size_t I, class T, size_t N>
class tuple_element<I, test::array<T, N> >
{
 public:
    using type = T;
};

}  // namespace std

RANGES_DIAGNOSTIC_POP

#endif // RANGES_V3_TEST_ARRAY_HPP
