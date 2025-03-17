/// \file
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
//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
#ifndef RANGES_V3_ALGORITHM_INPLACE_MERGE_HPP
#define RANGES_V3_ALGORITHM_INPLACE_MERGE_HPP

#include <functional>
#include <memory>
#include <new>
#include <type_traits>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/lower_bound.hpp>
#include <range/v3/algorithm/merge.hpp>
#include <range/v3/algorithm/min.hpp>
#include <range/v3/algorithm/move.hpp>
#include <range/v3/algorithm/rotate.hpp>
#include <range/v3/algorithm/upper_bound.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/functional/not_fn.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/move_iterators.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/reverse_iterator.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/dangling.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/memory.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/utility/swap.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        struct merge_adaptive_fn
        {
        private:
            template<typename I, typename C, typename P>
            static void impl(I first, I middle, I last, iter_difference_t<I> len1,
                             iter_difference_t<I> len2, iter_value_t<I> * const buf,
                             C & pred, P & proj)
            {
                auto tmpbuf = make_raw_buffer(buf);
                if(len1 <= len2)
                {
                    auto p = ranges::move(first, middle, tmpbuf.begin()).out;
                    merge(make_move_iterator(buf),
                          make_move_iterator(p.base().base()),
                          make_move_iterator(std::move(middle)),
                          make_move_iterator(std::move(last)),
                          std::move(first),
                          std::ref(pred),
                          std::ref(proj),
                          std::ref(proj));
                }
                else
                {
                    auto p = ranges::move(middle, last, tmpbuf.begin()).out;
                    using RBi = ranges::reverse_iterator<I>;
                    using Rv = ranges::reverse_iterator<iter_value_t<I> *>;
                    merge(make_move_iterator(RBi{std::move(middle)}),
                          make_move_iterator(RBi{std::move(first)}),
                          make_move_iterator(Rv{p.base().base()}),
                          make_move_iterator(Rv{buf}),
                          RBi{std::move(last)},
                          not_fn(std::ref(pred)),
                          std::ref(proj),
                          std::ref(proj));
                }
            }

        public:
            template(typename I, typename C = less, typename P = identity)(
                requires bidirectional_iterator<I> AND sortable<I, C, P>)
            void operator()(I first, I middle, I last, iter_difference_t<I> len1,
                            iter_difference_t<I> len2, iter_value_t<I> * buf,
                            std::ptrdiff_t buf_size, C pred = C{}, P proj = P{}) const
            {
                using D = iter_difference_t<I>;
                while(true)
                {
                    // if middle == last, we're done
                    if(len2 == 0)
                        return;
                    // shrink [first, middle) as much as possible (with no moves),
                    // returning if it shrinks to 0
                    for(; true; ++first, --len1)
                    {
                        if(len1 == 0)
                            return;
                        if(invoke(pred, invoke(proj, *middle), invoke(proj, *first)))
                            break;
                    }
                    if(len1 <= buf_size || len2 <= buf_size)
                    {
                        merge_adaptive_fn::impl(std::move(first),
                                                std::move(middle),
                                                std::move(last),
                                                len1,
                                                len2,
                                                buf,
                                                pred,
                                                proj);
                        return;
                    }
                    // first < middle < end
                    // *first > *middle
                    // partition [first, m1) [m1, middle) [middle, m2) [m2, last) such
                    // that
                    //     all elements in:
                    //         [first, m1)  <= [middle, m2)
                    //         [middle, m2) <  [m1, middle)
                    //         [m1, middle) <= [m2, last)
                    //     and m1 or m2 is in the middle of its range
                    I m1;    // "median" of [first, middle)
                    I m2;    // "median" of [middle, last)
                    D len11; // distance(first, m1)
                    D len21; // distance(middle, m2)
                    // binary search smaller range
                    if(len1 < len2)
                    { // len >= 1, len2 >= 2
                        len21 = len2 / 2;
                        m2 = next(middle, len21);
                        m1 = upper_bound(first,
                                         middle,
                                         invoke(proj, *m2),
                                         std::ref(pred),
                                         std::ref(proj));
                        len11 = distance(first, m1);
                    }
                    else
                    {
                        if(len1 == 1)
                        { // len1 >= len2 && len2 > 0, therefore len2 == 1
                            // It is known *first > *middle
                            ranges::iter_swap(first, middle);
                            return;
                        }
                        // len1 >= 2, len2 >= 1
                        len11 = len1 / 2;
                        m1 = next(first, len11);
                        m2 = lower_bound(middle,
                                         last,
                                         invoke(proj, *m1),
                                         std::ref(pred),
                                         std::ref(proj));
                        len21 = distance(middle, m2);
                    }
                    D len12 = len1 - len11; // distance(m1, middle)
                    D len22 = len2 - len21; // distance(m2, last)
                    // [first, m1) [m1, middle) [middle, m2) [m2, last)
                    // swap middle two partitions
                    middle = rotate(m1, std::move(middle), m2).begin();
                    // len12 and len21 now have swapped meanings
                    // merge smaller range with recursive call and larger with tail
                    // recursion elimination
                    if(len11 + len21 < len12 + len22)
                    {
                        (*this)(std::move(first),
                                std::move(m1),
                                middle,
                                len11,
                                len21,
                                buf,
                                buf_size,
                                std::ref(pred),
                                std::ref(proj));
                        first = std::move(middle);
                        middle = std::move(m2);
                        len1 = len12;
                        len2 = len22;
                    }
                    else
                    {
                        (*this)(middle,
                                std::move(m2),
                                std::move(last),
                                len12,
                                len22,
                                buf,
                                buf_size,
                                std::ref(pred),
                                std::ref(proj));
                        last = std::move(middle);
                        middle = std::move(m1);
                        len1 = len11;
                        len2 = len21;
                    }
                }
            }
        };

        RANGES_INLINE_VARIABLE(merge_adaptive_fn, merge_adaptive)

        struct inplace_merge_no_buffer_fn
        {
            template(typename I, typename C = less, typename P = identity)(
                requires bidirectional_iterator<I> AND sortable<I, C, P>)
            void operator()(I first, I middle, I last, iter_difference_t<I> len1,
                            iter_difference_t<I> len2, C pred = C{}, P proj = P{}) const
            {
                merge_adaptive(std::move(first),
                               std::move(middle),
                               std::move(last),
                               len1,
                               len2,
                               static_cast<iter_value_t<I> *>(nullptr),
                               0,
                               std::move(pred),
                               std::move(proj));
            }
        };

        RANGES_INLINE_VARIABLE(inplace_merge_no_buffer_fn, inplace_merge_no_buffer)
    } // namespace detail
    /// \endcond

    /// \addtogroup group-algorithms
    /// @{
    RANGES_FUNC_BEGIN(inplace_merge)

        // TODO reimplement to only need forward iterators

        /// \brief function template \c inplace_merge
        template(typename I, typename S, typename C = less, typename P = identity)(
            requires bidirectional_iterator<I> AND sortable<I, C, P>)
        I RANGES_FUNC(inplace_merge)(
            I first, I middle, S last, C pred = C{}, P proj = P{})
        {
            using value_type = iter_value_t<I>;
            auto len1 = distance(first, middle);
            auto len2_and_end = enumerate(middle, last);
            auto buf_size = ranges::min(len1, len2_and_end.first);
            std::pair<value_type *, std::ptrdiff_t> buf{nullptr, 0};
            std::unique_ptr<value_type, detail::return_temporary_buffer> h;
            if(detail::is_trivially_copy_assignable_v<value_type> && 8 < buf_size)
            {
                buf = detail::get_temporary_buffer<value_type>(buf_size);
                h.reset(buf.first);
            }
            detail::merge_adaptive(std::move(first),
                                   std::move(middle),
                                   len2_and_end.second,
                                   len1,
                                   len2_and_end.first,
                                   buf.first,
                                   buf.second,
                                   std::move(pred),
                                   std::move(proj));
            return len2_and_end.second;
        }

        /// \overload
        template(typename Rng, typename C = less, typename P = identity)(
            requires bidirectional_range<Rng> AND sortable<iterator_t<Rng>, C, P>)
        borrowed_iterator_t<Rng> RANGES_FUNC(inplace_merge)(
            Rng && rng, iterator_t<Rng> middle, C pred = C{}, P proj = P{})
        {
            return (*this)(begin(rng),
                           std::move(middle),
                           end(rng),
                           std::move(pred),
                           std::move(proj));
        }

    RANGES_FUNC_END(inplace_merge)

    namespace cpp20
    {
        using ranges::inplace_merge;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
