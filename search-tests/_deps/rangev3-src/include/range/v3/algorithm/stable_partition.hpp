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
//===-------------------------- algorithm ---------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
#ifndef RANGES_V3_ALGORITHM_STABLE_PARTITION_HPP
#define RANGES_V3_ALGORITHM_STABLE_PARTITION_HPP

#include <functional>
#include <memory>
#include <type_traits>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/move.hpp>
#include <range/v3/algorithm/partition_copy.hpp>
#include <range/v3/algorithm/rotate.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/move_iterators.hpp>
#include <range/v3/iterator/operations.hpp>
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
    /// \addtogroup group-algorithms
    /// @{

    /// \cond
    namespace detail
    {
        template<typename I, typename C, typename P, typename D, typename Pair>
        I stable_partition_impl(I first, I last, C pred, P proj, D len, Pair const p,
                                std::forward_iterator_tag fi)
        {
            // *first is known to be false
            // len >= 1
            if(len == 1)
                return first;
            if(len == 2)
            {
                I tmp = first;
                if(invoke(pred, invoke(proj, *++tmp)))
                {
                    ranges::iter_swap(first, tmp);
                    return tmp;
                }
                return first;
            }
            if(len <= p.second)
            { // The buffer is big enough to use
                // Move the falses into the temporary buffer, and the trues to the front
                // of the line Update first to always point to the last of the trues
                auto tmpbuf = make_raw_buffer(p.first);
                auto buf = tmpbuf.begin();
                *buf = iter_move(first);
                ++buf;
                auto res = partition_copy(make_move_iterator(next(first)),
                                          make_move_sentinel(last),
                                          first,
                                          buf,
                                          std::ref(pred),
                                          std::ref(proj));
                // All trues now at start of range, all falses in buffer
                // Move falses back into range, but don't mess up first which points to
                // first false
                ranges::move(p.first, res.out2.base().base(), res.out1);
                // h destructs moved-from values out of the temp buffer, but doesn't
                // deallocate buffer
                return res.out1;
            }
            // Else not enough buffer, do in place
            // len >= 3
            D half = len / 2; // half >= 2
            I middle = next(first, half);
            // recurse on [first, middle), *first know to be false
            // F?????????????????
            // f       m         l
            I begin_false =
                detail::stable_partition_impl(first, middle, pred, proj, half, p, fi);
            // TTTFFFFF??????????
            // f  ff   m         l
            // recurse on [middle, last], except increase middle until *(middle) is false,
            // *last know to be true
            I m1 = middle;
            D len_half = len - half;
            while(invoke(pred, invoke(proj, *m1)))
            {
                if(++m1 == last)
                    return ranges::rotate(begin_false, middle, last).begin();
                --len_half;
            }
            // TTTFFFFFTTTF??????
            // f  ff   m  m1     l
            I end_false =
                detail::stable_partition_impl(m1, last, pred, proj, len_half, p, fi);
            // TTTFFFFFTTTTTFFFFF
            // f  ff   m    sf   l
            return ranges::rotate(begin_false, middle, end_false).begin();
            // TTTTTTTTFFFFFFFFFF
            //         |
        }

        template<typename I, typename S, typename C, typename P>
        I stable_partition_impl(I first, S last, C pred, P proj,
                                std::forward_iterator_tag fi)
        {
            using difference_type = iter_difference_t<I>;
            difference_type const alloc_limit = 3; // might want to make this a function
                                                   // of trivial assignment.
            // Either prove all true and return first or point to first false
            while(true)
            {
                if(first == last)
                    return first;
                if(!invoke(pred, invoke(proj, *first)))
                    break;
                ++first;
            }
            // We now have a reduced range [first, last)
            // *first is known to be false
            using value_type = iter_value_t<I>;
            auto len_end = enumerate(first, last);
            auto p = len_end.first >= alloc_limit
                         ? detail::get_temporary_buffer<value_type>(len_end.first)
                         : detail::value_init{};
            std::unique_ptr<value_type, detail::return_temporary_buffer> const h{p.first};
            return detail::stable_partition_impl(
                first, len_end.second, pred, proj, len_end.first, p, fi);
        }

        template<typename I, typename C, typename P, typename D, typename Pair>
        I stable_partition_impl(I first, I last, C pred, P proj, D len, Pair p,
                                std::bidirectional_iterator_tag bi)
        {
            // *first is known to be false
            // *last is known to be true
            // len >= 2
            if(len == 2)
            {
                ranges::iter_swap(first, last);
                return last;
            }
            if(len == 3)
            {
                I tmp = first;
                if(invoke(pred, invoke(proj, *++tmp)))
                {
                    ranges::iter_swap(first, tmp);
                    ranges::iter_swap(tmp, last);
                    return last;
                }
                ranges::iter_swap(tmp, last);
                ranges::iter_swap(first, tmp);
                return tmp;
            }
            if(len <= p.second)
            { // The buffer is big enough to use
                // Move the falses into the temporary buffer, and the trues to the front
                // of the line Update first to always point to the last of the trues
                auto tmpbuf = ranges::make_raw_buffer(p.first);
                auto buf = tmpbuf.begin();
                *buf = iter_move(first);
                ++buf;
                auto res = partition_copy(make_move_iterator(next(first)),
                                          make_move_sentinel(last),
                                          first,
                                          buf,
                                          std::ref(pred),
                                          std::ref(proj));
                first = res.out1;
                // move *last, known to be true
                *first = iter_move(res.in);
                ++first;
                // All trues now at start of range, all falses in buffer
                // Move falses back into range, but don't mess up first which points to
                // first false
                ranges::move(p.first, res.out2.base().base(), first);
                // h destructs moved-from values out of the temp buffer, but doesn't
                // deallocate buffer
                return first;
            }
            // Else not enough buffer, do in place
            // len >= 4
            I middle = first;
            D half = len / 2; // half >= 2
            advance(middle, half);
            // recurse on [first, middle-1], except reduce middle-1 until *(middle-1) is
            // true, *first know to be false F????????????????T f       m        l
            I m1 = middle;
            I begin_false = first;
            D len_half = half;
            while(!invoke(pred, invoke(proj, *--m1)))
            {
                if(m1 == first)
                    goto first_half_done;
                --len_half;
            }
            // F???TFFF?????????T
            // f   m1  m        l
            begin_false =
                detail::stable_partition_impl(first, m1, pred, proj, len_half, p, bi);
        first_half_done:
            // TTTFFFFF?????????T
            // f  ff   m        l
            // recurse on [middle, last], except increase middle until *(middle) is false,
            // *last know to be true
            m1 = middle;
            len_half = len - half;
            while(invoke(pred, invoke(proj, *m1)))
            {
                if(++m1 == last)
                    return ranges::rotate(begin_false, middle, ++last).begin();
                --len_half;
            }
            // TTTFFFFFTTTF?????T
            // f  ff   m  m1    l
            I end_false =
                detail::stable_partition_impl(m1, last, pred, proj, len_half, p, bi);
            // TTTFFFFFTTTTTFFFFF
            // f  ff   m    sf  l
            return ranges::rotate(begin_false, middle, end_false).begin();
            // TTTTTTTTFFFFFFFFFF
            //         |
        }

        template<typename I, typename S, typename C, typename P>
        I stable_partition_impl(I first, S end_, C pred, P proj,
                                std::bidirectional_iterator_tag bi)
        {
            using difference_type = iter_difference_t<I>;
            using value_type = iter_value_t<I>;
            difference_type const alloc_limit =
                4; // might want to make this a function of trivial assignment
            // Either prove all true and return first or point to first false
            while(true)
            {
                if(first == end_)
                    return first;
                if(!invoke(pred, invoke(proj, *first)))
                    break;
                ++first;
            }
            // first points to first false, everything prior to first is already set.
            // Either prove [first, last) is all false and return first, or point last to
            // last true
            I last = ranges::next(first, end_);
            do
            {
                if(first == --last)
                    return first;
            } while(!invoke(pred, invoke(proj, *last)));
            // We now have a reduced range [first, last]
            // *first is known to be false
            // *last is known to be true
            // len >= 2
            auto len = distance(first, last) + 1;
            auto p = len >= alloc_limit ? detail::get_temporary_buffer<value_type>(len)
                                        : detail::value_init{};
            std::unique_ptr<value_type, detail::return_temporary_buffer> const h{p.first};
            return detail::stable_partition_impl(first, last, pred, proj, len, p, bi);
        }
    } // namespace detail
    /// \endcond

    RANGES_FUNC_BEGIN(stable_partition)

        /// \brief function template \c stable_partition
        template(typename I, typename S, typename C, typename P = identity)(
            requires bidirectional_iterator<I> AND sentinel_for<S, I> AND
            indirect_unary_predicate<C, projected<I, P>> AND permutable<I>)
        I RANGES_FUNC(stable_partition)(I first, S last, C pred, P proj = P{})
        {
            return detail::stable_partition_impl(std::move(first),
                                                 std::move(last),
                                                 std::ref(pred),
                                                 std::ref(proj),
                                                 iterator_tag_of<I>());
        }

        // BUGBUG Can this be optimized if Rng has O1 size?
        /// \overload
        template(typename Rng, typename C, typename P = identity)(
            requires bidirectional_range<Rng> AND
            indirect_unary_predicate<C, projected<iterator_t<Rng>, P>> AND
            permutable<iterator_t<Rng>>)
        borrowed_iterator_t<Rng> //
        RANGES_FUNC(stable_partition)(Rng && rng, C pred, P proj = P{}) //
        {
            return (*this)(begin(rng), end(rng), std::move(pred), std::move(proj));
        }

    RANGES_FUNC_END(stable_partition)

    namespace cpp20
    {
        using ranges::stable_partition;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
