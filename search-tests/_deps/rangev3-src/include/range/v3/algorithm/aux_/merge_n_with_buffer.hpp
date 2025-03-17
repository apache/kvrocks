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
// Copyright (c) 2009 Alexander Stepanov and Paul McJones
//
// Permission to use, copy, modify, distribute and sell this software
// and its documentation for any purpose is hereby granted without
// fee, provided that the above copyright notice appear in all copies
// and that both that copyright notice and this permission notice
// appear in supporting documentation. The authors make no
// representations about the suitability of this software for any
// purpose. It is provided "as is" without express or implied
// warranty.
//
// Algorithms from
// Elements of Programming
// by Alexander Stepanov and Paul McJones
// Addison-Wesley Professional, 2009
#ifndef RANGES_V3_ALGORITHM_AUX_MERGE_N_WITH_BUFFER_HPP
#define RANGES_V3_ALGORITHM_AUX_MERGE_N_WITH_BUFFER_HPP

#include <tuple>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/aux_/merge_n.hpp>
#include <range/v3/algorithm/copy_n.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    namespace aux
    {
        struct merge_n_with_buffer_fn
        {
            template(typename I, typename B, typename C = less, typename P = identity)(
                requires same_as<iter_common_reference_t<I>,
                                     iter_common_reference_t<B>> AND
                        indirectly_copyable<I, B> AND mergeable<B, I, I, C, P, P>)
            I operator()(I begin0,
                         iter_difference_t<I> n0,
                         I begin1,
                         iter_difference_t<I> n1,
                         B buff,
                         C r = C{},
                         P p = P{}) const
            {
                copy_n(begin0, n0, buff);
                return merge_n(buff, n0, begin1, n1, begin0, r, p, p).out;
            }
        };

        RANGES_INLINE_VARIABLE(merge_n_with_buffer_fn, merge_n_with_buffer)
    } // namespace aux
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
