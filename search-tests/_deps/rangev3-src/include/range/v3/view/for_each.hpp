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

#ifndef RANGES_V3_VIEW_FOR_EACH_HPP
#define RANGES_V3_VIEW_FOR_EACH_HPP

#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/generate_n.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/repeat_n.hpp>
#include <range/v3/view/single.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{

    namespace views
    {
        /// Lazily applies an unary function to each element in the source
        /// range that returns another range (possibly empty), flattening
        /// the result.
        struct for_each_base_fn
        {
            template(typename Rng, typename Fun)(
                requires viewable_range<Rng> AND transformable_range<Rng, Fun> AND
                    joinable_range<transform_view<all_t<Rng>, Fun>>)
            constexpr auto operator()(Rng && rng, Fun fun) const
            {
                return join(transform(static_cast<Rng &&>(rng), std::move(fun)));
            }
        };

        struct for_each_fn : for_each_base_fn
        {
            using for_each_base_fn::operator();

            template<typename Fun>
            constexpr auto operator()(Fun fun) const
            {
                return make_view_closure(bind_back(for_each_base_fn{}, std::move(fun)));
            }
        };

        /// \relates for_each_fn
        RANGES_INLINE_VARIABLE(for_each_fn, for_each)
    } // namespace views

    struct yield_fn
    {
        template(typename V)(
            requires copy_constructible<V>)
        single_view<V> operator()(V v) const
        {
            return views::single(std::move(v));
        }
    };

    /// \relates yield_fn
    RANGES_INLINE_VARIABLE(yield_fn, yield)

    struct yield_from_fn
    {
        template(typename Rng)(
            requires view_<Rng>)
        Rng operator()(Rng rng) const
        {
            return rng;
        }
    };

    /// \relates yield_from_fn
    RANGES_INLINE_VARIABLE(yield_from_fn, yield_from)

    struct yield_if_fn
    {
        template<typename V>
        repeat_n_view<V> operator()(bool b, V v) const
        {
            return views::repeat_n(std::move(v), b ? 1 : 0);
        }
    };

    /// \relates yield_if_fn
    RANGES_INLINE_VARIABLE(yield_if_fn, yield_if)

    struct lazy_yield_if_fn
    {
        template(typename F)(
            requires invocable<F &>)
        generate_n_view<F> operator()(bool b, F f) const
        {
            return views::generate_n(std::move(f), b ? 1 : 0);
        }
    };

    /// \relates lazy_yield_if_fn
    RANGES_INLINE_VARIABLE(lazy_yield_if_fn, lazy_yield_if)
    /// @}

    /// \cond
    template(typename Rng, typename Fun)(
        requires viewable_range<Rng> AND views::transformable_range<Rng, Fun> AND
            input_range<invoke_result_t<Fun &, range_reference_t<Rng>>>)
        auto
        operator>>=(Rng && rng, Fun fun)
    {
        return views::for_each(static_cast<Rng &&>(rng), std::move(fun));
    }
    /// \endcond
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
