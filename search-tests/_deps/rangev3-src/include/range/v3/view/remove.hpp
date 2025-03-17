/// \file
// Range v3 library
//
//  Copyright Andrey Diduh 2019
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_VIEW_REMOVE_HPP
#define RANGES_V3_VIEW_REMOVE_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/comparisons.hpp>
#include <range/v3/view/remove_if.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    namespace views
    {
        struct remove_base_fn
        {
        private:
            template<typename Value>
            struct pred_
            {
                Value value_;
                template(typename T)(
                    requires equality_comparable_with<T, Value const &>)
                bool operator()(T && other) const
                {
                    return static_cast<T &&>(other) == value_;
                }
            };

        public:
            template(typename Rng, typename Value)(
                requires move_constructible<Value> AND viewable_range<Rng> AND
                    input_range<Rng> AND
                    indirectly_comparable<iterator_t<Rng>, Value const *, equal_to>)
            constexpr auto operator()(Rng && rng, Value value) const
            {
                return remove_if(static_cast<Rng &&>(rng),
                                 pred_<Value>{std::move(value)});
            }

            template(typename Rng, typename Value, typename Proj)(
                requires move_constructible<Value> AND viewable_range<Rng> AND
                    input_range<Rng> AND
                    indirectly_comparable<iterator_t<Rng>, Value const *, equal_to, Proj>)
            constexpr auto operator()(Rng && rng, Value value, Proj proj) const
            {
                return remove_if(static_cast<Rng &&>(rng),
                                 pred_<Value>{std::move(value)},
                                 std::move(proj));
            }
        };

        struct remove_bind_fn
        {
            template<typename Value>
            constexpr auto operator()(Value value) const // TODO: underconstrained
            {
                return make_view_closure(bind_back(remove_base_fn{}, std::move(value)));
            }
            template(typename Value, typename Proj)(
                requires (!range<Value>)) // TODO: underconstrained
            constexpr auto operator()(Value && value, Proj proj) const
            {
                return make_view_closure(bind_back(
                    remove_base_fn{}, static_cast<Value &&>(value), std::move(proj)));
            }
        };

        struct RANGES_EMPTY_BASES remove_fn
          : remove_base_fn, remove_bind_fn
        {
            using remove_base_fn::operator();
            using remove_bind_fn::operator();
        };

        /// \relates remove_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(remove_fn, remove)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif // RANGES_V3_VIEW_REMOVE_HPP
