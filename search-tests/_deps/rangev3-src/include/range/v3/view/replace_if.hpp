/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_VIEW_REPLACE_IF_HPP
#define RANGES_V3_VIEW_REPLACE_IF_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/utility/compressed_pair.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/transform.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename Pred, typename Val>
        struct replacer_if_fn : compressed_pair<semiregular_box_t<Pred>, Val>
        {
        private:
            using base_t = compressed_pair<semiregular_box_t<Pred>, Val>;
            using base_t::first;
            using base_t::second;

        public:
            replacer_if_fn() = default;
            constexpr replacer_if_fn(Pred pred, Val new_value)
              : base_t{std::move(pred), std::move(new_value)}
            {}

            template<typename I>
            [[noreturn]] common_type_t<decay_t<unwrap_reference_t<Val const &>>,
                                       iter_value_t<I>> &
            operator()(copy_tag, I const &) const
            {
                RANGES_EXPECT(false);
            }

            template(typename I)(
                requires (!invocable<Pred const &, iter_reference_t<I>>))
            common_reference_t<unwrap_reference_t<Val const &>, iter_reference_t<I>> //
            operator()(I const & i)
            {
                auto && x = *i;
                if(invoke(first(), (decltype(x) &&)x)) //
                    return unwrap_reference(second());
                return (decltype(x) &&)x;
            }
            template(typename I)(
                requires invocable<Pred const &, iter_reference_t<I>>)
            common_reference_t<unwrap_reference_t<Val const &>, iter_reference_t<I>> //
            operator()(I const & i) const
            {
                auto && x = *i;
                if(invoke(first(), (decltype(x) &&)x)) //
                    return unwrap_reference(second());
                return (decltype(x) &&)x;
            }

            template(typename I)(
                requires (!invocable<Pred const &, iter_rvalue_reference_t<I>>))
            common_reference_t<
                unwrap_reference_t<Val const &>, //
                iter_rvalue_reference_t<I>> //
            operator()(move_tag, I const & i)
            {
                auto && x = iter_move(i);
                if(invoke(first(), (decltype(x) &&)x)) //
                    return unwrap_reference(second());
                return (decltype(x) &&)x;
            }
            template(typename I)(
                requires invocable<Pred const &, iter_rvalue_reference_t<I>>)
            common_reference_t< //
                unwrap_reference_t<Val const &>, //
                iter_rvalue_reference_t<I>> //
            operator()(move_tag, I const & i) const
            {
                auto && x = iter_move(i);
                if(invoke(first(), (decltype(x) &&)x)) //
                    return unwrap_reference(second());
                return (decltype(x) &&)x;
            }
        };
    } // namespace detail
    /// \endcond

    /// \addtogroup group-views
    /// @{
    namespace views
    {
        struct replace_if_base_fn
        {
            template(typename Rng, typename Pred, typename Val)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    indirect_unary_predicate<Pred, iterator_t<Rng>> AND
                    common_with<detail::decay_t<unwrap_reference_t<Val const &>>,
                                range_value_t<Rng>> AND
                    common_reference_with<unwrap_reference_t<Val const &>,
                                          range_reference_t<Rng>> AND
                    common_reference_with<unwrap_reference_t<Val const &>,
                                          range_rvalue_reference_t<Rng>>)
            constexpr replace_if_view<all_t<Rng>, Pred, Val> //
            operator()(Rng && rng, Pred pred, Val new_value) const
            {
                return {all(static_cast<Rng &&>(rng)),
                        {std::move(pred), std::move(new_value)}};
            }
        };

        struct replace_if_fn : replace_if_base_fn
        {
            using replace_if_base_fn::operator();

            template<typename Pred, typename Val>
            constexpr auto operator()(Pred pred, Val new_value) const
            {
                return make_view_closure(bind_back(
                    replace_if_base_fn{}, std::move(pred), std::move(new_value)));
            }
        };

        /// \relates replace_if_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(replace_if_fn, replace_if)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
