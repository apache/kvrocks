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

#ifndef RANGES_V3_VIEW_DELIMIT_HPP
#define RANGES_V3_VIEW_DELIMIT_HPP

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/bind_back.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/unreachable_sentinel.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/adaptor.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/subrange.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename Rng, typename Val>
    struct delimit_view
      : view_adaptor<delimit_view<Rng, Val>, Rng,
                     is_finite<Rng>::value ? finite : unknown>
    {
    private:
        friend range_access;
        Val value_;

        struct sentinel_adaptor : adaptor_base
        {
            sentinel_adaptor() = default;
            sentinel_adaptor(Val value)
              : value_(std::move(value))
            {}
            template<class I, class S>
            bool empty(I const & it, S const & last) const
            {
                return it == last || *it == value_;
            }
            Val value_;
        };

        sentinel_adaptor end_adaptor() const
        {
            return {value_};
        }

    public:
        delimit_view() = default;
        constexpr delimit_view(Rng rng, Val value)
          : delimit_view::view_adaptor{std::move(rng)}
          , value_(std::move(value))
        {}
    };

    // the begin iterator will be an iterator into the underlying view (conditionally
    // borrowed) and the end iterator owns the value to be compared against (borrowed)
    template<typename Rng, typename Val>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<delimit_view<Rng, Val>> = //
        enable_borrowed_range<Rng>;

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template(typename Rng, typename Val)(
        requires copy_constructible<Val>)
    delimit_view(Rng &&, Val)
        -> delimit_view<views::all_t<Rng>, Val>;
#endif

    namespace views
    {
        struct delimit_base_fn
        {
            template(typename I_, typename Val, typename I = detail::decay_t<I_>)(
                requires (!range<I_>) AND convertible_to<I_, I> AND input_iterator<I> AND
                    semiregular<Val> AND
                    equality_comparable_with<Val, iter_reference_t<I>>)
            constexpr auto operator()(I_ && begin_, Val value) const
                -> delimit_view<subrange<I, unreachable_sentinel_t>, Val>
            {
                return {{static_cast<I_ &&>(begin_), {}}, std::move(value)};
            }

            template(typename Rng, typename Val)(
                requires viewable_range<Rng> AND input_range<Rng> AND semiregular<
                        Val> AND equality_comparable_with<Val, range_reference_t<Rng>>)
            constexpr auto operator()(Rng && rng, Val value) const //
                -> delimit_view<all_t<Rng>, Val>
            {
                return {all(static_cast<Rng &&>(rng)), std::move(value)};
            }
        };

        struct delimit_fn : delimit_base_fn
        {
            using delimit_base_fn::operator();

            template<typename Val>
            constexpr auto operator()(Val value) const
            {
                return make_view_closure(bind_back(delimit_base_fn{}, std::move(value)));
            }
        };

        /// \relates delimit_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(delimit_fn, delimit)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::delimit_view)

#endif
