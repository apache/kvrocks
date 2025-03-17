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
#ifndef RANGES_V3_FUNCTIONAL_NOT_FN_HPP
#define RANGES_V3_FUNCTIONAL_NOT_FN_HPP

#include <type_traits>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/concepts.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-functional
    /// @{
    template<typename FD>
    struct logical_negate
    {
    private:
        CPP_assert(same_as<FD, detail::decay_t<FD>> && move_constructible<FD>);
        RANGES_NO_UNIQUE_ADDRESS FD pred_;

    public:
        CPP_member
        constexpr CPP_ctor(logical_negate)()(                          //
            noexcept(std::is_nothrow_default_constructible<FD>::value) //
                requires default_constructible<FD>)
        {}
        template(typename T)(
            requires (!same_as<detail::decay_t<T>, logical_negate>) AND
                constructible_from<FD, T>)
        constexpr explicit logical_negate(T && pred)
          : pred_(static_cast<T &&>(pred))
        {}

        template(typename... Args)(
            requires predicate<FD &, Args...>)
        constexpr bool operator()(Args &&... args) &
        {
            return !invoke(pred_, static_cast<Args &&>(args)...);
        }
        /// \overload
        template(typename... Args)(
            requires predicate<FD const &, Args...>)
        constexpr bool operator()(Args &&... args) const &
        {
            return !invoke(pred_, static_cast<Args &&>(args)...);
        }
        /// \overload
        template(typename... Args)(
            requires predicate<FD, Args...>)
        constexpr bool operator()(Args &&... args) &&
        {
            return !invoke(static_cast<FD &&>(pred_), static_cast<Args &&>(args)...);
        }
    };

    struct not_fn_fn
    {
        template(typename Pred)(
            requires move_constructible<detail::decay_t<Pred>> AND
                constructible_from<detail::decay_t<Pred>, Pred>)
        constexpr logical_negate<detail::decay_t<Pred>> operator()(Pred && pred) const
        {
            return logical_negate<detail::decay_t<Pred>>{(Pred &&) pred};
        }
    };

    /// \ingroup group-functional
    /// \sa `not_fn_fn`
    RANGES_INLINE_VARIABLE(not_fn_fn, not_fn)

    namespace cpp20
    {
        using ranges::not_fn;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
