
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
#ifndef RANGES_V3_FUNCTIONAL_BIND_HPP
#define RANGES_V3_FUNCTIONAL_BIND_HPP

#include <functional>
#include <type_traits>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-functional
    /// @{
    template<typename T,
             typename U = meta::if_<
                 std::is_lvalue_reference<T>,
                 std::reference_wrapper<meta::_t<std::remove_reference<T>>>, T &&>>
    U bind_forward(meta::_t<std::remove_reference<T>> & t) noexcept
    {
        return static_cast<U>(t);
    }

    template<typename T>
    T && bind_forward(meta::_t<std::remove_reference<T>> && t) noexcept
    {
        // This is to catch way sketchy stuff like: forward<int const &>(42)
        static_assert(!std::is_lvalue_reference<T>::value, "You didn't just do that!");
        return static_cast<T &&>(t);
    }

    template<typename T>
    struct bind_element
      : meta::if_c<RANGES_IS_SAME(detail::decay_t<T>, T), meta::id<T>,
                   bind_element<detail::decay_t<T>>>
    {};

    template<typename T>
    struct bind_element<std::reference_wrapper<T>>
    {
        using type = T &;
    };

    template<typename T>
    struct bind_element<reference_wrapper<T>>
    {
        using type = typename reference_wrapper<T>::reference;
    };

    template<typename T>
    using bind_element_t = meta::_t<bind_element<T>>;

    template<typename Bind>
    struct protector
    {
    private:
        Bind bind_;

    public:
        protector() = default;
        protector(Bind b)
          : bind_(std::move(b))
        {}
        // clang-format off
        template<typename...Ts>
        auto CPP_auto_fun(operator())(Ts &&...ts)
        (
            return bind_(static_cast<Ts &&>(ts)...)
        )
        /// \overload
        template<typename...Ts>
        auto CPP_auto_fun(operator())(Ts &&...ts) (const)
        (
            return bind_(static_cast<Ts &&>(ts)...)
        )
        // clang-format on
    };

    struct protect_fn
    {
        template(typename F)(
            requires std::is_bind_expression<uncvref_t<F>>::value) //
        protector<uncvref_t<F>> operator()(F && f) const
        {
            return {static_cast<F &&>(f)};
        }
        /// \overload
        template(typename F)(
            requires (!std::is_bind_expression<uncvref_t<F>>::value)) //
        F operator()(F && f) const
        {
            return static_cast<F &&>(f);
        }
    };

    /// Protect a callable so that it can be safely used in a bind expression without
    /// accidentally becoming a "nested" bind.
    /// \ingroup group-functional
    /// \sa `protect_fn`
    RANGES_INLINE_VARIABLE(protect_fn, protect)
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
