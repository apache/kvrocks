/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_FUNCTIONAL_INVOKE_HPP
#define RANGES_V3_FUNCTIONAL_INVOKE_HPP

#include <functional>
#include <type_traits>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_CXX17_COMPAT
RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS

#ifndef RANGES_CONSTEXPR_INVOKE
#ifdef RANGES_WORKAROUND_CLANG_23135
#define RANGES_CONSTEXPR_INVOKE 0
#else
#define RANGES_CONSTEXPR_INVOKE 1
#endif
#endif

namespace ranges
{
    /// \addtogroup group-functional
    /// @{

    /// \cond
    namespace detail
    {
        RANGES_DIAGNOSTIC_PUSH
        RANGES_DIAGNOSTIC_IGNORE_VOID_PTR_DEREFERENCE

        template<typename U>
        U & can_reference_(U &&);

        // clang-format off
        /// \concept dereferenceable_part_
        /// \brief The \c dereferenceable_part_ concept
        template<typename T>
        CPP_requires(dereferenceable_part_,
            requires(T && t) //
            (
                detail::can_reference_(*(T &&) t)
            ));
        /// \concept dereferenceable_
        /// \brief The \c dereferenceable_ concept
        template<typename T>
        CPP_concept dereferenceable_ = //
            CPP_requires_ref(detail::dereferenceable_part_, T);
        // clang-format on

        RANGES_DIAGNOSTIC_POP

        template<typename T>
        RANGES_INLINE_VAR constexpr bool is_reference_wrapper_v =
            meta::is<T, reference_wrapper>::value ||
            meta::is<T, std::reference_wrapper>::value;
    } // namespace detail
    /// \endcond

    template<typename T>
    RANGES_INLINE_VAR constexpr bool is_reference_wrapper_v =
        detail::is_reference_wrapper_v<detail::decay_t<T>>;

    template<typename T>
    using is_reference_wrapper = meta::bool_<is_reference_wrapper_v<T>>;

    /// \cond
    template<typename T>
    using is_reference_wrapper_t RANGES_DEPRECATED(
        "is_reference_wrapper_t is deprecated.") = meta::_t<is_reference_wrapper<T>>;
    /// \endcond

    struct invoke_fn
    {
    private:
        template(typename, typename T1)(
            requires detail::dereferenceable_<T1>)
        static constexpr decltype(auto) coerce(T1 && t1, long)
            noexcept(noexcept(*static_cast<T1 &&>(t1)))
        {
            return *static_cast<T1 &&>(t1);
        }

        template(typename T, typename T1)(
            requires derived_from<detail::decay_t<T1>, T>)
        static constexpr T1 && coerce(T1 && t1, int) noexcept
        {
            return static_cast<T1 &&>(t1);
        }

        template(typename, typename T1)(
            requires detail::is_reference_wrapper_v<detail::decay_t<T1>>)
        static constexpr decltype(auto) coerce(T1 && t1, int) noexcept
        {
            return static_cast<T1 &&>(t1).get();
        }

    public:
        template<typename F, typename T, typename T1, typename... Args>
        constexpr auto operator()(F T::*f, T1&& t1, Args&&... args) const
            noexcept(noexcept((invoke_fn::coerce<T>((T1&&) t1, 0).*f)((Args&&) args...)))
            -> decltype((invoke_fn::coerce<T>((T1&&) t1, 0).*f)((Args&&) args...))
        {
            return (invoke_fn::coerce<T>((T1&&) t1, 0).*f)((Args&&) args...);
        }

        template<typename D, typename T, typename T1>
        constexpr auto operator()(D T::*f, T1&& t1) const
            noexcept(noexcept(invoke_fn::coerce<T>((T1&&) t1, 0).*f))
            -> decltype(invoke_fn::coerce<T>((T1&&) t1, 0).*f)
        {
            return invoke_fn::coerce<T>((T1&&) t1, 0).*f;
        }

        template<typename F, typename... Args>
        CPP_PP_IIF(RANGES_CONSTEXPR_INVOKE)(CPP_PP_EXPAND, CPP_PP_EAT)(constexpr)
        auto operator()(F&& f, Args&&... args) const
            noexcept(noexcept(((F&&) f)((Args&&) args...)))
            -> decltype(((F&&) f)((Args&&) args...))
        {
            return ((F&&) f)((Args&&) args...);
        }
    };

    RANGES_INLINE_VARIABLE(invoke_fn, invoke)

#ifdef RANGES_WORKAROUND_MSVC_701385
    /// \cond
    namespace detail
    {
        template<typename Void, typename Fun, typename... Args>
        struct _invoke_result_
        {};

        template<typename Fun, typename... Args>
        struct _invoke_result_<
            meta::void_<decltype(invoke(std::declval<Fun>(), std::declval<Args>()...))>,
            Fun, Args...>
        {
            using type = decltype(invoke(std::declval<Fun>(), std::declval<Args>()...));
        };
    } // namespace detail
    /// \endcond

    template<typename Fun, typename... Args>
    using invoke_result = detail::_invoke_result_<void, Fun, Args...>;

    template<typename Fun, typename... Args>
    using invoke_result_t = meta::_t<invoke_result<Fun, Args...>>;

#else  // RANGES_WORKAROUND_MSVC_701385
    template<typename Fun, typename... Args>
    using invoke_result_t =
        decltype(invoke(std::declval<Fun>(), std::declval<Args>()...));

    template<typename Fun, typename... Args>
    struct invoke_result : meta::defer<invoke_result_t, Fun, Args...>
    {};
#endif // RANGES_WORKAROUND_MSVC_701385

    /// \cond
    namespace detail
    {
        template<bool IsInvocable>
        struct is_nothrow_invocable_impl_
        {
            template<typename Fn, typename... Args>
            static constexpr bool apply() noexcept
            {
                return false;
            }
        };
        template<>
        struct is_nothrow_invocable_impl_<true>
        {
            template<typename Fn, typename... Args>
            static constexpr bool apply() noexcept
            {
                return noexcept(invoke(std::declval<Fn>(), std::declval<Args>()...));
            }
        };
    } // namespace detail
    /// \endcond

    template<typename Fn, typename... Args>
    RANGES_INLINE_VAR constexpr bool is_invocable_v =
        meta::is_trait<invoke_result<Fn, Args...>>::value;

    template<typename Fn, typename... Args>
    RANGES_INLINE_VAR constexpr bool is_nothrow_invocable_v =
        detail::is_nothrow_invocable_impl_<is_invocable_v<Fn, Args...>>::template apply<
            Fn, Args...>();

    /// \cond
    template<typename Sig>
    struct RANGES_DEPRECATED(
        "ranges::result_of is deprecated. "
        "Please use ranges::invoke_result") result_of
    {};

    template<typename Fun, typename... Args>
    struct RANGES_DEPRECATED(
        "ranges::result_of is deprecated. "
        "Please use ranges::invoke_result") result_of<Fun(Args...)>
      : meta::defer<invoke_result_t, Fun, Args...>
    {};
    /// \endcond

    namespace cpp20
    {
        using ranges::invoke;
        using ranges::invoke_result;
        using ranges::invoke_result_t;
        using ranges::is_invocable_v;
        using ranges::is_nothrow_invocable_v;
    } // namespace cpp20

    /// @}
} // namespace ranges

RANGES_DIAGNOSTIC_POP

#include <range/v3/detail/epilogue.hpp>

#endif // RANGES_V3_FUNCTIONAL_INVOKE_HPP
