/// \file
// Concepts library
//
//  Copyright Eric Niebler 2013-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#ifndef CPP_TYPE_TRAITS_HPP
#define CPP_TYPE_TRAITS_HPP

#include <tuple>
#include <utility>
#include <type_traits>
#include <meta/meta.hpp>

namespace concepts
{
    template<typename T>
    using remove_cvref_t =
        typename std::remove_cv<
            typename std::remove_reference<T>::type>::type;

    /// \cond
    namespace detail
    {
        template<typename From, typename To>
        using is_convertible = std::is_convertible<meta::_t<std::add_rvalue_reference<From>>, To>;

        template<bool>
        struct if_else_
        {
            template<typename, typename U>
            using invoke = U;
        };
        template<>
        struct if_else_<true>
        {
            template<typename T, typename>
            using invoke = T;
        };
        template<bool B, typename T, typename U>
        using if_else_t = meta::invoke<if_else_<B>, T, U>;

        template<bool>
        struct if_
        {};
        template<>
        struct if_<true>
        {
            template<typename T>
            using invoke = T;
        };
        template<bool B, typename T = void>
        using if_t = meta::invoke<if_<B>, T>;

        template<typename From, typename To>
        struct _copy_cv_
        {
            using type = To;
        };
        template<typename From, typename To>
        struct _copy_cv_<From const, To>
        {
            using type = To const;
        };
        template<typename From, typename To>
        struct _copy_cv_<From volatile, To>
        {
            using type = To volatile;
        };
        template<typename From, typename To>
        struct _copy_cv_<From const volatile, To>
        {
            using type = To const volatile;
        };
        template<typename From, typename To>
        using _copy_cv = meta::_t<_copy_cv_<From, To>>;

        ////////////////////////////////////////////////////////////////////////////////////////
        template<typename T, typename U, typename = void>
        struct _builtin_common;

        template<typename T, typename U>
        using _builtin_common_t = meta::_t<_builtin_common<T, U>>;

        template<typename T, typename U>
        using _cond_res = decltype(true ? std::declval<T>() : std::declval<U>());

        template<typename T, typename U, typename R = _builtin_common_t<T &, U &>>
        using _rref_res =
            if_else_t<std::is_reference<R>::value, meta::_t<std::remove_reference<R>> &&, R>;

        template<typename T, typename U>
        using _lref_res = _cond_res<_copy_cv<T, U> &, _copy_cv<U, T> &>;

        template<typename T>
        struct as_cref_
        {
            using type = T const &;
        };
        template<typename T>
        struct as_cref_<T &>
        {
            using type = T const &;
        };
        template<typename T>
        struct as_cref_<T &&>
        {
            using type = T const &;
        };
        template<>
        struct as_cref_<void>
        {
            using type = void;
        };
        template<>
        struct as_cref_<void const>
        {
            using type = void const;
        };

        template<typename T>
        using as_cref_t = typename as_cref_<T>::type;

        template<typename T>
        using decay_t = typename std::decay<T>::type;

    #if !defined(__GNUC__) || defined(__clang__)
        template<typename T, typename U, typename = void>
        struct _builtin_common_3
        {};
        template<typename T, typename U>
        struct _builtin_common_3<T, U, meta::void_<_cond_res<as_cref_t<T>, as_cref_t<U>>>>
            : std::decay<_cond_res<as_cref_t<T>, as_cref_t<U>>>
        {};
        template<typename T, typename U, typename = void>
        struct _builtin_common_2
            : _builtin_common_3<T, U>
        {};
        template<typename T, typename U>
        struct _builtin_common_2<T, U, meta::void_<_cond_res<T, U>>>
            : std::decay<_cond_res<T, U>>
        {};
        template<typename T, typename U, typename /* = void */>
        struct _builtin_common
            : _builtin_common_2<T, U>
        {};
        template<typename T, typename U>
        struct _builtin_common<T &&, U &&, if_t<
            is_convertible<T &&, _rref_res<T, U>>::value &&
            is_convertible<U &&, _rref_res<T, U>>::value>>
        {
            using type = _rref_res<T, U>;
        };
        template<typename T, typename U>
        struct _builtin_common<T &, U &>
            : meta::defer<_lref_res, T, U>
        {};
        template<typename T, typename U>
        struct _builtin_common<T &, U &&, if_t<
            is_convertible<U &&, _builtin_common_t<T &, U const &>>::value>>
            : _builtin_common<T &, U const &>
        {};
        template<typename T, typename U>
        struct _builtin_common<T &&, U &>
            : _builtin_common<U &, T &&>
        {};
    #else
        template<typename T, typename U, typename = void>
        struct _builtin_common_3
        {};
        template<typename T, typename U>
        struct _builtin_common_3<T, U, meta::void_<_cond_res<as_cref_t<T>, as_cref_t<U>>>>
            : std::decay<_cond_res<as_cref_t<T>, as_cref_t<U>>>
        {};
        template<typename T, typename U, typename = void>
        struct _builtin_common_2
            : _builtin_common_3<T, U>
        {};
        template<typename T, typename U>
        struct _builtin_common_2<T, U, meta::void_<_cond_res<T, U>>>
            : std::decay<_cond_res<T, U>>
        {};
        template<typename T, typename U, typename /* = void */>
        struct _builtin_common
            : _builtin_common_2<T, U>
        {};
        template<typename T, typename U, typename = void>
        struct _builtin_common_rr
            : _builtin_common_2<T &&, U &&>
        {};
        template<typename T, typename U>
        struct _builtin_common_rr<T, U, if_t<
            is_convertible<T &&, _rref_res<T, U>>::value &&
            is_convertible<U &&, _rref_res<T, U>>::value>>
        {
            using type = _rref_res<T, U>;
        };
        template<typename T, typename U>
        struct _builtin_common<T &&, U &&>
            : _builtin_common_rr<T, U>
        {};
        template<typename T, typename U>
        struct _builtin_common<T &, U &>
            : meta::defer<_lref_res, T, U>
        {};
        template<typename T, typename U, typename = void>
        struct _builtin_common_lr
            : _builtin_common_2<T &, T &&>
        {};
        template<typename T, typename U>
        struct _builtin_common_lr<T, U, if_t<
            is_convertible<U &&, _builtin_common_t<T &, U const &>>::value>>
            : _builtin_common<T &, U const &>
        {};
        template<typename T, typename U>
        struct _builtin_common<T &, U &&>
            : _builtin_common_lr<T, U>
        {};
        template<typename T, typename U>
        struct _builtin_common<T &&, U &>
            : _builtin_common<U &, T &&>
        {};
    #endif
    }
    /// \endcond

    /// \addtogroup group-utility Utility
    /// @{
    ///

    /// Users should specialize this to hook the \c common_with concept
    /// until \c std gets a SFINAE-friendly \c std::common_type and there's
    /// some sane way to deal with cv and ref qualifiers.
    template<typename ...Ts>
    struct common_type
    {};

    template<typename T>
    struct common_type<T>
        : std::decay<T>
    {};

    template<typename T, typename U>
    struct common_type<T, U>
        : detail::if_else_t<
            (META_IS_SAME(detail::decay_t<T>, T) &&
                META_IS_SAME(detail::decay_t<U>, U) ),
            meta::defer<detail::_builtin_common_t, T, U>,
            common_type<detail::decay_t<T>, detail::decay_t<U>>>
    {};

    template<typename... Ts>
    using common_type_t = typename common_type<Ts...>::type;

    template<typename T, typename U, typename... Vs>
    struct common_type<T, U, Vs...>
        : meta::lazy::fold<meta::list<U, Vs...>, T, meta::quote<common_type_t>>
    {};

    /// @}

    /// \addtogroup group-utility Utility
    /// @{
    ///

    /// Users can specialize this to hook the \c common_reference_with concept.
    /// \sa `common_reference`
    template<
        typename T,
        typename U,
        template<typename> class TQual,
        template<typename> class UQual>
    struct basic_common_reference
    {};

    /// \cond
    namespace detail
    {
        using _rref =
            meta::quote_trait<std::add_rvalue_reference>;
        using _lref =
            meta::quote_trait<std::add_lvalue_reference>;

        template<typename>
        struct _xref
        {
            template<typename T>
            using invoke = T;
        };
        template<typename T>
        struct _xref<T &&>
        {
            template<typename U>
            using invoke =
                meta::_t<std::add_rvalue_reference<meta::invoke<_xref<T>, U>>>;
        };
        template<typename T>
        struct _xref<T &>
        {
            template<typename U>
            using invoke =
                meta::_t<std::add_lvalue_reference<meta::invoke<_xref<T>, U>>>;
        };
        template<typename T>
        struct _xref<T const>
        {
            template<typename U>
            using invoke = U const;
        };
        template<typename T>
        struct _xref<T volatile>
        {
            template<typename U>
            using invoke = U volatile;
        };
        template<typename T>
        struct _xref<T const volatile>
        {
            template<typename U>
            using invoke = U const volatile;
        };

        template<typename T, typename U>
        using _basic_common_reference =
            basic_common_reference<
                remove_cvref_t<T>,
                remove_cvref_t<U>,
                _xref<T>::template invoke,
                _xref<U>::template invoke>;

        template<typename T, typename U, typename = void>
        struct _common_reference2
            : if_else_t<
                meta::is_trait<_basic_common_reference<T, U>>::value,
                _basic_common_reference<T, U>,
                common_type<T, U>>
        {};

        template<typename T, typename U>
        struct _common_reference2<T, U, if_t<std::is_reference<_builtin_common_t<T, U>>::value>>
            : _builtin_common<T, U>
        {};
    }
    /// \endcond

    /// Users can specialize this to hook the \c common_reference_with concept.
    /// \sa `basic_common_reference`
    template<typename ...Ts>
    struct common_reference
    {};

    template<typename T>
    struct common_reference<T>
    {
        using type = T;
    };

    template<typename T, typename U>
    struct common_reference<T, U>
        : detail::_common_reference2<T, U>
    {};

    template<typename... Ts>
    using common_reference_t = typename common_reference<Ts...>::type;

    template<typename T, typename U, typename... Vs>
    struct common_reference<T, U, Vs...>
        : meta::lazy::fold<meta::list<U, Vs...>, T, meta::quote<common_reference_t>>
    {};
    /// @}
} // namespace concepts

#endif
