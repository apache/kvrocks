// Range v3 library
//
//  Copyright Eric Niebler 2013-2014, 2016-present
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_STD_DETAIL_ASSOCIATED_TYPES_HPP
#define RANGES_V3_STD_DETAIL_ASSOCIATED_TYPES_HPP

#include <climits>
#include <cstdint>

#include <range/v3/detail/config.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-iterator
    /// @{

    ////////////////////////////////////////////////////////////////////////////////////////
    /// \cond
    namespace detail
    {
        struct nil_
        {};

        template<typename T, typename...>
        using always_ = T;

#if defined(_MSC_VER) && !defined(__clang__) && !defined(__EDG__)
        // MSVC laughs at your silly micro-optimizations and implements
        // conditional_t, enable_if_t, is_object_v, and is_integral_v in the
        // compiler.
        using std::conditional_t;
        using std::enable_if;
        using std::enable_if_t;
#else  // ^^^ MSVC / not MSVC vvv
        template<bool>
        struct _cond
        {
            template<typename, typename U>
            using invoke = U;
        };
        template<>
        struct _cond<true>
        {
            template<typename T, typename>
            using invoke = T;
        };
        template<bool B, typename T, typename U>
        using conditional_t = typename _cond<B>::template invoke<T, U>;

        template<bool>
        struct enable_if
        {};
        template<>
        struct enable_if<true>
        {
            template<typename T>
            using invoke = T;
        };
        template<bool B, typename T = void>
        using enable_if_t = typename enable_if<B>::template invoke<T>;

#ifndef __clang__
        // NB: insufficient for MSVC, which (non-conformingly) allows function
        // pointers to implicitly convert to void*.
        void is_objptr_(void const volatile *);

        // std::is_object, optimized for compile time.
        template<typename T>
        constexpr bool is_object_(long)
        {
            return false;
        }
        template<typename T>
        constexpr bool is_object_(int, T * (*q)(T &) = nullptr, T * p = nullptr,
                                  decltype(detail::is_objptr_(q(*p))) * = nullptr)
        {
            return (void)p, (void)q, true;
        }
#endif // !__clang__

        template<typename T>
        constexpr bool is_integral_(...)
        {
            return false;
        }
        template<typename T, T = 1>
        constexpr bool is_integral_(long)
        {
            return true;
        }
#if defined(__cpp_nontype_template_parameter_class) && \
    __cpp_nontype_template_parameter_class > 0
        template<typename T>
        constexpr bool is_integral_(int, int T::* = nullptr)
        {
            return false;
        }
#endif
#endif // detect MSVC

        template<typename T>
        struct with_difference_type_
        {
            using difference_type = T;
        };

        template<typename T>
        using difference_result_t =
            decltype(std::declval<T const &>() - std::declval<T const &>());

        template<typename, typename = void>
        struct incrementable_traits_2_
        {};

        template<typename T>
        struct incrementable_traits_2_<
            T,
#if defined(_MSC_VER) && !defined(__clang__) && !defined(__EDG__)
            std::enable_if_t<std::is_integral_v<difference_result_t<T>>>>
#elif defined(RANGES_WORKAROUND_GCC_91923)
            std::enable_if_t<std::is_integral<difference_result_t<T>>::value>>
#else
            always_<void, int[is_integral_<difference_result_t<T>>(0)]>>
#endif // detect MSVC
        {
            using difference_type = std::make_signed_t<difference_result_t<T>>;
        };

        template<typename T, typename = void>
        struct incrementable_traits_1_ : incrementable_traits_2_<T>
        {};

        template<typename T>
        struct incrementable_traits_1_<T *>
#ifdef __clang__
          : conditional_t<__is_object(T), with_difference_type_<std::ptrdiff_t>, nil_>
#elif defined(_MSC_VER) && !defined(__EDG__)
          : conditional_t<std::is_object_v<T>, with_difference_type_<std::ptrdiff_t>, nil_>
#else // ^^^ MSVC / not MSVC vvv
          : conditional_t<is_object_<T>(0), with_difference_type_<std::ptrdiff_t>, nil_>
#endif // detect MSVC
        {};

        template<typename T>
        struct incrementable_traits_1_<T, always_<void, typename T::difference_type>>
        {
            using difference_type = typename T::difference_type;
        };
    } // namespace detail
    /// \endcond

    template<typename T>
    struct incrementable_traits : detail::incrementable_traits_1_<T>
    {};

    template<typename T>
    struct incrementable_traits<T const> : incrementable_traits<T>
    {};

    /// \cond
    namespace detail
    {
#ifdef __clang__
        template<typename T, bool = __is_object(T)>
#elif defined(_MSC_VER) && !defined(__EDG__)
        template<typename T, bool = std::is_object_v<T>>
#else // ^^^ MSVC / not MSVC vvv
        template<typename T, bool = is_object_<T>(0)>
#endif // detect MSVC
        struct with_value_type_
        {};
        template<typename T>
        struct with_value_type_<T, true>
        {
            using value_type = T;
        };
        template<typename T>
        struct with_value_type_<T const, true>
        {
            using value_type = T;
        };
        template<typename T>
        struct with_value_type_<T volatile, true>
        {
            using value_type = T;
        };
        template<typename T>
        struct with_value_type_<T const volatile, true>
        {
            using value_type = T;
        };
        template<typename, typename = void>
        struct readable_traits_2_
        {};
        template<typename T>
        struct readable_traits_2_<T, always_<void, typename T::element_type>>
          : with_value_type_<typename T::element_type>
        {};
        template<typename T, typename = void>
        struct readable_traits_1_ : readable_traits_2_<T>
        {};
        template<typename T>
        struct readable_traits_1_<T[]> : with_value_type_<T>
        {};
        template<typename T, std::size_t N>
        struct readable_traits_1_<T[N]> : with_value_type_<T>
        {};
        template<typename T>
        struct readable_traits_1_<T *> : detail::with_value_type_<T>
        {};
        template<typename T>
        struct readable_traits_1_<T, always_<void, typename T::value_type>>
          : with_value_type_<typename T::value_type>
        {};
    } // namespace detail
    /// \endcond

    ////////////////////////////////////////////////////////////////////////////////////////
    // Not to spec:
    // * For class types with both member value_type and element_type, value_type is
    //   preferred (see ericniebler/stl2#299).
    template<typename T>
    struct indirectly_readable_traits : detail::readable_traits_1_<T>
    {};

    template<typename T>
    struct indirectly_readable_traits<T const> : indirectly_readable_traits<T>
    {};

    /// \cond
    namespace detail
    {
        template<typename D = std::ptrdiff_t>
        struct std_output_iterator_traits
        {
            using iterator_category = std::output_iterator_tag;
            using difference_type = D;
            using value_type = void;
            using reference = void;
            using pointer = void;
        };

        // For testing whether a particular instantiation of std::iterator_traits
        // is user-specified or not.
#if defined(_MSVC_STL_UPDATE) && defined(__cpp_lib_concepts) && _MSVC_STL_UPDATE >= 201908L
        template<typename I>
        inline constexpr bool is_std_iterator_traits_specialized_v =
            !std::_Is_from_primary<std::iterator_traits<I>>;
#else
#if defined(__GLIBCXX__)
        template<typename I>
        char (&is_std_iterator_traits_specialized_impl_(std::__iterator_traits<I> *))[2];
        template<typename I>
        char is_std_iterator_traits_specialized_impl_(void *);
#elif defined(_LIBCPP_VERSION)
        // In older versions of libc++, the base template inherits from std::__iterator_traits<typename, bool>.
        template<template<typename, bool> class IteratorTraitsBase, typename I, bool B>
        char (&libcpp_iterator_traits_base_impl(IteratorTraitsBase<I, B> *))[2];
        template<template<typename, bool> class IteratorTraitsBase, typename I>
        char libcpp_iterator_traits_base_impl(void *);

        // In newer versions, the base template has only one template parameter and provides the
        // __primary_template typedef which aliases the iterator_traits specialization.
        template<template<typename> class, typename I>
        char (&libcpp_iterator_traits_base_impl(typename std::iterator_traits<I>::__primary_template *))[2];
        template<template<typename> class, typename I>
        char libcpp_iterator_traits_base_impl(void *);

        template<typename I>
        auto is_std_iterator_traits_specialized_impl_(std::iterator_traits<I>* traits)
            -> decltype(libcpp_iterator_traits_base_impl<std::__iterator_traits, I>(traits));
#elif defined(_MSVC_STL_VERSION)
        template<typename I>
        char (&is_std_iterator_traits_specialized_impl_(
            std::_Iterator_traits_base<I> *))[2];
        template<typename I>
        char is_std_iterator_traits_specialized_impl_(void *);
#else
        template<typename I>
        char (&is_std_iterator_traits_specialized_impl_(void *))[2];
#endif
        template<typename, typename T>
        char (&is_std_iterator_traits_specialized_impl_(std::iterator_traits<T *> *))[2];

        template<typename I>
        RANGES_INLINE_VAR constexpr bool is_std_iterator_traits_specialized_v =
            1 == sizeof(is_std_iterator_traits_specialized_impl_<I>(
                     static_cast<std::iterator_traits<I> *>(nullptr)));
#endif
        // The standard iterator_traits<T *> specialization(s) do not count
        // as user-specialized. This will no longer be necessary in C++20.
        // This helps with `T volatile*` and `void *`.
        template<typename T>
        RANGES_INLINE_VAR constexpr bool is_std_iterator_traits_specialized_v<T *> =
            false;
    } // namespace detail
    /// \endcond
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
