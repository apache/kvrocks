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

#ifndef RANGES_V3_UTILITY_COMMON_TUPLE_HPP
#define RANGES_V3_UTILITY_COMMON_TUPLE_HPP

#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/detail/adl_get.hpp>
#include <range/v3/functional/bind.hpp>
#include <range/v3/functional/reference_wrapper.hpp>
#include <range/v3/utility/common_type.hpp>
#include <range/v3/utility/tuple_algorithm.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename... Us, typename Tup, std::size_t... Is>
        std::tuple<Us...> to_std_tuple(Tup && tup, meta::index_sequence<Is...>)
        {
            return std::tuple<Us...>{adl_get<Is>(static_cast<Tup &&>(tup))...};
        }

#ifdef RANGES_WORKAROUND_MSVC_786312
        template<std::size_t, typename...>
        struct args_;

        template<typename, typename>
        inline constexpr bool argstructible = false;
        template<std::size_t N, typename... Ts, typename... Us>
        inline constexpr bool argstructible<args_<N, Ts...>, args_<N, Us...>> =
            (META_IS_CONSTRUCTIBLE(Ts, Us) && ...);

        template<typename, typename>
        inline constexpr bool argsignable = false;
        template<std::size_t N, typename... Ts, typename... Us>
        inline constexpr bool argsignable<args_<N, Ts...>, args_<N, Us...>> =
            (std::is_assignable_v<Ts &, Us> && ...);
#endif // RANGES_WORKAROUND_MSVC_786312

        template<std::size_t N, typename... Ts>
        struct args_
        {
            template<typename... Us>
            args_(args_<N, Us...>, meta::if_c<
#ifdef RANGES_WORKAROUND_MSVC_786312
                                       argstructible<args_, args_<N, Us...>>
#else  // ^^^ workaround / no workaround vvv
                                       meta::and_c<META_IS_CONSTRUCTIBLE(Ts,
                                                                         Us)...>::value
#endif // RANGES_WORKAROUND_MSVC_786312
                                       > * = nullptr)
            {}
            template<typename... Us>
            meta::if_c<
#ifdef RANGES_WORKAROUND_MSVC_786312
                argsignable<args_, args_<N, Us...>>,
#else  // ^^^ workaround / no workaround vvv
                meta::and_c<std::is_assignable<Ts &, Us>::value...>::value,
#endif // RANGES_WORKAROUND_MSVC_786312
                args_ &>
            operator=(args_<N, Us...>)
            {
                return *this;
            }
        };
        template<typename... Ts>
        using args = args_<sizeof...(Ts), Ts...>;
        template<typename... Ts>
        using rargs = args_<sizeof...(Ts), Ts &...>;
    } // namespace detail
    /// \endcond

    template<typename... Ts>
    struct common_tuple : _tuple_wrapper_::forward_tuple_interface<std::tuple<Ts...>>
    {
    private:
        template<typename That, std::size_t... Is>
        common_tuple(That && that, meta::index_sequence<Is...>)
          : common_tuple::forward_tuple_interface{
                detail::adl_get<Is>(static_cast<That &&>(that))...}
        {}
        struct element_assign_
        {
            template<typename T, typename U>
            int operator()(T & t, U && u) const
            {
                t = static_cast<U &&>(u);
                return 0;
            }
        };

    public:
        // Construction
        CPP_member
        CPP_ctor(common_tuple)()( //
            noexcept( //
                meta::and_c<std::is_nothrow_default_constructible<Ts>::value...>::value)
                requires default_constructible<std::tuple<Ts...>>)
          : common_tuple::forward_tuple_interface{}
        {}
        template(typename... Us)(
            requires constructible_from<detail::args<Ts...>, detail::args<Us...>>)
        explicit common_tuple(Us &&... us) //
            noexcept(meta::and_c<std::is_nothrow_constructible<Ts, Us>::value...>::value)
          : common_tuple::forward_tuple_interface{static_cast<Us &&>(us)...}
        {}
        template(typename... Us)(
            requires constructible_from<detail::args<Ts...>, detail::rargs<Us...>>)
        common_tuple(std::tuple<Us...> & that) //
            noexcept(
                meta::and_c<std::is_nothrow_constructible<Ts, Us &>::value...>::value) //
          : common_tuple(that, meta::make_index_sequence<sizeof...(Ts)>{})
        {}
        template(typename... Us)(
            requires constructible_from<detail::args<Ts...>, detail::rargs<Us const...>>)
        common_tuple(std::tuple<Us...> const & that) //
            noexcept(meta::and_c<
                     std::is_nothrow_constructible<Ts, Us const &>::value...>::value) //
          : common_tuple(that, meta::make_index_sequence<sizeof...(Ts)>{})
        {}
        template(typename... Us)(
            requires constructible_from<detail::args<Ts...>, detail::args<Us...>>)
        common_tuple(std::tuple<Us...> && that) //
            noexcept(
                meta::and_c<std::is_nothrow_constructible<Ts, Us>::value...>::value) //
          : common_tuple(std::move(that), meta::make_index_sequence<sizeof...(Ts)>{})
        {}
        template(typename... Us)(
            requires constructible_from<detail::args<Ts...>, detail::rargs<Us...>>)
        common_tuple(common_tuple<Us...> & that) //
            noexcept(
                meta::and_c<std::is_nothrow_constructible<Ts, Us &>::value...>::value) //
          : common_tuple(that, meta::make_index_sequence<sizeof...(Ts)>{})
        {}
        template(typename... Us)(
            requires constructible_from<detail::args<Ts...>, detail::rargs<Us const...>>)
        common_tuple(common_tuple<Us...> const & that) //
            noexcept(meta::and_c<
                     std::is_nothrow_constructible<Ts, Us const &>::value...>::value) //
          : common_tuple(that, meta::make_index_sequence<sizeof...(Ts)>{})
        {}
        template(typename... Us)(
            requires constructible_from<detail::args<Ts...>, detail::args<Us...>>)
        common_tuple(common_tuple<Us...> && that) //
            noexcept(
                meta::and_c<std::is_nothrow_constructible<Ts, Us>::value...>::value) //
          : common_tuple(std::move(that), meta::make_index_sequence<sizeof...(Ts)>{})
        {}

        std::tuple<Ts...> & base() noexcept
        {
            return *this;
        }
        std::tuple<Ts...> const & base() const noexcept
        {
            return *this;
        }

        // Assignment
        template(typename... Us)(
            requires std::is_assignable<detail::args<Ts...> &,
                                        detail::rargs<Us...>>::value) //
        common_tuple & operator=(std::tuple<Us...> & that) noexcept(
            meta::and_c<std::is_nothrow_assignable<Ts &, Us &>::value...>::value)
        {
            (void)tuple_transform(base(), that, element_assign_{});
            return *this;
        }
        template(typename... Us)(
            requires std::is_assignable<detail::args<Ts...> &,
                                        detail::rargs<Us const...>>::value) //
        common_tuple & operator=(std::tuple<Us...> const & that) noexcept(
            meta::and_c<std::is_nothrow_assignable<Ts &, Us const &>::value...>::value)
        {
            (void)tuple_transform(base(), that, element_assign_{});
            return *this;
        }
        template(typename... Us)(
            requires std::is_assignable<detail::args<Ts...> &,
                                        detail::args<Us...>>::value) //
        common_tuple & operator=(std::tuple<Us...> && that) noexcept(
            meta::and_c<std::is_nothrow_assignable<Ts &, Us>::value...>::value)
        {
            (void)tuple_transform(base(), std::move(that), element_assign_{});
            return *this;
        }

        template(typename... Us)(
            requires std::is_assignable<detail::args<Ts const...> &,
                                        detail::rargs<Us...>>::value)
        common_tuple const & operator=(std::tuple<Us...> & that) const noexcept(
            meta::and_c<std::is_nothrow_assignable<Ts const &, Us &>::value...>::value)
        {
            (void)tuple_transform(base(), that, element_assign_{});
            return *this;
        }
        template(typename... Us)(
            requires std::is_assignable<detail::args<Ts const...> &,
                                        detail::rargs<Us const...>>::value)
        common_tuple const & operator=(std::tuple<Us...> const & that) const
            noexcept(meta::and_c<
                     std::is_nothrow_assignable<Ts const &, Us const &>::value...>::value)
        {
            (void)tuple_transform(base(), that, element_assign_{});
            return *this;
        }
        template(typename... Us)(
            requires std::is_assignable<detail::args<Ts const...> &,
                                        detail::args<Us...>>::value)
        common_tuple const & operator=(std::tuple<Us...> && that) const noexcept(
            meta::and_c<std::is_nothrow_assignable<Ts const &, Us &&>::value...>::value)
        {
            (void)tuple_transform(base(), std::move(that), element_assign_{});
            return *this;
        }

        // Conversion
        template(typename... Us)(
            requires constructible_from<detail::args<Us...>, detail::rargs<Ts...>>)
        operator std::tuple<Us...>() & noexcept(
                meta::and_c<std::is_nothrow_constructible<Us, Ts &>::value...>::value)
        {
            return detail::to_std_tuple<Us...>(
                *this, meta::make_index_sequence<sizeof...(Ts)>{});
        }
        template(typename... Us)(
            requires constructible_from<detail::args<Us...>,
                                        detail::rargs<Ts const...>>)
        operator std::tuple<Us...>() const & noexcept(
            meta::and_c<std::is_nothrow_constructible<Us, Ts const &>::value...>::value)
        {
            return detail::to_std_tuple<Us...>(
                *this, meta::make_index_sequence<sizeof...(Ts)>{});
        }
        template(typename... Us)(
            requires constructible_from<detail::args<Us...>, detail::args<Ts...>>)
        operator std::tuple<Us...>() &&
            noexcept(meta::and_c<std::is_nothrow_constructible<Us, Ts>::value...>::value)
        {
            return detail::to_std_tuple<Us...>(
                std::move(*this), meta::make_index_sequence<sizeof...(Ts)>{});
        }
    };

    // Logical operators
#define LOGICAL_OP(OP, CONCEPT)                                                    \
    template(typename... Ts, typename... Us)(                                      \
        requires and_v<CONCEPT<Ts, Us>...>)                                        \
    bool operator OP(common_tuple<Ts...> const & a, common_tuple<Us...> const & b) \
    {                                                                              \
        return a.base() OP b.base();                                               \
    }                                                                              \
    template(typename... Ts, typename... Us)(                                      \
        requires and_v<CONCEPT<Ts, Us>...>)                                        \
    bool operator OP(std::tuple<Ts...> const & a, common_tuple<Us...> const & b)   \
    {                                                                              \
        return a OP b.base();                                                      \
    }                                                                              \
    template(typename... Ts, typename... Us)(                                      \
        requires and_v<CONCEPT<Ts, Us>...>)                                        \
    bool operator OP(common_tuple<Ts...> const & a, std::tuple<Us...> const & b)   \
    {                                                                              \
        return a.base() OP b;                                                      \
    }                                                                              \
    /**/
    LOGICAL_OP(==, equality_comparable_with)
    LOGICAL_OP(!=, equality_comparable_with)
    LOGICAL_OP(<, totally_ordered_with)
    LOGICAL_OP(<=, totally_ordered_with)
    LOGICAL_OP(>, totally_ordered_with)
    LOGICAL_OP(>=, totally_ordered_with)
#undef LOGICAL_OP

    struct make_common_tuple_fn
    {
        template<typename... Args>
        common_tuple<bind_element_t<Args>...> operator()(Args &&... args) const noexcept(
            meta::and_c<std::is_nothrow_constructible<
                bind_element_t<Args>, unwrap_reference_t<Args>>::value...>::value)
        {
            return common_tuple<bind_element_t<Args>...>{
                unwrap_reference(static_cast<Args &&>(args))...};
        }
    };

    /// \ingroup group-utility
    /// \sa `make_common_tuple_fn`
    RANGES_INLINE_VARIABLE(make_common_tuple_fn, make_common_tuple)

    template<typename F, typename S>
    struct common_pair : std::pair<F, S>
    {
    private:
        std::pair<F, S> const & base() const noexcept
        {
            return *this;
        }

    public:
        // Construction
        CPP_member
        CPP_ctor(common_pair)()( //
            noexcept(std::is_nothrow_default_constructible<F>::value && //
                     std::is_nothrow_default_constructible<S>::value) //
            requires default_constructible<F> && default_constructible<S>)
          : std::pair<F, S>{}
        {}
        template(typename F2, typename S2)(
            requires constructible_from<F, F2> AND constructible_from<S, S2>)
        common_pair(F2 && f2, S2 && s2) //
            noexcept(std::is_nothrow_constructible<F, F2>::value &&
                     std::is_nothrow_constructible<S, S2>::value) //
          : std::pair<F, S>{static_cast<F2 &&>(f2), static_cast<S2 &&>(s2)}
        {}
        template(typename F2, typename S2)(
            requires constructible_from<F, F2 &> AND constructible_from<S, S2 &>)
        common_pair(std::pair<F2, S2> & that) //
            noexcept(std::is_nothrow_constructible<F, F2 &>::value &&
                     std::is_nothrow_constructible<S, S2 &>::value) //
          : std::pair<F, S>{that.first, that.second}
        {}
        template(typename F2, typename S2)(
            requires constructible_from<F, F2 const &> AND
                constructible_from<S, S2 const &>)
        common_pair(std::pair<F2, S2> const & that) //
            noexcept(std::is_nothrow_constructible<F, F2 const &>::value &&
                     std::is_nothrow_constructible<S, S2 const &>::value) //
          : std::pair<F, S>{that.first, that.second}
        {}
        template(typename F2, typename S2)(
            requires constructible_from<F, F2> AND constructible_from<S, S2>)
        common_pair(std::pair<F2, S2> && that) //
            noexcept(std::is_nothrow_constructible<F, F2>::value &&
                     std::is_nothrow_constructible<S, S2>::value) //
          : std::pair<F, S>{std::forward<F2>(that.first), std::forward<S2>(that.second)}
        {}

        // Conversion
        template(typename F2, typename S2)(
            requires constructible_from<F2, F &> AND constructible_from<S2, S &>)
        operator std::pair<F2, S2>() & //
            noexcept(std::is_nothrow_constructible<F2, F &>::value &&
                     std::is_nothrow_constructible<S2, S &>::value)
        {
            return {this->first, this->second};
        }
        template(typename F2, typename S2)(
            requires constructible_from<F2, F const &> AND
                constructible_from<S2, S const &>)
        operator std::pair<F2, S2>() const & //
            noexcept(std::is_nothrow_constructible<F2, F const &>::value &&
                     std::is_nothrow_constructible<S2, S const &>::value)
        {
            return {this->first, this->second};
        }
        template(typename F2, typename S2)(
            requires constructible_from<F2, F> AND constructible_from<S2, S>)
        operator std::pair<F2, S2>() &&
            noexcept(std::is_nothrow_constructible<F2, F>::value &&
                     std::is_nothrow_constructible<S2, S>::value)
        {
            return {std::forward<F>(this->first), std::forward<S>(this->second)};
        }

        // Assignment
        template(typename F2, typename S2)(
            requires assignable_from<F &, F2 &> AND assignable_from<S &, S2 &>)
        common_pair & operator=(std::pair<F2, S2> & that) //
            noexcept(std::is_nothrow_assignable<F &, F2 &>::value &&
                     std::is_nothrow_assignable<S &, S2 &>::value)
        {
            this->first = that.first;
            this->second = that.second;
            return *this;
        }
        template(typename F2, typename S2)(
            requires assignable_from<F &, F2 const &> AND
                assignable_from<S &, S2 const &>)
        common_pair & operator=(std::pair<F2, S2> const & that) //
            noexcept(std::is_nothrow_assignable<F &, F2 const &>::value &&
                     std::is_nothrow_assignable<S &, S2 const &>::value)
        {
            this->first = that.first;
            this->second = that.second;
            return *this;
        }
        template(typename F2, typename S2)(
            requires assignable_from<F &, F2> AND assignable_from<S &, S2>)
        common_pair & operator=(std::pair<F2, S2> && that) //
            noexcept(std::is_nothrow_assignable<F &, F2>::value &&
                     std::is_nothrow_assignable<S &, S2>::value)
        {
            this->first = static_cast<F2 &&>(that.first);
            this->second = static_cast<S2 &&>(that.second);
            return *this;
        }

        template(typename F2, typename S2)(
            requires assignable_from<F const &, F2 &> AND
                assignable_from<S const &, S2 &>)
        common_pair const & operator=(std::pair<F2, S2> & that) const //
            noexcept(std::is_nothrow_assignable<F const &, F2 &>::value &&
                     std::is_nothrow_assignable<S const &, S2 &>::value)
        {
            this->first = that.first;
            this->second = that.second;
            return *this;
        }
        template(typename F2, typename S2)(
            requires assignable_from<F const &, F2 const &> AND
                        assignable_from<S const &, S2 const &>)
        common_pair const & operator=(std::pair<F2, S2> const & that) const //
            noexcept(std::is_nothrow_assignable<F const &, F2 const &>::value &&
                     std::is_nothrow_assignable<S const &, S2 const &>::value)
        {
            this->first = that.first;
            this->second = that.second;
            return *this;
        }
        template(typename F2, typename S2)(
            requires assignable_from<F const &, F2> AND assignable_from<S const &, S2>)
        common_pair const & operator=(std::pair<F2, S2> && that) const //
            noexcept(std::is_nothrow_assignable<F const &, F2 &&>::value &&
                     std::is_nothrow_assignable<S const &, S2 &&>::value)
        {
            this->first = static_cast<F2 &&>(that.first);
            this->second = static_cast<S2 &&>(that.second);
            return *this;
        }
    };

    // Logical operators
    template(typename F1, typename S1, typename F2, typename S2)(
        requires equality_comparable_with<F1, F2> AND equality_comparable_with<S1, S2>)
    bool operator==(common_pair<F1, S1> const & a, common_pair<F2, S2> const & b)
    {
        return a.first == b.first && a.second == b.second;
    }
    template(typename F1, typename S1, typename F2, typename S2)(
        requires equality_comparable_with<F1, F2> AND equality_comparable_with<S1, S2>)
    bool operator==(common_pair<F1, S1> const & a, std::pair<F2, S2> const & b)
    {
        return a.first == b.first && a.second == b.second;
    }
    template(typename F1, typename S1, typename F2, typename S2)(
        requires equality_comparable_with<F1, F2> AND equality_comparable_with<S1, S2>)
    bool operator==(std::pair<F1, S1> const & a, common_pair<F2, S2> const & b)
    {
        return a.first == b.first && a.second == b.second;
    }
    template(typename F1, typename S1, typename F2, typename S2)(
        requires totally_ordered_with<F1, F2> AND totally_ordered_with<S1, S2>)
    bool operator<(common_pair<F1, S1> const & a, common_pair<F2, S2> const & b)
    {
        return a.first < b.first || (!(b.first < a.first) && a.second < b.second);
    }
    template(typename F1, typename S1, typename F2, typename S2)(
        requires totally_ordered_with<F1, F2> AND totally_ordered_with<S1, S2>)
    bool operator<(std::pair<F1, S1> const & a, common_pair<F2, S2> const & b)
    {
        return a.first < b.first || (!(b.first < a.first) && a.second < b.second);
    }
    template(typename F1, typename S1, typename F2, typename S2)(
        requires totally_ordered_with<F1, F2> AND totally_ordered_with<S1, S2>)
    bool operator<(common_pair<F1, S1> const & a, std::pair<F2, S2> const & b)
    {
        return a.first < b.first || (!(b.first < a.first) && a.second < b.second);
    }
#define LOGICAL_OP(OP, CONCEPT, RET)                                               \
    template(typename F1, typename S1, typename F2, typename S2)(                  \
        requires CONCEPT<F1, F2> AND CONCEPT<S1, S2>)                              \
    bool operator OP(common_pair<F1, S1> const & a, common_pair<F2, S2> const & b) \
    {                                                                              \
        return RET;                                                                \
    }                                                                              \
    template(typename F1, typename S1, typename F2, typename S2)(                  \
        requires CONCEPT<F1, F2> AND CONCEPT<S1, S2>)                              \
    bool operator OP(std::pair<F1, S1> const & a, common_pair<F2, S2> const & b)   \
    {                                                                              \
        return RET;                                                                \
    }                                                                              \
    template(typename F1, typename S1, typename F2, typename S2)(                  \
        requires CONCEPT<F1, F2> AND CONCEPT<S1, S2>)                              \
    bool operator OP(common_pair<F1, S1> const & a, std::pair<F2, S2> const & b)   \
    {                                                                              \
        return RET;                                                                \
    }                                                                              \
    /**/
    LOGICAL_OP(!=, equality_comparable_with, !(a == b))
    LOGICAL_OP(<=, totally_ordered_with, !(b < a))
    LOGICAL_OP(>, totally_ordered_with, (b < a))
    LOGICAL_OP(>=, totally_ordered_with, !(a < b))
#undef LOGICAL_OP

    struct make_common_pair_fn
    {
        template<typename First, typename Second, typename F = bind_element_t<First>,
                 typename S = bind_element_t<Second>>
        common_pair<F, S> operator()(First && f, Second && s) const //
            noexcept(std::is_nothrow_constructible<F, unwrap_reference_t<First>>::value &&
                     std::is_nothrow_constructible<F, unwrap_reference_t<Second>>::value)
        {
            return {unwrap_reference(static_cast<First &&>(f)),
                    unwrap_reference(static_cast<Second &&>(s))};
        }
    };

    /// \ingroup group-utility
    /// \sa `make_common_pair_fn`
    RANGES_INLINE_VARIABLE(make_common_pair_fn, make_common_pair)

    /// \cond
    namespace detail
    {
        template<typename, typename, typename, typename = void>
        struct common_type_tuple_like
        {};

        template<template<typename...> class T0, typename... Ts,
                 template<typename...> class T1, typename... Us, typename TupleLike>
        struct common_type_tuple_like<T0<Ts...>, T1<Us...>, TupleLike,
                                      meta::if_c<sizeof...(Ts) == sizeof...(Us)>>
          : meta::lazy::let<
                meta::lazy::invoke<TupleLike, meta::lazy::_t<common_type<Ts, Us>>...>>
        {};

        template<typename, typename, typename, typename = void>
        struct common_ref_tuple_like
        {};

        template<template<typename...> class T0, typename... Ts,
                 template<typename...> class T1, typename... Us, typename TupleLike>
        struct common_ref_tuple_like<T0<Ts...>, T1<Us...>, TupleLike,
                                     meta::if_c<sizeof...(Ts) == sizeof...(Us)>>
          : meta::lazy::let<meta::lazy::invoke<
                TupleLike, meta::lazy::_t<common_reference<Ts, Us>>...>>
        {};
    } // namespace detail
    /// \endcond
} // namespace ranges

/// \cond
namespace concepts
{
    // common_type for pairs
    template<typename F1, typename S1, typename F2, typename S2>
    struct common_type<std::pair<F1, S1>, ranges::common_pair<F2, S2>>
      : ranges::detail::common_type_tuple_like<
            std::pair<F1, S1>, ranges::common_pair<F2, S2>,
            meta::quote<ranges::common_pair>>
    {};
    template<typename F1, typename S1, typename F2, typename S2>
    struct common_type<ranges::common_pair<F1, S1>, std::pair<F2, S2>>
      : ranges::detail::common_type_tuple_like<
            ranges::common_pair<F1, S1>, std::pair<F2, S2>,
            meta::quote<ranges::common_pair>>
    {};
    template<typename F1, typename S1, typename F2, typename S2>
    struct common_type<ranges::common_pair<F1, S1>, ranges::common_pair<F2, S2>>
      : ranges::detail::common_type_tuple_like<ranges::common_pair<F1, S1>,
                                               ranges::common_pair<F2, S2>,
                                               meta::quote<ranges::common_pair>>
    {};
    // common_type for tuples
    template<typename... Ts, typename... Us>
    struct common_type<ranges::common_tuple<Ts...>, std::tuple<Us...>>
      : ranges::detail::common_type_tuple_like<
            ranges::common_tuple<Ts...>, std::tuple<Us...>,
            meta::quote<ranges::common_tuple>>
    {};
    template<typename... Ts, typename... Us>
    struct common_type<std::tuple<Ts...>, ranges::common_tuple<Us...>>
      : ranges::detail::common_type_tuple_like<
            std::tuple<Ts...>, ranges::common_tuple<Us...>,
            meta::quote<ranges::common_tuple>>
    {};
    template<typename... Ts, typename... Us>
    struct common_type<ranges::common_tuple<Ts...>, ranges::common_tuple<Us...>>
      : ranges::detail::common_type_tuple_like<ranges::common_tuple<Ts...>,
                                               ranges::common_tuple<Us...>,
                                               meta::quote<ranges::common_tuple>>
    {};

    // common reference for pairs
    template<typename F1, typename S1, typename F2, typename S2,
             template<typename> class Qual1, template<typename> class Qual2>
    struct basic_common_reference<ranges::common_pair<F1, S1>, std::pair<F2, S2>, Qual1,
                                  Qual2>
      : ranges::detail::common_ref_tuple_like<
            ranges::common_pair<Qual1<F1>, Qual1<S1>>, std::pair<Qual2<F2>, Qual2<S2>>,
            meta::quote<ranges::common_pair>>
    {};
    template<typename F1, typename S1, typename F2, typename S2,
             template<typename> class Qual1, template<typename> class Qual2>
    struct basic_common_reference<std::pair<F1, S1>, ranges::common_pair<F2, S2>, Qual1,
                                  Qual2>
      : ranges::detail::common_ref_tuple_like<
            std::pair<Qual1<F1>, Qual1<S1>>, ranges::common_pair<Qual2<F2>, Qual2<S2>>,
            meta::quote<ranges::common_pair>>
    {};
    template<typename F1, typename S1, typename F2, typename S2,
             template<typename> class Qual1, template<typename> class Qual2>
    struct basic_common_reference<ranges::common_pair<F1, S1>,
                                  ranges::common_pair<F2, S2>, Qual1, Qual2>
      : ranges::detail::common_ref_tuple_like<ranges::common_pair<Qual1<F1>, Qual1<S1>>,
                                              ranges::common_pair<Qual2<F2>, Qual2<S2>>,
                                              meta::quote<ranges::common_pair>>
    {};
    // common reference for tuples
    template<typename... Ts, typename... Us, template<typename> class Qual1,
             template<typename> class Qual2>
    struct basic_common_reference<ranges::common_tuple<Ts...>, std::tuple<Us...>, Qual1,
                                  Qual2>
      : ranges::detail::common_ref_tuple_like<
            ranges::common_tuple<Qual1<Ts>...>, std::tuple<Qual2<Us>...>,
            meta::quote<ranges::common_tuple>>
    {};
    template<typename... Ts, typename... Us, template<typename> class Qual1,
             template<typename> class Qual2>
    struct basic_common_reference<std::tuple<Ts...>, ranges::common_tuple<Us...>, Qual1,
                                  Qual2>
      : ranges::detail::common_ref_tuple_like<
            std::tuple<Qual1<Ts>...>, ranges::common_tuple<Qual2<Us>...>,
            meta::quote<ranges::common_tuple>>
    {};
    template<typename... Ts, typename... Us, template<typename> class Qual1,
             template<typename> class Qual2>
    struct basic_common_reference<ranges::common_tuple<Ts...>,
                                  ranges::common_tuple<Us...>, Qual1, Qual2>
      : ranges::detail::common_ref_tuple_like<ranges::common_tuple<Qual1<Ts>...>,
                                              ranges::common_tuple<Qual2<Us>...>,
                                              meta::quote<ranges::common_tuple>>
    {};
} // namespace concepts
/// \endcond

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_MISMATCHED_TAGS
RANGES_BEGIN_NAMESPACE_STD
    template<typename First, typename Second>
    struct tuple_size<::ranges::common_pair<First, Second>>
      : std::integral_constant<size_t, 2>
    {};

    template<typename First, typename Second>
    struct tuple_element<0, ::ranges::common_pair<First, Second>>
    {
        using type = First;
    };

    template<typename First, typename Second>
    struct tuple_element<1, ::ranges::common_pair<First, Second>>
    {
        using type = Second;
    };

    template<typename... Ts>
    struct tuple_size<::ranges::common_tuple<Ts...>>
      : std::integral_constant<size_t, sizeof...(Ts)>
    {};

    template<size_t N, typename... Ts>
    struct tuple_element<N, ::ranges::common_tuple<Ts...>>
      : tuple_element<N, tuple<Ts...>>
    {};

#if RANGES_CXX_VER > RANGES_CXX_STD_17
RANGES_BEGIN_NAMESPACE_VERSION
    template<typename...>
    struct common_type;

    // common_type for pairs
    template<typename F1, typename S1, typename F2, typename S2>
    struct common_type<std::pair<F1, S1>, ::ranges::common_pair<F2, S2>>
      : ::ranges::detail::common_type_tuple_like<
            std::pair<F1, S1>, ::ranges::common_pair<F2, S2>,
            ::meta::quote<::ranges::common_pair>>
    {};
    template<typename F1, typename S1, typename F2, typename S2>
    struct common_type<::ranges::common_pair<F1, S1>, std::pair<F2, S2>>
      : ::ranges::detail::common_type_tuple_like<
            ::ranges::common_pair<F1, S1>, std::pair<F2, S2>,
            ::meta::quote<::ranges::common_pair>>
    {};
    template<typename F1, typename S1, typename F2, typename S2>
    struct common_type<::ranges::common_pair<F1, S1>, ::ranges::common_pair<F2, S2>>
      : ::ranges::detail::common_type_tuple_like<::ranges::common_pair<F1, S1>,
                                               ::ranges::common_pair<F2, S2>,
                                               ::meta::quote<::ranges::common_pair>>
    {};
    // common_type for tuples
    template<typename... Ts, typename... Us>
    struct common_type<::ranges::common_tuple<Ts...>, std::tuple<Us...>>
      : ::ranges::detail::common_type_tuple_like<
            ::ranges::common_tuple<Ts...>, std::tuple<Us...>,
            ::meta::quote<::ranges::common_tuple>>
    {};
    template<typename... Ts, typename... Us>
    struct common_type<std::tuple<Ts...>, ::ranges::common_tuple<Us...>>
      : ::ranges::detail::common_type_tuple_like<
            std::tuple<Ts...>, ::ranges::common_tuple<Us...>,
            ::meta::quote<::ranges::common_tuple>>
    {};
    template<typename... Ts, typename... Us>
    struct common_type<::ranges::common_tuple<Ts...>, ::ranges::common_tuple<Us...>>
      : ::ranges::detail::common_type_tuple_like<::ranges::common_tuple<Ts...>,
                                               ::ranges::common_tuple<Us...>,
                                               ::meta::quote<::ranges::common_tuple>>
    {};

    template<typename, typename, template<typename> class, template<typename> class>
    struct basic_common_reference;

    // common reference for pairs
    template<typename F1, typename S1, typename F2, typename S2,
             template<typename> class Qual1, template<typename> class Qual2>
    struct basic_common_reference<::ranges::common_pair<F1, S1>, std::pair<F2, S2>, Qual1,
                                  Qual2>
      : ::ranges::detail::common_ref_tuple_like<
            ::ranges::common_pair<Qual1<F1>, Qual1<S1>>, std::pair<Qual2<F2>, Qual2<S2>>,
            ::meta::quote<::ranges::common_pair>>
    {};
    template<typename F1, typename S1, typename F2, typename S2,
             template<typename> class Qual1, template<typename> class Qual2>
    struct basic_common_reference<std::pair<F1, S1>, ::ranges::common_pair<F2, S2>, Qual1,
                                  Qual2>
      : ::ranges::detail::common_ref_tuple_like<
            std::pair<Qual1<F1>, Qual1<S1>>, ::ranges::common_pair<Qual2<F2>, Qual2<S2>>,
            ::meta::quote<::ranges::common_pair>>
    {};
    template<typename F1, typename S1, typename F2, typename S2,
             template<typename> class Qual1, template<typename> class Qual2>
    struct basic_common_reference<::ranges::common_pair<F1, S1>,
                                  ::ranges::common_pair<F2, S2>, Qual1, Qual2>
      : ::ranges::detail::common_ref_tuple_like<::ranges::common_pair<Qual1<F1>, Qual1<S1>>,
                                              ::ranges::common_pair<Qual2<F2>, Qual2<S2>>,
                                              ::meta::quote<::ranges::common_pair>>
    {};
    // common reference for tuples
    template<typename... Ts, typename... Us, template<typename> class Qual1,
             template<typename> class Qual2>
    struct basic_common_reference<::ranges::common_tuple<Ts...>, std::tuple<Us...>, Qual1,
                                  Qual2>
      : ::ranges::detail::common_ref_tuple_like<
            ::ranges::common_tuple<Qual1<Ts>...>, std::tuple<Qual2<Us>...>,
            ::meta::quote<::ranges::common_tuple>>
    {};
    template<typename... Ts, typename... Us, template<typename> class Qual1,
             template<typename> class Qual2>
    struct basic_common_reference<std::tuple<Ts...>, ::ranges::common_tuple<Us...>, Qual1,
                                  Qual2>
      : ::ranges::detail::common_ref_tuple_like<
            std::tuple<Qual1<Ts>...>, ::ranges::common_tuple<Qual2<Us>...>,
            ::meta::quote<::ranges::common_tuple>>
    {};
    template<typename... Ts, typename... Us, template<typename> class Qual1,
             template<typename> class Qual2>
    struct basic_common_reference<::ranges::common_tuple<Ts...>,
                                  ::ranges::common_tuple<Us...>, Qual1, Qual2>
      : ::ranges::detail::common_ref_tuple_like<::ranges::common_tuple<Qual1<Ts>...>,
                                              ::ranges::common_tuple<Qual2<Us>...>,
                                              ::meta::quote<::ranges::common_tuple>>
    {};
RANGES_END_NAMESPACE_VERSION
#endif // RANGES_CXX_VER > RANGES_CXX_STD_17
RANGES_END_NAMESPACE_STD
RANGES_DIAGNOSTIC_POP

#include <range/v3/detail/epilogue.hpp>

#endif
