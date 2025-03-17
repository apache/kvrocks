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

#ifndef RANGES_V3_UTILITY_TAGGED_PAIR_HPP
#define RANGES_V3_UTILITY_TAGGED_PAIR_HPP

#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/detail/adl_get.hpp>
#include <range/v3/utility/swap.hpp>

#include <range/v3/detail/prologue.hpp>

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename T>
        using tag_spec = meta::front<meta::as_list<T>>;

        template<typename T>
        using tag_elem = meta::back<meta::as_list<T>>;

        template<typename Base, std::size_t, typename...>
        struct tagged_chain
        {
            using type = _tuple_wrapper_::forward_tuple_interface<Base>;
        };
        template<typename Base, std::size_t I, typename First, typename... Rest>
        struct tagged_chain<Base, I, First, Rest...>
        {
            using type = typename First::template getter<
                Base, I, meta::_t<tagged_chain<Base, I + 1, Rest...>>>;
        };
    } // namespace detail

#if RANGES_BROKEN_CPO_LOOKUP
    namespace _tagged_
    {
        struct adl_hook_
        {};
    } // namespace _tagged_
#endif
    /// \endcond

    template<typename Base, typename... Tags>
    class RANGES_EMPTY_BASES RANGES_DEPRECATED(
        "Class template tagged is deprecated.") tagged
      : public meta::_t<detail::tagged_chain<Base, 0, Tags...>>
#if RANGES_BROKEN_CPO_LOOKUP
      , private _tagged_::adl_hook_
#endif
    {
        CPP_assert(same_as<Base, uncvref_t<Base>>);
        using base_t = meta::_t<detail::tagged_chain<Base, 0, Tags...>>;

        template<typename Other>
        struct can_convert
          : meta::bool_<!RANGES_IS_SAME(Other, Base) &&
                        detail::is_convertible<Other, Base>::value>
        {};

    public:
        tagged() = default;
        using base_t::base_t;
        template(typename Other)(
            requires can_convert<Other>::value)          //
        constexpr tagged(tagged<Other, Tags...> && that) //
            noexcept(std::is_nothrow_constructible<Base, Other>::value)
          : base_t(static_cast<Other &&>(that))
        {}
        template(typename Other)(
            requires can_convert<Other>::value)               //
        constexpr tagged(tagged<Other, Tags...> const & that) //
            noexcept(std::is_nothrow_constructible<Base, Other const &>::value)
          : base_t(static_cast<Other const &>(that))
        {}
        template(typename Other)(
            requires can_convert<Other>::value) //
        constexpr tagged & operator=(tagged<Other, Tags...> && that) //
            noexcept(
                noexcept(std::declval<Base &>() = static_cast<Other &&>(that)))
        {
            static_cast<Base &>(*this) = static_cast<Other &&>(that);
            return *this;
        }
        template(typename Other)(
            requires can_convert<Other>::value) //
        constexpr tagged & operator=(tagged<Other, Tags...> const & that) //
            noexcept(
                noexcept(std::declval<Base &>() = static_cast<Other const &>(that)))
        {
            static_cast<Base &>(*this) = static_cast<Other const &>(that);
            return *this;
        }
        template(typename U)(
            requires (!same_as<tagged, detail::decay_t<U>>) AND
                satisfies<Base &, std::is_assignable, U>)
        constexpr tagged & operator=(U && u) //
            noexcept(noexcept(std::declval<Base &>() = static_cast<U &&>(u)))
        {
            static_cast<Base &>(*this) = static_cast<U &&>(u);
            return *this;
        }
        template(typename B = Base)(
            requires is_swappable<B>::value) //
        constexpr void swap(tagged & that) noexcept(is_nothrow_swappable<B>::value)
        {
            ranges::swap(static_cast<Base &>(*this), static_cast<Base &>(that));
        }
#if !RANGES_BROKEN_CPO_LOOKUP
        template<typename B = Base>
        friend constexpr auto swap(tagged & x, tagged & y) //
            noexcept(is_nothrow_swappable<B>::value)
            -> CPP_broken_friend_ret(void)(
                requires is_swappable<B>::value)
        {
            x.swap(y);
        }
#endif
    };

#if RANGES_BROKEN_CPO_LOOKUP
    namespace _tagged_
    {
        template(typename Base, typename... Tags)(
            requires is_swappable<Base>::value) //
        constexpr void swap(tagged<Base, Tags...> & x, tagged<Base, Tags...> & y) //
            noexcept(is_nothrow_swappable<Base>::value)
        {
            x.swap(y);
        }
    } // namespace _tagged_
#endif

    template<typename F, typename S>
    using tagged_pair RANGES_DEPRECATED("ranges::tagged_pair is deprecated.") =
        tagged<std::pair<detail::tag_elem<F>, detail::tag_elem<S>>, detail::tag_spec<F>,
               detail::tag_spec<S>>;

    template<typename Tag1, typename Tag2, typename T1, typename T2,
             typename R = tagged_pair<Tag1(bind_element_t<T1>), Tag2(bind_element_t<T2>)>>
    RANGES_DEPRECATED("ranges::make_tagged_pair is deprecated.")
    constexpr R make_tagged_pair(T1 && t1, T2 && t2) noexcept(
        std::is_nothrow_constructible<R, T1, T2>::value)
    {
        return {static_cast<T1 &&>(t1), static_cast<T2 &&>(t2)};
    }
} // namespace ranges

#define RANGES_DEFINE_TAG_SPECIFIER(NAME)                                               \
    namespace tag                                                                       \
    {                                                                                   \
        struct NAME                                                                     \
        {                                                                               \
            template<typename Untagged, std::size_t I, typename Next>                   \
            class getter : public Next                                                  \
            {                                                                           \
            protected:                                                                  \
                ~getter() = default;                                                    \
                                                                                        \
            public:                                                                     \
                getter() = default;                                                     \
                getter(getter &&) = default;                                            \
                getter(getter const &) = default;                                       \
                using Next::Next;                                                       \
                getter & operator=(getter &&) = default;                                \
                getter & operator=(getter const &) = default;                           \
                constexpr meta::_t<std::tuple_element<I, Untagged>> & NAME() &          \
                    noexcept(noexcept(detail::adl_get<I>(std::declval<Untagged &>())))  \
                {                                                                       \
                    return detail::adl_get<I>(static_cast<Untagged &>(*this));          \
                }                                                                       \
                constexpr meta::_t<std::tuple_element<I, Untagged>> && NAME() &&        \
                    noexcept(noexcept(detail::adl_get<I>(std::declval<Untagged>())))    \
                {                                                                       \
                    return detail::adl_get<I>(static_cast<Untagged &&>(*this));         \
                }                                                                       \
                constexpr meta::_t<std::tuple_element<I, Untagged>> const & NAME()      \
                    const & noexcept(                                                   \
                        noexcept(detail::adl_get<I>(std::declval<Untagged const &>()))) \
                {                                                                       \
                    return detail::adl_get<I>(static_cast<Untagged const &>(*this));    \
                }                                                                       \
            };                                                                          \
        };                                                                              \
    }                                                                                   \
    /**/

RANGES_DIAGNOSTIC_IGNORE_MISMATCHED_TAGS

namespace std
{
    template<typename Untagged, typename... Tags>
    struct tuple_size<::ranges::tagged<Untagged, Tags...>> : tuple_size<Untagged>
    {};

    template<size_t N, typename Untagged, typename... Tags>
    struct tuple_element<N, ::ranges::tagged<Untagged, Tags...>>
      : tuple_element<N, Untagged>
    {};
} // namespace std

RANGES_DIAGNOSTIC_POP

#include <range/v3/detail/epilogue.hpp>

#endif
