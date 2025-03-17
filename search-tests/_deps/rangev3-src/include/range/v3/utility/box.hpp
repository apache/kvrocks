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

#ifndef RANGES_V3_UTILITY_BOX_HPP
#define RANGES_V3_UTILITY_BOX_HPP

#include <cstdlib>
#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/utility/get.hpp>

#include <range/v3/detail/prologue.hpp>

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_DECLARATIONS

namespace ranges
{
    /// \addtogroup group-utility Utility
    /// @{
    ///

    /// \cond
    template<typename T>
    struct RANGES_DEPRECATED("The ranges::mutable_ class template is deprecated") mutable_
    {
        mutable T value;

        CPP_member
        constexpr CPP_ctor(mutable_)()(
            requires std::is_default_constructible<T>::value)
          : value{}
        {}
        constexpr explicit mutable_(T const & t)
          : value(t)
        {}
        constexpr explicit mutable_(T && t)
          : value(detail::move(t))
        {}
        mutable_ const & operator=(T const & t) const
        {
            value = t;
            return *this;
        }
        mutable_ const & operator=(T && t) const
        {
            value = detail::move(t);
            return *this;
        }
        constexpr operator T &() const &
        {
            return value;
        }
    };

    template<typename T, T v>
    struct RANGES_DEPRECATED("The ranges::constant class template is deprecated") constant
    {
        constant() = default;
        constexpr explicit constant(T const &)
        {}
        constant & operator=(T const &)
        {
            return *this;
        }
        constant const & operator=(T const &) const
        {
            return *this;
        }
        constexpr operator T() const
        {
            return v;
        }
        constexpr T exchange(T const &) const
        {
            return v;
        }
    };
    /// \endcond

    /// \cond
    namespace detail
    {
        // "box" has three different implementations that store a T differently:
        enum class box_compress
        {
            none, // Nothing special: get() returns a reference to a T member subobject
            ebo,  // Apply Empty Base Optimization: get() returns a reference to a T base
                  // subobject
            coalesce // Coalesce all Ts into one T: get() returns a reference to a static
                     // T singleton
        };

        // Per N4582, lambda closures are *not*:
        // - aggregates             ([expr.prim.lambda]/4)
        // - default constructible_from  ([expr.prim.lambda]/p21)
        // - copy assignable        ([expr.prim.lambda]/p21)
        template<typename Fn>
        using could_be_lambda = meta::bool_<!std::is_default_constructible<Fn>::value &&
                                            !std::is_copy_assignable<Fn>::value>;

        template<typename>
        constexpr box_compress box_compression_(...)
        {
            return box_compress::none;
        }
        template<typename T, typename = meta::if_<meta::strict_and<
                                 std::is_empty<T>,
                                 meta::bool_<!detail::is_final_v<T>>
#if defined(__GNUC__) && !defined(__clang__) && __GNUC__ == 6 && __GNUC_MINOR__ < 2
                                 // GCC 6.0 & 6.1 find empty lambdas' implicit conversion
                                 // to function pointer when doing overload resolution
                                 // for function calls. That causes hard errors.
                                 // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=71117
                                 ,
                                 meta::not_<could_be_lambda<T>>
#endif
                                 >>>
        constexpr box_compress box_compression_(long)
        {
            return box_compress::ebo;
        }
#ifndef RANGES_WORKAROUND_MSVC_249830
        // MSVC pukes passing non-constant-expression objects to constexpr
        // functions, so do not coalesce.
        template<typename T,
                 typename =
                     meta::if_<meta::strict_and<std::is_empty<T>, detail::is_trivial<T>>>>
        constexpr box_compress box_compression_(int)
        {
            return box_compress::coalesce;
        }
#endif
        template<typename T>
        constexpr box_compress box_compression()
        {
            return box_compression_<T>(0);
        }
    } // namespace detail
    /// \endcond

    template<typename Element, typename Tag = void,
             detail::box_compress = detail::box_compression<Element>()>
    class box
    {
        Element value;

    public:
        CPP_member
        constexpr CPP_ctor(box)()(                                          //
            noexcept(std::is_nothrow_default_constructible<Element>::value) //
                requires std::is_default_constructible<Element>::value)
          : value{}
        {}
#if defined(__cpp_conditional_explicit) && __cpp_conditional_explicit > 0
        template(typename E)(
            requires (!same_as<box, detail::decay_t<E>>) AND
                constructible_from<Element, E>)
        constexpr explicit(!convertible_to<E, Element>) box(E && e)
            noexcept(std::is_nothrow_constructible<Element, E>::value) //
          : value(static_cast<E &&>(e))
        {}
#else
        template(typename E)(
            requires (!same_as<box, detail::decay_t<E>>) AND
                constructible_from<Element, E> AND
                convertible_to<E, Element>)
        constexpr box(E && e)
            noexcept(std::is_nothrow_constructible<Element, E>::value)
          : value(static_cast<E &&>(e))
        {}
        template(typename E)(
            requires (!same_as<box, detail::decay_t<E>>) AND
                constructible_from<Element, E> AND
                (!convertible_to<E, Element>))
        constexpr explicit box(E && e)
            noexcept(std::is_nothrow_constructible<Element, E>::value) //
          : value(static_cast<E &&>(e))
        {}
#endif

        constexpr Element & get() & noexcept
        {
            return value;
        }
        constexpr Element const & get() const & noexcept
        {
            return value;
        }
        constexpr Element && get() && noexcept
        {
            return detail::move(value);
        }
        constexpr Element const && get() const && noexcept
        {
            return detail::move(value);
        }
    };

    template<typename Element, typename Tag>
    class box<Element, Tag, detail::box_compress::ebo> : Element
    {
    public:
        CPP_member
        constexpr CPP_ctor(box)()(                                          //
            noexcept(std::is_nothrow_default_constructible<Element>::value) //
                requires std::is_default_constructible<Element>::value)
          : Element{}
        {}
#if defined(__cpp_conditional_explicit) && __cpp_conditional_explicit > 0
        template(typename E)(
            requires (!same_as<box, detail::decay_t<E>>) AND
                constructible_from<Element, E>)
        constexpr explicit(!convertible_to<E, Element>) box(E && e)
            noexcept(std::is_nothrow_constructible<Element, E>::value) //
          : Element(static_cast<E &&>(e))
        {}
#else
        template(typename E)(
            requires (!same_as<box, detail::decay_t<E>>) AND
                constructible_from<Element, E> AND
                convertible_to<E, Element>)
        constexpr box(E && e)
            noexcept(std::is_nothrow_constructible<Element, E>::value) //
          : Element(static_cast<E &&>(e))
        {}
        template(typename E)(
            requires (!same_as<box, detail::decay_t<E>>) AND
                constructible_from<Element, E> AND
                (!convertible_to<E, Element>))
        constexpr explicit box(E && e)
            noexcept(std::is_nothrow_constructible<Element, E>::value) //
          : Element(static_cast<E &&>(e))
        {}
#endif

        constexpr Element & get() & noexcept
        {
            return *this;
        }
        constexpr Element const & get() const & noexcept
        {
            return *this;
        }
        constexpr Element && get() && noexcept
        {
            return detail::move(*this);
        }
        constexpr Element const && get() const && noexcept
        {
            return detail::move(*this);
        }
    };

    template<typename Element, typename Tag>
    class box<Element, Tag, detail::box_compress::coalesce>
    {
        static Element value;

    public:
        constexpr box() noexcept = default;

#if defined(__cpp_conditional_explicit) && __cpp_conditional_explicit > 0
        template(typename E)(
            requires (!same_as<box, detail::decay_t<E>>) AND
                constructible_from<Element, E>)
        constexpr explicit(!convertible_to<E, Element>) box(E &&) noexcept
        {}
#else
        template(typename E)(
            requires (!same_as<box, detail::decay_t<E>>) AND
                constructible_from<Element, E> AND
                convertible_to<E, Element>)
        constexpr box(E &&) noexcept
        {}
        template(typename E)(
            requires (!same_as<box, detail::decay_t<E>>) AND
                constructible_from<Element, E> AND
                (!convertible_to<E, Element>))
        constexpr explicit box(E &&) noexcept
        {}
#endif

        constexpr Element & get() & noexcept
        {
            return value;
        }
        constexpr Element const & get() const & noexcept
        {
            return value;
        }
        constexpr Element && get() && noexcept
        {
            return detail::move(value);
        }
        constexpr Element const && get() const && noexcept
        {
            return detail::move(value);
        }
    };

    template<typename Element, typename Tag>
    Element box<Element, Tag, detail::box_compress::coalesce>::value{};

    /// \cond
    namespace _get_
    {
        /// \endcond
        // Get by tag type
        template<typename Tag, typename Element, detail::box_compress BC>
        constexpr Element & get(box<Element, Tag, BC> & b) noexcept
        {
            return b.get();
        }
        template<typename Tag, typename Element, detail::box_compress BC>
        constexpr Element const & get(box<Element, Tag, BC> const & b) noexcept
        {
            return b.get();
        }
        template<typename Tag, typename Element, detail::box_compress BC>
        constexpr Element && get(box<Element, Tag, BC> && b) noexcept
        {
            return detail::move(b).get();
        }
        // Get by index
        template<std::size_t I, typename Element, detail::box_compress BC>
        constexpr Element & get(box<Element, meta::size_t<I>, BC> & b) noexcept
        {
            return b.get();
        }
        template<std::size_t I, typename Element, detail::box_compress BC>
        constexpr Element const & get(
            box<Element, meta::size_t<I>, BC> const & b) noexcept
        {
            return b.get();
        }
        template<std::size_t I, typename Element, detail::box_compress BC>
        constexpr Element && get(box<Element, meta::size_t<I>, BC> && b) noexcept
        {
            return detail::move(b).get();
        }
        /// \cond
    } // namespace _get_
    /// \endcond
    /// @}
} // namespace ranges

RANGES_DIAGNOSTIC_POP

#include <range/v3/detail/epilogue.hpp>

#endif
