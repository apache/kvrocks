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

#ifndef RANGES_V3_UTILITY_SEMIREGULAR_BOX_HPP
#define RANGES_V3_UTILITY_SEMIREGULAR_BOX_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/concepts.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/functional/reference_wrapper.hpp>
#include <range/v3/utility/get.hpp>
#include <range/v3/utility/in_place.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    template<typename T>
    struct semiregular_box;

    namespace detail
    {
        struct semiregular_get
        {
            // clang-format off
            template<typename T>
            friend auto CPP_auto_fun(get)(meta::id_t<semiregular_box<T>> &t)
            (
                return t.get()
            )
            template<typename T>
            friend auto CPP_auto_fun(get)(meta::id_t<semiregular_box<T>> const &t)
            (
                return t.get()
            )
            template<typename T>
            friend auto CPP_auto_fun(get)(meta::id_t<semiregular_box<T>> &&t)
            (
                return detail::move(t).get()
            )
            // clang-format on
        };
    } // namespace detail
    /// \endcond

    /// \addtogroup group-utility
    /// @{
    template<typename T>
    struct semiregular_box : private detail::semiregular_get
    {
    private:
        struct tag
        {};
        template<typename... Args>
        void construct_from(Args &&... args)
        {
            new((void *)std::addressof(data_)) T(static_cast<Args &&>(args)...);
            engaged_ = true;
        }
        void move_assign(T && t, std::true_type)
        {
            data_ = detail::move(t);
        }
        void move_assign(T && t, std::false_type)
        {
            reset();
            construct_from(detail::move(t));
        }
        void copy_assign(T const & t, std::true_type)
        {
            data_ = t;
        }
        void copy_assign(T && t, std::false_type)
        {
            reset();
            construct_from(t);
        }
        constexpr semiregular_box(tag, std::false_type) noexcept
        {}
        constexpr semiregular_box(tag, std::true_type) noexcept(
            std::is_nothrow_default_constructible<T>::value)
          : data_{}
          , engaged_(true)
        {}
        void reset()
        {
            if(engaged_)
            {
                data_.~T();
                engaged_ = false;
            }
        }
        union
        {
            char ch_{};
            T data_;
        };
        bool engaged_{false};

    public:
        constexpr semiregular_box() noexcept(
            std::is_nothrow_default_constructible<T>::value ||
            !std::is_default_constructible<T>::value)
          : semiregular_box(tag{}, std::is_default_constructible<T>{})
        {}
        semiregular_box(semiregular_box && that) noexcept(
            std::is_nothrow_move_constructible<T>::value)
        {
            if(that.engaged_)
                this->construct_from(detail::move(that.data_));
        }
        semiregular_box(semiregular_box const & that) noexcept(
            std::is_nothrow_copy_constructible<T>::value)
        {
            if(that.engaged_)
                this->construct_from(that.data_);
        }
#if defined(__cpp_conditional_explicit) && 0 < __cpp_conditional_explicit
        template(typename U)(
            requires (!same_as<uncvref_t<U>, semiregular_box>) AND
                constructible_from<T, U>)
        explicit(!convertible_to<U, T>) constexpr semiregular_box(U && u)
            noexcept(std::is_nothrow_constructible<T, U>::value)
          : semiregular_box(in_place, static_cast<U &&>(u))
        {}
#else
        template(typename U)(
            requires (!same_as<uncvref_t<U>, semiregular_box>) AND
                constructible_from<T, U> AND (!convertible_to<U, T>)) //
        constexpr explicit semiregular_box(U && u)
            noexcept(std::is_nothrow_constructible<T, U>::value)
          : semiregular_box(in_place, static_cast<U &&>(u))
        {}
        template(typename U)(
            requires (!same_as<uncvref_t<U>, semiregular_box>) AND
                constructible_from<T, U> AND convertible_to<U, T>)
        constexpr semiregular_box(U && u)
            noexcept(std::is_nothrow_constructible<T, U>::value)
          : semiregular_box(in_place, static_cast<U &&>(u))
        {}
#endif
        template(typename... Args)(
            requires constructible_from<T, Args...>)
        constexpr semiregular_box(in_place_t, Args &&... args) //
            noexcept(std::is_nothrow_constructible<T, Args...>::value)
          : data_(static_cast<Args &&>(args)...)
          , engaged_(true)
        {}
        ~semiregular_box()
        {
            reset();
        }
        semiregular_box & operator=(semiregular_box && that) noexcept(
            std::is_nothrow_move_constructible<T>::value &&
            (!std::is_move_assignable<T>::value ||
             std::is_nothrow_move_assignable<T>::value))
        {
            if(engaged_ && that.engaged_)
                this->move_assign(detail::move(that.data_), std::is_move_assignable<T>());
            else if(that.engaged_)
                this->construct_from(detail::move(that.data_));
            else if(engaged_)
                this->reset();
            return *this;
        }
        semiregular_box & operator=(semiregular_box const & that) noexcept(
            std::is_nothrow_copy_constructible<T>::value &&
            (!std::is_copy_assignable<T>::value ||
             std::is_nothrow_copy_assignable<T>::value))
        {
            if(engaged_ && that.engaged_)
                this->copy_assign(that.data_, std::is_copy_assignable<T>());
            else if(that.engaged_)
                this->construct_from(that.data_);
            else if(engaged_)
                this->reset();
            return *this;
        }
        constexpr T & get() & noexcept
        {
            return RANGES_ENSURE(engaged_), data_;
        }
        constexpr T const & get() const & noexcept
        {
            return RANGES_ENSURE(engaged_), data_;
        }
        constexpr T && get() && noexcept
        {
            return RANGES_ENSURE(engaged_), detail::move(data_);
        }
        T const && get() const && = delete;
        constexpr operator T &() & noexcept
        {
            return get();
        }
        constexpr operator T const &() const & noexcept
        {
            return get();
        }
        constexpr operator T &&() && noexcept
        {
            return detail::move(get());
        }
        operator T const &&() const && = delete;
        // clang-format off
        template(typename... Args)(
            requires invocable<T &, Args...>)
        constexpr decltype(auto) operator()(Args &&... args) &
            noexcept(is_nothrow_invocable_v<T &, Args...>)
        {
            return invoke(data_, static_cast<Args &&>(args)...);
        }
        template(typename... Args)(
            requires invocable<T const &, Args...>)
        constexpr decltype(auto) operator()(Args &&... args) const &
            noexcept(is_nothrow_invocable_v<T const &, Args...>)
        {
            return invoke(data_, static_cast<Args &&>(args)...);
        }
        template(typename... Args)(
            requires invocable<T, Args...>)
        constexpr decltype(auto) operator()(Args &&... args) &&
            noexcept(is_nothrow_invocable_v<T, Args...>)
        {
            return invoke(static_cast<T &&>(data_), static_cast<Args &&>(args)...);
        }
        template<typename... Args>
        void operator()(Args &&...) const && = delete;
        // clang-format on
    };

    template<typename T>
    struct semiregular_box<T &>
      : private ranges::reference_wrapper<T &>
      , private detail::semiregular_get
    {
        semiregular_box() = default;
        template(typename Arg)(
            requires constructible_from<ranges::reference_wrapper<T &>, Arg &>)
        semiregular_box(in_place_t, Arg & arg) noexcept //
          : ranges::reference_wrapper<T &>(arg)
        {}
        using ranges::reference_wrapper<T &>::get;
        using ranges::reference_wrapper<T &>::operator T &;
        using ranges::reference_wrapper<T &>::operator();

#if defined(_MSC_VER)
        template(typename U)(
            requires (!same_as<uncvref_t<U>, semiregular_box>) AND
            constructible_from<ranges::reference_wrapper<T &>, U>)
        constexpr semiregular_box(U && u) noexcept(
            std::is_nothrow_constructible<ranges::reference_wrapper<T &>, U>::value)
          : ranges::reference_wrapper<T &>{static_cast<U &&>(u)}
        {}
#else
        using ranges::reference_wrapper<T &>::reference_wrapper;
#endif
    };

    template<typename T>
    struct semiregular_box<T &&>
      : private ranges::reference_wrapper<T &&>
      , private detail::semiregular_get
    {
        semiregular_box() = default;
        template(typename Arg)(
            requires constructible_from<ranges::reference_wrapper<T &&>, Arg>)
        semiregular_box(in_place_t, Arg && arg) noexcept //
          : ranges::reference_wrapper<T &&>(static_cast<Arg &&>(arg))
        {}
        using ranges::reference_wrapper<T &&>::get;
        using ranges::reference_wrapper<T &&>::operator T &&;
        using ranges::reference_wrapper<T &&>::operator();

#if defined(_MSC_VER)
        template(typename U)(
            requires (!same_as<uncvref_t<U>, semiregular_box>) AND
            constructible_from<ranges::reference_wrapper<T &&>, U>)
        constexpr semiregular_box(U && u) noexcept(
            std::is_nothrow_constructible<ranges::reference_wrapper<T &&>, U>::value)
          : ranges::reference_wrapper<T &&>{static_cast<U &&>(u)}
        {}
#else
        using ranges::reference_wrapper<T &&>::reference_wrapper;
#endif
    };

    template<typename T>
    using semiregular_box_t = meta::if_c<(bool)semiregular<T>, T, semiregular_box<T>>;

    template<typename T, bool IsConst = false>
    using semiregular_box_ref_or_val_t = meta::if_c<
        (bool)semiregular<T>,
        meta::if_c<IsConst || std::is_empty<T>::value, T, reference_wrapper<T>>,
        reference_wrapper<
            meta::if_c<IsConst, semiregular_box<T> const, semiregular_box<T>>>>;
    /// @}

    /// \cond
    template<typename T>
    using semiregular_t RANGES_DEPRECATED("Please use semiregular_box_t instead.") =
        semiregular_box_t<T>;

    template<typename T, bool IsConst = false>
    using semiregular_ref_or_val_t RANGES_DEPRECATED(
        "Please use semiregular_box_ref_or_val_t instead.") =
        semiregular_box_ref_or_val_t<T, IsConst>;
    /// \endcond

} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
