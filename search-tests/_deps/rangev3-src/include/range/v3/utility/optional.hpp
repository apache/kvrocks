/// \file
// Range v3 library
//
//  Copyright Casey Carter 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_UTILITY_OPTIONAL_HPP
#define RANGES_V3_UTILITY_OPTIONAL_HPP

#include <exception>
#include <initializer_list>
#include <memory>
#include <new>

#include <concepts/concepts.hpp>

#include <range/v3/detail/config.hpp>
#include <range/v3/utility/addressof.hpp>
#include <range/v3/utility/in_place.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/utility/swap.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    template<typename>
    struct optional;

    struct bad_optional_access : std::exception
    {
        virtual const char * what() const noexcept override
        {
            return "bad optional access";
        }
    };

    struct nullopt_t
    {
        struct tag
        {};
        constexpr explicit nullopt_t(tag) noexcept
        {}
    };
#if RANGES_CXX_INLINE_VARIABLES >= RANGES_CXX_INLINE_VARIABLES_17
    inline constexpr nullopt_t nullopt{nullopt_t::tag{}};
#else
    /// \cond
    namespace detail
    {
        template<typename>
        struct nullopt_holder
        {
            static constexpr nullopt_t nullopt{nullopt_t::tag{}};
        };
        template<typename T>
        constexpr nullopt_t nullopt_holder<T>::nullopt;
    } // namespace detail
    /// \endcond
    namespace
    {
        constexpr auto & nullopt = detail::nullopt_holder<void>::nullopt;
    }
#endif

    /// \cond
    namespace detail
    {
        template<typename = void>
        [[noreturn]] bool throw_bad_optional_access()
        {
            throw bad_optional_access{};
        }

        namespace optional_adl
        {
            template<typename T, bool = std::is_trivially_destructible<T>::value>
            struct optional_storage
            {
                union
                {
                    char dummy_;
                    meta::_t<std::remove_cv<T>> data_;
                };
                bool engaged_;

                constexpr optional_storage() noexcept
                  : optional_storage(
                        tag{},
                        meta::bool_<detail::is_trivially_default_constructible_v<T> &&
                                    detail::is_trivially_copyable_v<T>>{})
                {}
                template(typename... Args)(
                    requires constructible_from<T, Args...>)
                    constexpr explicit optional_storage(in_place_t,
                                                        Args &&... args) //
                    noexcept(std::is_nothrow_constructible<T, Args...>::value)
                  : data_(static_cast<Args &&>(args)...)
                  , engaged_{true}
                {}

                constexpr void reset() noexcept
                {
                    engaged_ = false;
                }

            private:
                struct tag
                {};
                constexpr optional_storage(tag, std::false_type) noexcept
                  : dummy_{}
                  , engaged_{false}
                {}
                constexpr optional_storage(tag, std::true_type) noexcept
                  : data_{}
                  , engaged_{false}
                {}
            };

            template<typename T>
            struct optional_storage<T, false>
            {
                union
                {
                    char dummy_;
                    meta::_t<std::remove_cv<T>> data_;
                };
                bool engaged_;

                ~optional_storage()
                {
                    reset();
                }
                constexpr optional_storage() noexcept
                  : dummy_{}
                  , engaged_{false}
                {}
                template(typename... Args)(
                    requires constructible_from<T, Args...>)
                    constexpr explicit optional_storage(in_place_t,
                                                        Args &&... args) //
                    noexcept(std::is_nothrow_constructible<T, Args...>::value)
                  : data_(static_cast<Args &&>(args)...)
                  , engaged_{true}
                {}
                optional_storage(optional_storage const &) = default;
                optional_storage(optional_storage &&) = default;
                optional_storage & operator=(optional_storage const &) = default;
                optional_storage & operator=(optional_storage &&) = default;

                void reset() noexcept
                {
                    if(engaged_)
                    {
                        data_.~T();
                        engaged_ = false;
                    }
                }
            };

            template<typename T>
            struct optional_base : private optional_storage<T>
            {
                using optional_storage<T>::optional_storage;
                using optional_storage<T>::reset;

                constexpr bool has_value() const noexcept
                {
                    return engaged_;
                }
                constexpr T & operator*() & noexcept
                {
                    return RANGES_EXPECT(engaged_), data_;
                }
                constexpr T const & operator*() const & noexcept
                {
                    return RANGES_EXPECT(engaged_), data_;
                }
                constexpr T && operator*() && noexcept
                {
                    return RANGES_EXPECT(engaged_), detail::move(data_);
                }
                constexpr T const && operator*() const && noexcept
                {
                    return RANGES_EXPECT(engaged_), detail::move(data_);
                }
                constexpr T * operator->() noexcept
                {
                    return RANGES_EXPECT(engaged_), detail::addressof(data_);
                }
                constexpr T const * operator->() const noexcept
                {
                    return RANGES_EXPECT(engaged_), detail::addressof(data_);
                }
                CPP_member
                constexpr auto swap(optional_base & that) //
                    noexcept(std::is_nothrow_move_constructible<T>::value &&
                             is_nothrow_swappable<T>::value) //
                    -> CPP_ret(void)(
                        requires move_constructible<T> && swappable<T>)
                {
                    constexpr bool can_swap_trivially =
                        !::concepts::adl_swap_detail::is_adl_swappable_v<T> &&
                        detail::is_trivially_move_constructible_v<T> &&
                        detail::is_trivially_move_assignable_v<T>;

                    swap_(meta::bool_<can_swap_trivially>{}, that);
                }

            protected:
                template(typename... Args)(
                    requires constructible_from<T, Args...>)
                T & construct_from(Args &&... args)
                    noexcept(std::is_nothrow_constructible<T, Args...>::value)
                {
                    RANGES_EXPECT(!engaged_);
                    auto const address = static_cast<void *>(std::addressof(data_));
                    ::new(address) T(static_cast<Args &&>(args)...);
                    engaged_ = true;
                    return data_;
                }
                template(typename I)(
                    requires constructible_from<T, decltype(*std::declval<const I &>())>)
                T & construct_from_deref(const I & it)
                {
                    RANGES_EXPECT(!engaged_);
                    auto const address = static_cast<void *>(std::addressof(data_));
                    ::new(address) T(*it);
                    engaged_ = true;
                    return data_;
                }
                template<typename U>
                constexpr void assign_from(U && that) noexcept(
                    std::is_nothrow_constructible<T, decltype(*static_cast<U &&>(that))>::
                        value && std::is_nothrow_assignable<
                            T &, decltype(*static_cast<U &&>(that))>::value)
                {
                    if(!that.has_value())
                        reset();
                    else if(engaged_)
                        data_ = *static_cast<U &&>(that);
                    else
                    {
                        auto const address =
                            static_cast<void *>(detail::addressof(data_));
                        ::new(address) T(*static_cast<U &&>(that));
                        engaged_ = true;
                    }
                }

            private:
                constexpr void swap_(std::true_type, optional_base & that) noexcept
                {
                    ranges::swap(static_cast<optional_storage<T> &>(*this),
                                 static_cast<optional_storage<T> &>(that));
                }
                constexpr void swap_(std::false_type, optional_base & that) noexcept(
                    std::is_nothrow_move_constructible<T>::value &&
                        is_nothrow_swappable<T>::value)
                {
                    if(that.engaged_ == engaged_)
                    {
                        if(engaged_)
                            ranges::swap(data_, that.data_);
                    }
                    else
                    {
                        auto & src = engaged_ ? *this : that;
                        auto & dst = engaged_ ? that : *this;
                        dst.construct_from(detail::move(src.data_));
                        src.reset();
                    }
                }

                using optional_storage<T>::engaged_;
                using optional_storage<T>::data_;
            };

            template<typename T>
            struct optional_base<T &>
            {
                optional_base() = default;
                template(typename Arg)(
                    requires constructible_from<T &, Arg>)
                constexpr explicit optional_base(in_place_t, Arg && arg) noexcept //
                  : ptr_(detail::addressof(arg))
                {}
                constexpr bool has_value() const noexcept
                {
                    return ptr_;
                }
                constexpr T & operator*() const noexcept
                {
                    return RANGES_EXPECT(ptr_), *ptr_;
                }
                constexpr T * operator->() const noexcept
                {
                    return RANGES_EXPECT(ptr_), ptr_;
                }
                constexpr void reset() noexcept
                {
                    ptr_ = nullptr;
                }
                CPP_member
                constexpr auto swap(optional_base & that) //
                    noexcept(is_nothrow_swappable<T>::value) //
                    -> CPP_ret(void)(
                        requires swappable<T>)
                {
                    if(ptr_ && that.ptr_)
                        ranges::swap(*ptr_, *that.ptr_);
                    else
                        ranges::swap(ptr_, that.ptr_);
                }

            protected:
                template(typename U)(
                    requires convertible_to<U &, T &>)
                constexpr T & construct_from(U && ref) noexcept
                {
                    RANGES_EXPECT(!ptr_);
                    ptr_ = detail::addressof(ref);
                    return *ptr_;
                }
                template<typename U>
                constexpr void assign_from(U && that)
                {
                    if(ptr_ && that.ptr_)
                        *ptr_ = *that.ptr_;
                    else
                        ptr_ = that.ptr_;
                }

            private:
                T * ptr_ = nullptr;
            };

            template<typename T>
            struct optional_copy : optional_base<T>
            {
                optional_copy() = default;
                optional_copy(optional_copy const & that) noexcept(
                    std::is_nothrow_copy_constructible<T>::value)
                {
                    if(that.has_value())
                        this->construct_from(*that);
                }
                optional_copy(optional_copy &&) = default;
                optional_copy & operator=(optional_copy const &) = default;
                optional_copy & operator=(optional_copy &&) = default;

                using optional_base<T>::optional_base;
            };

            template<typename T>
            using copy_construct_layer =
                meta::if_c<std::is_copy_constructible<T>::value &&
                               !detail::is_trivially_copy_constructible_v<T>,
                           optional_copy<T>, optional_base<T>>;

            template<typename T>
            struct optional_move : copy_construct_layer<T>
            {
                optional_move() = default;
                optional_move(optional_move const &) = default;
                optional_move(optional_move && that) noexcept(
                    std::is_nothrow_move_constructible<T>::value)
                {
                    if(that.has_value())
                        this->construct_from(std::move(*that));
                }
                optional_move & operator=(optional_move const &) = default;
                optional_move & operator=(optional_move &&) = default;

                using copy_construct_layer<T>::copy_construct_layer;
            };

            template<typename T>
            using move_construct_layer =
                meta::if_c<std::is_move_constructible<T>::value &&
                               !detail::is_trivially_move_constructible_v<T>,
                           optional_move<T>, copy_construct_layer<T>>;

            template<typename T>
            struct optional_copy_assign : move_construct_layer<T>
            {
                optional_copy_assign() = default;
                optional_copy_assign(optional_copy_assign const &) = default;
                optional_copy_assign(optional_copy_assign &&) = default;
                optional_copy_assign & operator=(optional_copy_assign const & that) //
                    noexcept(std::is_nothrow_copy_constructible<T>::value &&
                                 std::is_nothrow_copy_assignable<T>::value)
                {
                    this->assign_from(that);
                    return *this;
                }
                optional_copy_assign & operator=(optional_copy_assign &&) = default;

                using move_construct_layer<T>::move_construct_layer;
            };

            template<typename T>
            struct deleted_copy_assign : move_construct_layer<T>
            {
                deleted_copy_assign() = default;
                deleted_copy_assign(deleted_copy_assign const &) = default;
                deleted_copy_assign(deleted_copy_assign &&) = default;
                deleted_copy_assign & operator=(deleted_copy_assign const &) = delete;
                deleted_copy_assign & operator=(deleted_copy_assign &&) = default;

                using move_construct_layer<T>::move_construct_layer;
            };

            template<typename T>
            using copy_assign_layer = meta::if_c<
                std::is_copy_constructible<T>::value && std::is_copy_assignable<T>::value,
                meta::if_c<std::is_reference<T>::value ||
                               !(detail::is_trivially_copy_constructible_v<T> &&
                                 detail::is_trivially_copy_assignable_v<T>),
                           optional_copy_assign<T>, move_construct_layer<T>>,
                deleted_copy_assign<T>>;

            template<typename T>
            struct optional_move_assign : copy_assign_layer<T>
            {
                optional_move_assign() = default;
                optional_move_assign(optional_move_assign const &) = default;
                optional_move_assign(optional_move_assign &&) = default;
                optional_move_assign & operator=(optional_move_assign const &) = default;
                optional_move_assign & operator=(optional_move_assign && that) noexcept(
                    std::is_nothrow_move_constructible<T>::value &&
                        std::is_nothrow_move_assignable<T>::value)
                {
                    this->assign_from(std::move(that));
                    return *this;
                }

                using copy_assign_layer<T>::copy_assign_layer;
            };

            template<typename T>
            struct deleted_move_assign : copy_assign_layer<T>
            {
                deleted_move_assign() = default;
                deleted_move_assign(deleted_move_assign const &) = default;
                deleted_move_assign(deleted_move_assign &&) = default;
                deleted_move_assign & operator=(deleted_move_assign const &) = default;
                deleted_move_assign & operator=(deleted_move_assign &&) = delete;

                using copy_assign_layer<T>::copy_assign_layer;
            };

            template<typename T>
            using move_assign_layer = meta::if_c<
                std::is_move_constructible<T>::value && std::is_move_assignable<T>::value,
                meta::if_c<std::is_reference<T>::value ||
                               !(detail::is_trivially_move_constructible_v<T> &&
                                 detail::is_trivially_move_assignable_v<T>),
                           optional_move_assign<T>, copy_assign_layer<T>>,
                deleted_move_assign<T>>;
        } // namespace optional_adl
    }     // namespace detail
    /// \endcond

    // clang-format off
    /// \concept optional_should_convert
    /// \brief The \c optional_should_convert concept
    template<typename U, typename T>
    CPP_concept optional_should_convert =
        !(
            constructible_from<T, optional<U> &       > ||
            constructible_from<T, optional<U> &&      > ||
            constructible_from<T, optional<U> const & > ||
            constructible_from<T, optional<U> const &&> ||
            convertible_to<optional<U> &,        T> ||
            convertible_to<optional<U> &&,       T> ||
            convertible_to<optional<U> const &,  T> ||
            convertible_to<optional<U> const &&, T>
        );

    /// \concept optional_should_convert_assign
    /// \brief The \c optional_should_convert_assign concept
    template<typename U, typename T>
    CPP_concept optional_should_convert_assign =
        optional_should_convert<U, T> &&
        !(assignable_from<T &, optional<U> &> ||
        assignable_from<T &, optional<U> &&> ||
        assignable_from<T &, optional<U> const &> ||
        assignable_from<T &, optional<U> const &&>);
    // clang-format on

    template<typename T>
    struct optional : detail::optional_adl::move_assign_layer<T>
    {
    private:
        using base_t = detail::optional_adl::move_assign_layer<T>;

    public:
        CPP_assert(destructible<T>);
        static_assert(std::is_object<T>::value || std::is_lvalue_reference<T>::value, "");
        static_assert((bool)!same_as<nullopt_t, uncvref_t<T>>, "");
        static_assert((bool)!same_as<in_place_t, uncvref_t<T>>, "");
        using value_type = meta::_t<std::remove_cv<T>>;

        constexpr optional() noexcept
        {}
        constexpr optional(nullopt_t) noexcept
          : optional{}
        {}
        optional(optional const &) = default;
        optional(optional &&) = default;

        using base_t::base_t;

        template(typename E, typename... Args)(
            requires constructible_from<T, std::initializer_list<E> &, Args...>)
        constexpr explicit optional(in_place_t, std::initializer_list<E> il,
                                    Args &&... args) //
            noexcept(std::is_nothrow_constructible<T, std::initializer_list<E> &,
                                                   Args...>::value)
          : base_t(in_place, il, static_cast<Args &&>(args)...)
        {}

#if defined(__cpp_conditional_explicit) && __cpp_conditional_explicit > 0
        template(typename U = T)(
            requires (!same_as<detail::decay_t<U>, in_place_t>) AND
                (!same_as<detail::decay_t<U>, optional>) AND
                constructible_from<T, U>)
        constexpr explicit(!convertible_to<U, T>) optional(U && v)
          : base_t(in_place, static_cast<U &&>(v))
        {}

        template(typename U)(
            requires optional_should_convert<U, T> AND
                constructible_from<T, U const &>)
        explicit(!convertible_to<U const &, T>) optional(optional<U> const & that)
        {
            if(that.has_value())
                base_t::construct_from(*that);
        }
#else
        template(typename U = T)(
            requires (!same_as<detail::decay_t<U>, in_place_t>) AND
                (!same_as<detail::decay_t<U>, optional>) AND
                constructible_from<T, U> AND
                convertible_to<U, T>)
        constexpr optional(U && v)
          : base_t(in_place, static_cast<U &&>(v))
        {}
        template(typename U = T)(
            requires (!same_as<detail::decay_t<U>, in_place_t>) AND
                (!same_as<detail::decay_t<U>, optional>) AND
                constructible_from<T, U> AND
                (!convertible_to<U, T>))
        constexpr explicit optional(U && v)
          : base_t(in_place, static_cast<U &&>(v))
        {}

        template(typename U)(
            requires optional_should_convert<U, T> AND
                constructible_from<T, U const &> AND
                convertible_to<U const &, T>)
        optional(optional<U> const & that)
        {
            if(that.has_value())
                base_t::construct_from(*that);
        }
        template(typename U)(
            requires optional_should_convert<U, T> AND
                constructible_from<T, U const &> AND
                (!convertible_to<U const &, T>))
        explicit optional(optional<U> const & that)
        {
            if(that.has_value())
                base_t::construct_from(*that);
        }
#endif

        template(typename U)(
            requires optional_should_convert<U, T> AND constructible_from<T, U> AND
                convertible_to<U, T>)
        optional(optional<U> && that)
        {
            if(that.has_value())
                base_t::construct_from(detail::move(*that));
        }
        template(typename U)(
            requires optional_should_convert<U, T> AND constructible_from<T, U> AND
            (!convertible_to<U, T>)) //
        explicit optional(optional<U> && that)
        {
            if(that.has_value())
                base_t::construct_from(detail::move(*that));
        }

        constexpr optional & operator=(nullopt_t) noexcept
        {
            reset();
            return *this;
        }

        optional & operator=(optional const &) = default;
        optional & operator=(optional &&) = default;

        template(typename U = T)(
            requires (!same_as<optional, detail::decay_t<U>>) AND
                (!(satisfies<T, std::is_scalar> && same_as<T, detail::decay_t<U>>)) AND
                constructible_from<T, U> AND
                assignable_from<T &, U>)
        constexpr optional & operator=(U && u) noexcept(
            std::is_nothrow_constructible<T, U>::value &&
                std::is_nothrow_assignable<T &, U>::value)
        {
            if(has_value())
                **this = static_cast<U &&>(u);
            else
                base_t::construct_from(static_cast<U &&>(u));
            return *this;
        }

        template(typename U)(
            requires optional_should_convert_assign<U, T> AND
                constructible_from<T, const U &> AND
                assignable_from<T &, const U &>)
        constexpr optional & operator=(optional<U> const & that)
        {
            base_t::assign_from(that);
            return *this;
        }

        template(typename U)(
            requires optional_should_convert_assign<U, T> AND
                constructible_from<T, U> AND
                assignable_from<T &, U>)
        constexpr optional & operator=(optional<U> && that)
        {
            base_t::assign_from(std::move(that));
            return *this;
        }

        template(typename I)(
            requires constructible_from<T, decltype(*std::declval<const I &>())>)
        T & emplace_deref(const I & it)
        {
            reset();
            return base_t::construct_from_deref(it);
        }

        template(typename... Args)(
            requires constructible_from<T, Args...>)
        T & emplace(Args &&... args) noexcept(
            std::is_nothrow_constructible<T, Args...>::value)
        {
            reset();
            return base_t::construct_from(static_cast<Args &&>(args)...);
        }
        template(typename E, typename... Args)(
            requires constructible_from<T, std::initializer_list<E> &, Args...>)
        T & emplace(std::initializer_list<E> il, Args &&... args) noexcept(
            std::is_nothrow_constructible<T, std::initializer_list<E> &, Args...>::value)
        {
            reset();
            return base_t::construct_from(il, static_cast<Args &&>(args)...);
        }

        using base_t::swap;
        using base_t::operator->;
        using base_t::operator*;

        constexpr explicit operator bool() const noexcept
        {
            return has_value();
        }
        using base_t::has_value;

        constexpr T const & value() const &
        {
            return (has_value() || detail::throw_bad_optional_access()), **this;
        }
        constexpr T & value() &
        {
            return (has_value() || detail::throw_bad_optional_access()), **this;
        }
        constexpr T const && value() const &&
        {
            return (has_value() || detail::throw_bad_optional_access()),
                   detail::move(**this);
        }
        constexpr T && value() &&
        {
            return (has_value() || detail::throw_bad_optional_access()),
                   detail::move(**this);
        }

        template(typename U)(
            requires copy_constructible<T> AND convertible_to<U, T>)
        constexpr T value_or(U && u) const &
        {
            return has_value() ? **this : static_cast<T>((U &&) u);
        }
        template(typename U)(
            requires move_constructible<T> AND convertible_to<U, T>)
        constexpr T value_or(U && u) &&
        {
            return has_value() ? detail::move(**this) : static_cast<T>((U &&) u);
        }

        using base_t::reset;
    };

    /// \cond
    namespace detail
    {
        namespace optional_adl
        {
            constexpr bool convert_bool(bool b) noexcept
            {
                return b;
            }

            // Relational operators [optional.relops]
            template<typename T, typename U>
            constexpr auto operator==(optional<T> const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(*x == *y)))
                    -> decltype(convert_bool(*x == *y))
            {
                return x.has_value() == y.has_value() && (!x || convert_bool(*x == *y));
            }
            template<typename T, typename U>
            constexpr auto operator!=(optional<T> const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(*x != *y)))
                    -> decltype(convert_bool(*x != *y))
            {
                return x.has_value() != y.has_value() || (x && convert_bool(*x != *y));
            }
            template<typename T, typename U>
            constexpr auto operator<(optional<T> const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(*x < *y)))
                    -> decltype(convert_bool(*x < *y))
            {
                return y && (!x || convert_bool(*x < *y));
            }
            template<typename T, typename U>
            constexpr auto operator>(optional<T> const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(*x > *y)))
                    -> decltype(convert_bool(*x > *y))
            {
                return x && (!y || convert_bool(*x > *y));
            }
            template<typename T, typename U>
            constexpr auto operator<=(optional<T> const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(*x <= *y)))
                    -> decltype(convert_bool(*x <= *y))
            {
                return !x || (y && convert_bool(*x <= *y));
            }
            template<typename T, typename U>
            constexpr auto operator>=(optional<T> const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(*x >= *y)))
                    -> decltype(convert_bool(*x >= *y))
            {
                return !y || (x && convert_bool(*x >= *y));
            }

            // Comparisons with nullopt [optional.nullops]
            template<typename T>
            constexpr bool operator==(optional<T> const & x, nullopt_t) noexcept
            {
                return !x;
            }
            template<typename T>
            constexpr bool operator==(nullopt_t, optional<T> const & x) noexcept
            {
                return !x;
            }
            template<typename T>
            constexpr bool operator!=(optional<T> const & x, nullopt_t) noexcept
            {
                return !!x;
            }
            template<typename T>
            constexpr bool operator!=(nullopt_t, optional<T> const & x) noexcept
            {
                return !!x;
            }
            template<typename T>
            constexpr bool operator<(optional<T> const &, nullopt_t) noexcept
            {
                return false;
            }
            template<typename T>
            constexpr bool operator<(nullopt_t, optional<T> const & x) noexcept
            {
                return !!x;
            }
            template<typename T>
            constexpr bool operator>(optional<T> const & x, nullopt_t) noexcept
            {
                return !!x;
            }
            template<typename T>
            constexpr bool operator>(nullopt_t, optional<T> const &) noexcept
            {
                return false;
            }
            template<typename T>
            constexpr bool operator<=(optional<T> const & x, nullopt_t) noexcept
            {
                return !x;
            }
            template<typename T>
            constexpr bool operator<=(nullopt_t, optional<T> const &) noexcept
            {
                return true;
            }
            template<typename T>
            constexpr bool operator>=(optional<T> const &, nullopt_t) noexcept
            {
                return true;
            }
            template<typename T>
            constexpr bool operator>=(nullopt_t, optional<T> const & x) noexcept
            {
                return !x;
            }

            // Comparisons with T [optional.comp_with_t]
            template<typename T, typename U>
            constexpr auto operator==(optional<T> const & x, U const & y) //
                noexcept(noexcept(convert_bool(*x == y)))                 //
                -> decltype(convert_bool(*x == y))
            {
                return x && convert_bool(*x == y);
            }
            template<typename T, typename U>
            constexpr auto operator==(T const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(x == *y)))                 //
                -> decltype(convert_bool(x == *y))
            {
                return y && convert_bool(x == *y);
            }
            template<typename T, typename U>
            constexpr auto operator!=(optional<T> const & x, U const & y) //
                noexcept(noexcept(convert_bool(*x != y)))                 //
                -> decltype(convert_bool(*x != y))
            {
                return !x || convert_bool(*x != y);
            }
            template<typename T, typename U>
            constexpr auto operator!=(T const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(x != *y)))                 //
                -> decltype(convert_bool(x != *y))
            {
                return !y || convert_bool(x != *y);
            }
            template<typename T, typename U>
            constexpr auto operator<(optional<T> const & x, U const & y) //
                noexcept(noexcept(convert_bool(*x < y)))                 //
                -> decltype(convert_bool(*x < y))
            {
                return !x || convert_bool(*x < y);
            }
            template<typename T, typename U>
            constexpr auto operator<(T const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(x < *y)))                 //
                -> decltype(convert_bool(x < *y))
            {
                return y && convert_bool(x < *y);
            }
            template<typename T, typename U>
            constexpr auto operator>(optional<T> const & x, U const & y) //
                noexcept(noexcept(convert_bool(*x > y))) -> decltype(convert_bool(*x > y))
            {
                return x && convert_bool(*x > y);
            }
            template<typename T, typename U>
            constexpr auto operator>(T const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(x > *y)))                 //
                -> decltype(convert_bool(x > *y))
            {
                return !y || convert_bool(x > *y);
            }
            template<typename T, typename U>
            constexpr auto operator<=(optional<T> const & x, U const & y) //
                noexcept(noexcept(convert_bool(*x <= y)))                 //
                -> decltype(convert_bool(*x <= y))
            {
                return !x || convert_bool(*x <= y);
            }
            template<typename T, typename U>
            constexpr auto operator<=(T const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(x <= *y)))                 //
                -> decltype(convert_bool(x <= *y))
            {
                return y && convert_bool(x <= *y);
            }
            template<typename T, typename U>
            constexpr auto operator>=(optional<T> const & x, U const & y) //
                noexcept(noexcept(convert_bool(*x >= y)))                 //
                -> decltype(convert_bool(*x >= y))
            {
                return x && convert_bool(*x >= y);
            }
            template<typename T, typename U>
            constexpr auto operator>=(T const & x, optional<U> const & y) //
                noexcept(noexcept(convert_bool(x >= *y)))                 //
                -> decltype(convert_bool(x >= *y))
            {
                return !y || convert_bool(x >= *y);
            }

            // clang-format off
            template<typename T>
            auto CPP_auto_fun(swap)(optional<T> &x, optional<T> &y)
            (
                return x.swap(y)
            )
            // clang-format on
        } // namespace optional_adl
    } // namespace detail
    /// \endcond

    // clang-format off
    template<typename T>
    constexpr auto CPP_auto_fun(make_optional)(T &&t)
    (
        return optional<detail::decay_t<T>>{static_cast<T &&>(t)}
    )
    template<typename T, typename... Args>
    constexpr auto CPP_auto_fun(make_optional)(Args &&... args)
    (
        return optional<T>{in_place, static_cast<Args &&>(args)...}
    )
    template<typename T, typename U, typename... Args>
    constexpr auto CPP_auto_fun(make_optional)(std::initializer_list<U> il,
                                               Args &&... args)
    (
        return optional<T>{in_place, il, static_cast<Args &&>(args)...}
    )
    // clang-format on

    /// \cond
    namespace detail
    {
        template<typename T, typename Tag = void, bool Enable = true>
        struct non_propagating_cache : optional<T>
        {
            non_propagating_cache() = default;
            constexpr non_propagating_cache(nullopt_t) noexcept
            {}
            constexpr non_propagating_cache(non_propagating_cache const &) noexcept
              : optional<T>{}
            {}
            constexpr non_propagating_cache(non_propagating_cache && that) noexcept
              : optional<T>{}
            {
                that.optional<T>::reset();
            }
            constexpr non_propagating_cache & operator=(
                non_propagating_cache const &) noexcept
            {
                optional<T>::reset();
                return *this;
            }
            constexpr non_propagating_cache & operator=(
                non_propagating_cache && that) noexcept
            {
                that.optional<T>::reset();
                optional<T>::reset();
                return *this;
            }
            using optional<T>::operator=;
            template<class I>
            constexpr T & emplace_deref(const I & i)
            {
                return optional<T>::emplace(*i);
            }
        };

        template<typename T, typename Tag>
        struct non_propagating_cache<T, Tag, false>
        {};
    } // namespace detail
    /// \endcond
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
