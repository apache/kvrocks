/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_ITERATOR_MOVE_ITERATORS_HPP
#define RANGES_V3_ITERATOR_MOVE_ITERATORS_HPP

#include <cstddef>
#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/basic_iterator.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-iterator
    /// @{
    template<typename I>
    struct move_iterator
    {
    private:
        CPP_assert(input_iterator<I>);
        I current_ = I{};

    public:
        using iterator_type = I;
        using difference_type = iter_difference_t<I>;
        using value_type = iter_value_t<I>;
        using iterator_category = std::input_iterator_tag;
        using reference = iter_rvalue_reference_t<I>;

        constexpr move_iterator() = default;
        explicit move_iterator(I i)
          : current_(i)
        {}
        template(typename O)(
            requires convertible_to<O, I>)
        move_iterator(move_iterator<O> const & i)
          : current_(i.base())
        {}
        template(typename O)(
            requires convertible_to<O, I>)
        move_iterator & operator=(move_iterator<O> const & i)
        {
            current_ = i.base();
            return *this;
        }
        I base() const
        {
            return current_;
        }
        // clang-format off
        auto CPP_auto_fun(operator*)()(const)
        (
            return iter_move(current_)
        )
            // clang-format on
            move_iterator &
            operator++()
        {
            ++current_;
            return *this;
        }
        CPP_member
        auto operator++(int) //
            -> CPP_ret(void)(
                requires (!forward_iterator<I>))
        {
            ++current_;
        }
        CPP_member
        auto operator++(int) //
            -> CPP_ret(move_iterator)(
                requires forward_iterator<I>)
        {
            return move_iterator(current_++);
        }
        CPP_member
        auto operator--() //
            -> CPP_ret(move_iterator &)(
                requires forward_iterator<I>)
        {
            --current_;
            return *this;
        }
        CPP_member
        auto operator--(int) //
            -> CPP_ret(move_iterator)(
                requires bidirectional_iterator<I>)
        {
            return move_iterator(current_--);
        }
        CPP_member
        auto operator+(difference_type n) const //
            -> CPP_ret(move_iterator)(
                requires random_access_iterator<I>)
        {
            return move_iterator(current_ + n);
        }
        CPP_member
        auto operator+=(difference_type n)
            -> CPP_ret(move_iterator &)(
                requires random_access_iterator<I>)
        {
            current_ += n;
            return *this;
        }
        CPP_member
        auto operator-(difference_type n) const //
            -> CPP_ret(move_iterator)(
                requires random_access_iterator<I>)
        {
            return move_iterator(current_ - n);
        }
        CPP_member
        auto operator-=(difference_type n) //
            -> CPP_ret(move_iterator &)(
                requires random_access_iterator<I>)
        {
            current_ -= n;
            return *this;
        }
        CPP_member
        auto operator[](difference_type n) const //
            -> CPP_ret(reference)(
                requires random_access_iterator<I>)
        {
            return iter_move(current_ + n);
        }

        template<typename I2>
        friend auto operator==(move_iterator const & x, move_iterator<I2> const & y)
            -> CPP_broken_friend_ret(bool)(
                requires equality_comparable_with<I, I2>)
        {
            return x.base() == y.base();
        }
        template<typename I2>
        friend auto operator!=(move_iterator const & x, move_iterator<I2> const & y)
            -> CPP_broken_friend_ret(bool)(
                requires equality_comparable_with<I, I2>)
        {
            return !(x == y);
        }
        template<typename I2>
        friend auto operator<(move_iterator const & x, move_iterator<I2> const & y)
            -> CPP_broken_friend_ret(bool)(
                requires totally_ordered_with<I, I2>)
        {
            return x.base() < y.base();
        }
        template<typename I2>
        friend auto operator<=(move_iterator const & x, move_iterator<I2> const & y)
            -> CPP_broken_friend_ret(bool)(
                requires totally_ordered_with<I, I2>)
        {
            return !(y < x);
        }
        template<typename I2>
        friend auto operator>(move_iterator const & x, move_iterator<I2> const & y)
            -> CPP_broken_friend_ret(bool)(
                requires totally_ordered_with<I, I2>)
        {
            return y < x;
        }
        template<typename I2>
        friend auto operator>=(move_iterator const & x, move_iterator<I2> const & y)
            -> CPP_broken_friend_ret(bool)(
                requires totally_ordered_with<I, I2>)
        {
            return !(x < y);
        }

        template<typename I2>
        friend auto operator-(move_iterator const & x, move_iterator<I2> const & y)
            -> CPP_broken_friend_ret(iter_difference_t<I2>)(
                requires sized_sentinel_for<I, I2>)
        {
            return x.base() - y.base();
        }
        CPP_broken_friend_member
        friend auto operator+(iter_difference_t<I> n,
                              move_iterator const & x)
            -> CPP_broken_friend_ret(move_iterator)(
                requires random_access_iterator<I>)
        {
            return x + n;
        }
    };

    struct make_move_iterator_fn
    {
        template(typename I)(
            requires input_iterator<I>)
        constexpr move_iterator<I> operator()(I it) const
        {
            return move_iterator<I>{detail::move(it)};
        }
    };

    RANGES_INLINE_VARIABLE(make_move_iterator_fn, make_move_iterator)

    template<typename S>
    struct move_sentinel
    {
    private:
        S sent_;

    public:
        constexpr move_sentinel()
          : sent_{}
        {}
        constexpr explicit move_sentinel(S s)
          : sent_(detail::move(s))
        {}
        template(typename OS)(
            requires convertible_to<OS, S>)
        constexpr explicit move_sentinel(move_sentinel<OS> const & that)
          : sent_(that.base())
        {}
        template(typename OS)(
            requires convertible_to<OS, S>)
        move_sentinel & operator=(move_sentinel<OS> const & that)
        {
            sent_ = that.base();
            return *this;
        }
        S base() const
        {
            return sent_;
        }

        template<typename I>
        friend auto operator==(move_iterator<I> const & i, move_sentinel const & s)
            -> CPP_broken_friend_ret(bool)(
                requires sentinel_for<S, I>)
        {
            return i.base() == s.base();
        }
        template<typename I>
        friend auto operator==(move_sentinel const & s, move_iterator<I> const & i)
            -> CPP_broken_friend_ret(bool)(
                requires sentinel_for<S, I>)
        {
            return s.base() == i.base();
        }
        template<typename I>
        friend auto operator!=(move_iterator<I> const & i, move_sentinel const & s)
            -> CPP_broken_friend_ret(bool)(
                requires sentinel_for<S, I>)
        {
            return i.base() != s.base();
        }
        template<typename I>
        friend auto operator!=(move_sentinel const & s, move_iterator<I> const & i)
            -> CPP_broken_friend_ret(bool)(
                requires sentinel_for<S, I>)
        {
            return s.base() != i.base();
        }
    };

    struct make_move_sentinel_fn
    {
        template(typename I)(
            requires input_iterator<I>)
        constexpr move_iterator<I> operator()(I i) const
        {
            return move_iterator<I>{detail::move(i)};
        }

        template(typename S)(
            requires semiregular<S> AND (!input_iterator<S>)) //
        constexpr move_sentinel<S> operator()(S s) const
        {
            return move_sentinel<S>{detail::move(s)};
        }
    };

    RANGES_INLINE_VARIABLE(make_move_sentinel_fn, make_move_sentinel)

    /// \cond
    namespace detail
    {
        template<typename I, bool IsReadable>
        struct move_into_cursor_types_
        {};

        template<typename I>
        struct move_into_cursor_types_<I, true>
        {
            using value_type = iter_value_t<I>;
            using single_pass = meta::bool_<(bool)single_pass_iterator_<I>>;
        };

        template<typename I>
        using move_into_cursor_types =
            move_into_cursor_types_<I, (bool)indirectly_readable<I>>;

        template<typename I>
        struct move_into_cursor : move_into_cursor_types<I>
        {
        private:
            friend range_access;
            struct mixin : basic_mixin<move_into_cursor>
            {
                mixin() = default;
                #ifndef _MSC_VER
                using basic_mixin<move_into_cursor>::basic_mixin;
                #else
                constexpr explicit mixin(move_into_cursor && cur)
                  : basic_mixin<move_into_cursor>(static_cast<move_into_cursor &&>(cur))
                {}
                constexpr explicit mixin(move_into_cursor const & cur)
                  : basic_mixin<move_into_cursor>(cur)
                {}
                #endif
                explicit mixin(I it)
                  : mixin{move_into_cursor{std::move(it)}}
                {}
                I base() const
                {
                    return this->get().it_;
                }
            };

            I it_ = I();

            explicit move_into_cursor(I it)
              : it_(std::move(it))
            {}
            void next()
            {
                ++it_;
            }
            template(typename T)(
                requires indirectly_writable<I, aux::move_t<T>>)
            void write(T && t) noexcept(noexcept(*it_ = std::move(t)))
            {
                *it_ = std::move(t);
            }
            template(typename T)(
                requires indirectly_writable<I, aux::move_t<T>>)
            void write(T && t) const noexcept(noexcept(*it_ = std::move(t)))
            {
                *it_ = std::move(t);
            }
            CPP_member
            auto read() const noexcept(noexcept(*std::declval<I const &>()))
                -> CPP_ret(iter_reference_t<I>)(
                    requires indirectly_readable<I>)
            {
                return *it_;
            }
            CPP_member
            auto equal(move_into_cursor const & that) const //
                -> CPP_ret(bool)(
                    requires input_iterator<I>)
            {
                return it_ == that.it_;
            }
            CPP_member
            auto prev() //
                -> CPP_ret(void)(
                    requires bidirectional_iterator<I>)
            {
                --it_;
            }
            CPP_member
            auto advance(iter_difference_t<I> n) //
                -> CPP_ret(void)(
                    requires random_access_iterator<I>)
            {
                it_ += n;
            }
            CPP_member
            auto distance_to(move_into_cursor const & that) const //
                -> CPP_ret(iter_difference_t<I>)(
                    requires sized_sentinel_for<I, I>)
            {
                return that.it_ - it_;
            }
            template(typename II = I const)(
                requires same_as<I const, II> AND indirectly_readable<II>)
            constexpr iter_rvalue_reference_t<II> move() const //
                noexcept(has_nothrow_iter_move_v<II>)
            {
                return iter_move(it_);
            }

        public:
            constexpr move_into_cursor() = default;
        };
    } // namespace detail
    /// \endcond

    struct move_into_fn
    {
        template<typename I>
        constexpr move_into_iterator<I> operator()(I it) const
        {
            return move_into_iterator<I>{std::move(it)};
        }
    };

    /// \sa `move_into_fn`
    RANGES_INLINE_VARIABLE(move_into_fn, move_into)

    namespace cpp20
    {
        using ranges::make_move_iterator;
        using ranges::move_iterator;
        using ranges::move_sentinel;
    } // namespace cpp20
    /// @}
} // namespace ranges

/// \cond
RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_MISMATCHED_TAGS

namespace std
{
    template<typename I>
    struct iterator_traits<::ranges::move_iterator<I>>
    {
        using iterator_category = std::input_iterator_tag;
        using difference_type = typename ::ranges::move_iterator<I>::difference_type;
        using value_type = typename ::ranges::move_iterator<I>::value_type;
        using reference = typename ::ranges::move_iterator<I>::reference;
        using pointer = meta::_t<std::add_pointer<reference>>;
    };
} // namespace std

RANGES_DIAGNOSTIC_POP
/// \endcond

#include <range/v3/detail/epilogue.hpp>

#endif // RANGES_V3_ITERATOR_MOVE_ITERATORS_HPP
