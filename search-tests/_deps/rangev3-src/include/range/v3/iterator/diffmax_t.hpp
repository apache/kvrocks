/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2019-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_ITERATOR_DIFFMAX_T_HPP
#define RANGES_V3_ITERATOR_DIFFMAX_T_HPP

#include <cstdint>
#include <iosfwd>
#include <limits>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/concepts.hpp>

#include <range/v3/detail/prologue.hpp>

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_UNSIGNED_MATH

namespace ranges
{
    /// \cond
    namespace detail
    {
        struct diffmax_t
        {
        private:
            bool neg_;
            std::uintmax_t val_;
            struct tag
            {};

            constexpr diffmax_t(tag, bool neg, std::uintmax_t val)
              : neg_(val && neg)
              , val_(val)
            {}

            /// \cond
            constexpr void _check()
            {
                RANGES_ENSURE(!neg_ || val_);
            }
            static constexpr diffmax_t _normalize(bool neg, std::uintmax_t val)
            {
                return diffmax_t{tag{}, val && neg, val};
            }
            /// \endcond

        public:
            diffmax_t() = default;

            template(typename T)(
                requires integral<T>)
            constexpr diffmax_t(T val) noexcept
              : neg_(0 > val)
              , val_(0 > val ? static_cast<std::uintmax_t>(-val)
                             : static_cast<std::uintmax_t>(val))
            {}

            friend constexpr bool operator<(diffmax_t a, diffmax_t b) noexcept
            {
                a._check();
                b._check();
                return a.neg_ ? (b.neg_ ? a.val_ > b.val_ : true)
                              : (b.neg_ ? false : a.val_ < b.val_);
            }
            friend constexpr bool operator>(diffmax_t a, diffmax_t b) noexcept
            {
                return b < a;
            }
            friend constexpr bool operator<=(diffmax_t a, diffmax_t b) noexcept
            {
                return !(b < a);
            }
            friend constexpr bool operator>=(diffmax_t a, diffmax_t b) noexcept
            {
                return !(a < b);
            }
            friend constexpr bool operator==(diffmax_t a, diffmax_t b) noexcept
            {
                a._check();
                b._check();
                return a.val_ == b.val_ && a.neg_ == b.neg_;
            }
            friend constexpr bool operator!=(diffmax_t a, diffmax_t b) noexcept
            {
                return !(a == b);
            }

            friend constexpr diffmax_t operator+(diffmax_t a) noexcept
            {
                return a;
            }
            friend constexpr diffmax_t operator-(diffmax_t a) noexcept
            {
                return _normalize(!a.neg_, a.val_);
            }

            friend constexpr diffmax_t operator+(diffmax_t a, diffmax_t b) noexcept
            {
                return a.neg_ == b.neg_
                           ? diffmax_t{tag{}, a.neg_, a.val_ + b.val_}
                           : (a.neg_ ? (a.val_ > b.val_
                                            ? diffmax_t{tag{}, true, a.val_ - b.val_}
                                            : diffmax_t{tag{}, false, b.val_ - a.val_})
                                     : (b.val_ > a.val_
                                            ? diffmax_t{tag{}, true, b.val_ - a.val_}
                                            : diffmax_t{tag{}, false, a.val_ - b.val_}));
            }
            friend constexpr diffmax_t operator-(diffmax_t a, diffmax_t b) noexcept
            {
                return a + -b;
            }
            friend constexpr diffmax_t operator*(diffmax_t a, diffmax_t b) noexcept
            {
                return _normalize(a.neg_ ^ b.neg_, a.val_ * b.val_);
            }
            friend constexpr diffmax_t operator/(diffmax_t a, diffmax_t b) noexcept
            {
                return _normalize(a.neg_ ^ b.neg_, a.val_ / b.val_);
            }
            friend constexpr diffmax_t operator%(diffmax_t a, diffmax_t b) noexcept
            {
                return _normalize(a.neg_, a.val_ % b.val_);
            }
            static constexpr std::uintmax_t compl_if(bool neg,
                                                     std::uintmax_t val) noexcept
            {
                return neg ? ~val + 1 : val;
            }
            friend constexpr diffmax_t operator&(diffmax_t a, diffmax_t b) noexcept
            {
                return _normalize(
                    a.neg_ && b.neg_,
                    compl_if(a.neg_ && b.neg_,
                             compl_if(a.neg_, a.val_) & compl_if(b.neg_, b.val_)));
            }
            friend constexpr diffmax_t operator|(diffmax_t a, diffmax_t b) noexcept
            {
                return _normalize(
                    a.neg_ || b.neg_,
                    compl_if(a.neg_ || b.neg_,
                             compl_if(a.neg_, a.val_) | compl_if(b.neg_, b.val_)));
            }
            friend constexpr diffmax_t operator^(diffmax_t a, diffmax_t b) noexcept
            {
                return _normalize(
                    bool(a.neg_ ^ b.neg_),
                    compl_if(bool(a.neg_ ^ b.neg_),
                             compl_if(a.neg_, a.val_) ^ compl_if(b.neg_, b.val_)));
            }

            friend constexpr diffmax_t operator<<(diffmax_t a, diffmax_t b) noexcept
            {
                RANGES_ENSURE(!a.neg_);
                return b.neg_ ? diffmax_t{tag{}, false, a.val_ >> b.val_}
                              : diffmax_t{tag{}, false, a.val_ << b.val_};
            }
            friend constexpr diffmax_t operator>>(diffmax_t a, diffmax_t b) noexcept
            {
                return b.neg_ ? diffmax_t{tag{}, a.neg_, a.val_ << b.val_}
                              : diffmax_t{tag{}, a.neg_, a.val_ >> b.val_};
            }

            friend constexpr diffmax_t & operator+=(diffmax_t & a, diffmax_t b) noexcept
            {
                return (a = a + b);
            }
            friend constexpr diffmax_t & operator-=(diffmax_t & a, diffmax_t b) noexcept
            {
                return (a = a - b);
            }
            friend constexpr diffmax_t & operator*=(diffmax_t & a, diffmax_t b) noexcept
            {
                return (a = a * b);
            }
            friend constexpr diffmax_t & operator/=(diffmax_t & a, diffmax_t b) noexcept
            {
                return (a = a / b);
            }
            friend constexpr diffmax_t & operator%=(diffmax_t & a, diffmax_t b) noexcept
            {
                return (a = a % b);
            }
            friend constexpr diffmax_t & operator&=(diffmax_t & a, diffmax_t b) noexcept
            {
                return (a = a & b);
            }
            friend constexpr diffmax_t & operator|=(diffmax_t & a, diffmax_t b) noexcept
            {
                return (a = a | b);
            }
            friend constexpr diffmax_t & operator^=(diffmax_t & a, diffmax_t b) noexcept
            {
                return (a = a ^ b);
            }
            friend constexpr diffmax_t & operator<<=(diffmax_t & a, diffmax_t b) noexcept
            {
                a = (a << b);
                return a;
            }
            friend constexpr diffmax_t & operator>>=(diffmax_t & a, diffmax_t b) noexcept
            {
                a = (a >> b);
                return a;
            }

            template<typename T>
            friend constexpr auto operator+=(T & a, diffmax_t b) noexcept
                -> CPP_broken_friend_ret(T &)(
                    requires integral<T>)
            {
                return (a = static_cast<T>(diffmax_t{a} + b));
            }
            template<typename T>
            friend constexpr auto operator-=(T & a, diffmax_t b) noexcept
                -> CPP_broken_friend_ret(T &)(
                    requires integral<T>)
            {
                return (a = static_cast<T>(diffmax_t{a} - b));
            }
            template<typename T>
            friend constexpr auto operator*=(T & a, diffmax_t b) noexcept
                -> CPP_broken_friend_ret(T &)(
                    requires integral<T>)
            {
                return (a = static_cast<T>(diffmax_t{a} * b));
            }
            template<typename T>
            friend constexpr auto operator/=(T & a, diffmax_t b) noexcept
                -> CPP_broken_friend_ret(T &)(
                    requires integral<T>)
            {
                return (a = static_cast<T>(diffmax_t{a} / b));
            }
            template<typename T>
            friend constexpr auto operator%=(T & a, diffmax_t b) noexcept
                -> CPP_broken_friend_ret(T &)(
                    requires integral<T>)
            {
                return (a = static_cast<T>(diffmax_t{a} % b));
            }
            template<typename T>
            friend constexpr auto operator&=(T & a, diffmax_t b) noexcept
                -> CPP_broken_friend_ret(T &)(
                    requires integral<T>)
            {
                return (a = static_cast<T>(diffmax_t{a} & b));
            }
            template<typename T>
            friend constexpr auto operator|=(T & a, diffmax_t b) noexcept
                -> CPP_broken_friend_ret(T &)(
                    requires integral<T>)
            {
                return (a = static_cast<T>(diffmax_t{a} | b));
            }
            template<typename T>
            friend constexpr auto operator^=(T & a, diffmax_t b) noexcept
                -> CPP_broken_friend_ret(T &)(
                    requires integral<T>)
            {
                return (a = static_cast<T>(diffmax_t{a} ^ b));
            }
            template<typename T>
            friend constexpr auto operator<<=(T & a, diffmax_t b) noexcept
                -> CPP_broken_friend_ret(T &)(
                    requires integral<T>)
            {
                a = static_cast<T>(diffmax_t{a} << b);
                return a;
            }
            template<typename T>
            friend constexpr auto operator>>=(T & a, diffmax_t b) noexcept
                -> CPP_broken_friend_ret(T &)(
                    requires integral<T>)
            {
                a = static_cast<T>(diffmax_t{a} >> b);
                return a;
            }

            friend constexpr diffmax_t & operator++(diffmax_t & a) noexcept
            {
                a.neg_ = (a.neg_ ? --a.val_ : ++a.val_) && a.neg_;
                return a;
            }
            friend constexpr diffmax_t & operator--(diffmax_t & a) noexcept
            {
                a.neg_ = (a.neg_ ? ++a.val_ : --a.val_) && a.neg_;
                return a;
            }
            friend constexpr diffmax_t operator++(diffmax_t & a, int) noexcept
            {
                auto tmp = a;
                ++a;
                return tmp;
            }
            friend constexpr diffmax_t operator--(diffmax_t & a, int) noexcept
            {
                auto tmp = a;
                --a;
                return tmp;
            }

            template(typename T)(
                requires integral<T>)
                constexpr explicit
                operator T() const noexcept
            {
                return neg_ ? -static_cast<T>(val_) : static_cast<T>(val_);
            }
            constexpr explicit operator bool() const noexcept
            {
                return val_ != 0;
            }
            constexpr bool operator!() const noexcept
            {
                return val_ == 0;
            }

            template<typename Ostream>
            friend auto operator<<(Ostream & sout, diffmax_t a)
                -> CPP_broken_friend_ret(std::ostream &)(
                    requires derived_from<
                        Ostream, std::basic_ostream<typename Ostream::char_type,
                                                    typename Ostream::traits_type>>)
            {
                return sout << (&"-"[!a.neg_]) << a.val_;
            }
        };

#if RANGES_CXX_INLINE_VARIABLES >= RANGES_CXX_INLINE_VARIABLES_17
        template<>
        inline constexpr bool _is_integer_like_<diffmax_t> = true;
#else
        template<typename Enable>
        constexpr bool _is_integer_like_<diffmax_t, Enable> = true;
#endif
    } // namespace detail
    /// \endcond
} // namespace ranges

/// \cond
RANGES_DIAGNOSTIC_IGNORE_MISMATCHED_TAGS

namespace std
{
    template<>
    struct numeric_limits<::ranges::detail::diffmax_t>
    {
        static constexpr bool is_specialized = true;
        static constexpr bool is_signed = true;
        static constexpr bool is_integer = true;
        static constexpr bool is_exact = true;
        static constexpr bool has_infinity = false;
        static constexpr bool has_quiet_NaN = false;
        static constexpr bool has_signaling_NaN = false;
        static constexpr bool has_denorm = false;
        static constexpr bool has_denorm_loss = false;
        static constexpr std::float_round_style round_style = std::round_toward_zero;
        static constexpr bool is_iec559 = false;
        static constexpr bool is_bounded = true;
        static constexpr bool is_modulo = false;
        static constexpr int digits = CHAR_BIT * sizeof(std::uintmax_t) + 1;
        static constexpr int digits10 =
            static_cast<int>(digits * 0.301029996); // digits * std::log10(2)
        static constexpr int max_digits10 = 0;
        static constexpr int radix = 2;
        static constexpr int min_exponent = 0;
        static constexpr int min_exponent10 = 0;
        static constexpr int max_exponent = 0;
        static constexpr int max_exponent10 = 0;
        static constexpr bool traps = true;
        static constexpr bool tinyness_before = false;

        static constexpr ::ranges::detail::diffmax_t max() noexcept
        {
            return std::uintmax_t(-1);
        }
        static constexpr ::ranges::detail::diffmax_t min() noexcept
        {
            return -max();
        }
        static constexpr ::ranges::detail::diffmax_t lowest() noexcept
        {
            return min();
        }
        static constexpr ::ranges::detail::diffmax_t epsilon() noexcept
        {
            return 0;
        }
        static constexpr ::ranges::detail::diffmax_t round_error() noexcept
        {
            return 0;
        }
        static constexpr ::ranges::detail::diffmax_t infinity() noexcept
        {
            return 0;
        }
        static constexpr ::ranges::detail::diffmax_t quiet_NaN() noexcept
        {
            return 0;
        }
        static constexpr ::ranges::detail::diffmax_t signaling_NaN() noexcept
        {
            return 0;
        }
        static constexpr ::ranges::detail::diffmax_t denorm_min() noexcept
        {
            return 0;
        }
    };
    template<>
    struct numeric_limits<::ranges::detail::diffmax_t const>
      : numeric_limits<::ranges::detail::diffmax_t>
    {};
    template<>
    struct numeric_limits<::ranges::detail::diffmax_t volatile>
      : numeric_limits<::ranges::detail::diffmax_t>
    {};
    template<>
    struct numeric_limits<::ranges::detail::diffmax_t const volatile>
      : numeric_limits<::ranges::detail::diffmax_t>
    {};

#if RANGES_CXX_INLINE_VARIABLES >= RANGES_CXX_INLINE_VARIABLES_17
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::is_specialized;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::is_signed;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::is_integer;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::is_exact;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::has_infinity;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::has_quiet_NaN;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::has_signaling_NaN;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::has_denorm;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::has_denorm_loss;
    inline constexpr std::float_round_style
        numeric_limits<::ranges::detail::diffmax_t>::round_style;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::is_iec559;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::is_bounded;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::is_modulo;
    inline constexpr int numeric_limits<::ranges::detail::diffmax_t>::digits;
    inline constexpr int numeric_limits<::ranges::detail::diffmax_t>::digits10;
    inline constexpr int numeric_limits<::ranges::detail::diffmax_t>::max_digits10;
    inline constexpr int numeric_limits<::ranges::detail::diffmax_t>::radix;
    inline constexpr int numeric_limits<::ranges::detail::diffmax_t>::min_exponent;
    inline constexpr int numeric_limits<::ranges::detail::diffmax_t>::min_exponent10;
    inline constexpr int numeric_limits<::ranges::detail::diffmax_t>::max_exponent;
    inline constexpr int numeric_limits<::ranges::detail::diffmax_t>::max_exponent10;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::traps;
    inline constexpr bool numeric_limits<::ranges::detail::diffmax_t>::tinyness_before;
#endif
} // namespace std
/// \endcond

RANGES_DIAGNOSTIC_POP

#include <range/v3/detail/epilogue.hpp>

#endif
