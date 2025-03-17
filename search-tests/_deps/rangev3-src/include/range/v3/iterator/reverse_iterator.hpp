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
#ifndef RANGES_V3_ITERATOR_REVERSE_ITERATOR_HPP
#define RANGES_V3_ITERATOR_REVERSE_ITERATOR_HPP

#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/basic_iterator.hpp>
#include <range/v3/iterator/concepts.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-iterator
    /// @{

    /// \cond
    namespace detail
    {
        template<typename I>
        struct reverse_cursor
        {
        private:
            CPP_assert(bidirectional_iterator<I>);
            friend range_access;

            using value_type = iter_value_t<I>;

            template<typename OtherI>
            friend struct reverse_cursor;
            struct mixin : basic_mixin<reverse_cursor>
            {
                mixin() = default;
                #ifndef _MSC_VER
                using basic_mixin<reverse_cursor>::basic_mixin;
                #else
                constexpr explicit mixin(reverse_cursor && cur)
                  : basic_mixin<reverse_cursor>(static_cast<reverse_cursor &&>(cur))
                {}
                constexpr explicit mixin(reverse_cursor const & cur)
                  : basic_mixin<reverse_cursor>(cur)
                {}
                #endif
                constexpr mixin(I it)
                  : mixin{reverse_cursor{it}}
                {}
                constexpr I base() const
                {
                    return this->get().base();
                }
            };

            I it_;

            constexpr reverse_cursor(I it)
              : it_(std::move(it))
            {}
            constexpr iter_reference_t<I> read() const
            {
                return *arrow();
            }
            constexpr I arrow() const
            {
                auto tmp = it_;
                --tmp;
                return tmp;
            }
            constexpr I base() const
            {
                return it_;
            }
            template(typename J)(
                requires sentinel_for<J, I>)
            constexpr bool equal(reverse_cursor<J> const & that) const
            {
                return it_ == that.it_;
            }
            constexpr void next()
            {
                --it_;
            }
            constexpr void prev()
            {
                ++it_;
            }
            CPP_member
            constexpr auto advance(iter_difference_t<I> n) //
                -> CPP_ret(void)(
                    requires random_access_iterator<I>)
            {
                it_ -= n;
            }
            template(typename J)(
                requires sized_sentinel_for<J, I>)
            constexpr iter_difference_t<I> distance_to(reverse_cursor<J> const & that) //
                const
            {
                return it_ - that.base();
            }
            constexpr iter_rvalue_reference_t<I> move() const
                noexcept(noexcept((void)I(I(it_)), (void)--const_cast<I &>(it_),
                                  iter_move(it_)))
            {
                auto tmp = it_;
                --tmp;
                return iter_move(tmp);
            }

        public:
            reverse_cursor() = default;
            template(typename U)(
                requires convertible_to<U, I>)
            constexpr reverse_cursor(reverse_cursor<U> const & u)
              : it_(u.base())
            {}
        };
    } // namespace detail
    /// \endcond

    struct make_reverse_iterator_fn
    {
        template(typename I)(
            requires bidirectional_iterator<I>)
        constexpr reverse_iterator<I> operator()(I i) const
        {
            return reverse_iterator<I>(i);
        }
    };

    RANGES_INLINE_VARIABLE(make_reverse_iterator_fn, make_reverse_iterator)

    namespace cpp20
    {
        using ranges::make_reverse_iterator;
        using ranges::reverse_iterator;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif // RANGES_V3_ITERATOR_REVERSE_ITERATOR_HPP
