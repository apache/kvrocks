/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_DETAIL_RANGE_ACCESS_HPP
#define RANGES_V3_DETAIL_RANGE_ACCESS_HPP

#include <cstddef>
#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/concepts.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    struct range_access
    {
        /// \cond
    private:
        template<typename T>
        static std::false_type single_pass_2_(long);
        template<typename T>
        static typename T::single_pass single_pass_2_(int);

        template<typename T>
        struct single_pass_
        {
            using type = decltype(range_access::single_pass_2_<T>(42));
        };

        template<typename T>
        static std::false_type contiguous_2_(long);
        template<typename T>
        static typename T::contiguous contiguous_2_(int);

        template<typename T>
        struct contiguous_
        {
            using type = decltype(range_access::contiguous_2_<T>(42));
        };

        template<typename T>
        static basic_mixin<T> mixin_base_2_(long);
        template<typename T>
        static typename T::mixin mixin_base_2_(int);

        template<typename Cur>
        struct mixin_base_
        {
            using type = decltype(range_access::mixin_base_2_<Cur>(42));
        };


    public:
        template<typename Cur>
        using single_pass_t = meta::_t<single_pass_<Cur>>;

        template<typename Cur>
        using contiguous_t = meta::_t<contiguous_<Cur>>;

        template<typename Cur>
        using mixin_base_t = meta::_t<mixin_base_<Cur>>;

        // clang-format off
        template<typename Rng>
        static constexpr auto CPP_auto_fun(begin_cursor)(Rng &rng)
        (
            return rng.begin_cursor()
        )
        template<typename Rng>
        static constexpr auto CPP_auto_fun(end_cursor)(Rng &rng)
        (
            return rng.end_cursor()
        )

        template<typename Rng>
        static constexpr auto CPP_auto_fun(begin_adaptor)(Rng &rng)
        (
            return rng.begin_adaptor()
        )
        template<typename Rng>
        static constexpr auto CPP_auto_fun(end_adaptor)(Rng &rng)
        (
            return rng.end_adaptor()
        )

        template<typename Cur>
        static constexpr auto CPP_auto_fun(read)(Cur const &pos)
        (
            return pos.read()
        )
        template<typename Cur>
        static constexpr auto CPP_auto_fun(arrow)(Cur const &pos)
        (
            return pos.arrow()
        )
        template<typename Cur>
        static constexpr auto CPP_auto_fun(move)(Cur const &pos)
        (
            return pos.move()
        )
        template<typename Cur, typename T>
        static constexpr auto CPP_auto_fun(write)(Cur &pos, T &&t)
        (
            return pos.write((T &&) t)
        )
        template<typename Cur>
        static constexpr auto CPP_auto_fun(next)(Cur & pos)
        (
            return pos.next()
        )
        template<typename Cur, typename O>
        static constexpr auto CPP_auto_fun(equal)(Cur const &pos, O const &other)
        (
            return pos.equal(other)
        )
        template<typename Cur>
        static constexpr auto CPP_auto_fun(prev)(Cur & pos)
        (
            return pos.prev()
        )
        template<typename Cur, typename D>
        static constexpr auto CPP_auto_fun(advance)(Cur & pos, D n)
        (
            return pos.advance(n)
        )
        template<typename Cur, typename O>
        static constexpr auto CPP_auto_fun(distance_to)(Cur const &pos, O const &other)
        (
            return pos.distance_to(other)
        )

    private:
        template<typename Cur>
        using sized_cursor_difference_t = decltype(
            range_access::distance_to(std::declval<Cur>(), std::declval<Cur>()));
        // clang-format on

        template<typename T>
        static std::ptrdiff_t cursor_difference_2_(detail::ignore_t);
        template<typename T>
        static sized_cursor_difference_t<T> cursor_difference_2_(long);
        template<typename T>
        static typename T::difference_type cursor_difference_2_(int);

        template<typename T>
        using cursor_reference_t = decltype(std::declval<T const &>().read());

        template<typename T>
        static meta::id<uncvref_t<cursor_reference_t<T>>> cursor_value_2_(long);
        template<typename T>
        static meta::id<typename T::value_type> cursor_value_2_(int);

#ifdef RANGES_WORKAROUND_CWG_1554
        template<typename Cur>
        struct cursor_difference
        {
            using type = decltype(range_access::cursor_difference_2_<Cur>(42));
        };

        template<typename Cur>
        struct cursor_value : decltype(range_access::cursor_value_2_<Cur>(42))
        {};
#endif // RANGES_WORKAROUND_CWG_1554
    public:
#ifdef RANGES_WORKAROUND_CWG_1554
        template<typename Cur>
        using cursor_difference_t = meta::_t<cursor_difference<Cur>>;

        template<typename Cur>
        using cursor_value_t = meta::_t<cursor_value<Cur>>;
#else  // ^^^ workaround ^^^ / vvv no workaround vvv
        template<typename Cur>
        using cursor_difference_t = decltype(range_access::cursor_difference_2_<Cur>(42));

        template<typename Cur>
        using cursor_value_t = meta::_t<decltype(range_access::cursor_value_2_<Cur>(42))>;
#endif // RANGES_WORKAROUND_CWG_1554

        template<typename Cur>
        static constexpr Cur & pos(basic_iterator<Cur> & it) noexcept
        {
            return it.pos();
        }
        template<typename Cur>
        static constexpr Cur const & pos(basic_iterator<Cur> const & it) noexcept
        {
            return it.pos();
        }
        template<typename Cur>
        static constexpr Cur && pos(basic_iterator<Cur> && it) noexcept
        {
            return detail::move(it.pos());
        }

        template<typename Cur>
        static constexpr Cur cursor(basic_iterator<Cur> it)
        {
            return std::move(it.pos());
        }
        /// endcond
    };
    /// @}

    /// \cond
    namespace detail
    {
        //
        // Concepts that the range cursor must model
        // clang-format off
        //
        /// \concept cursor
        /// \brief The \c cursor concept
        template<typename T>
        CPP_concept cursor =
            semiregular<T> && semiregular<range_access::mixin_base_t<T>> &&
            constructible_from<range_access::mixin_base_t<T>, T> &&
            constructible_from<range_access::mixin_base_t<T>, T const &>;
            // Axiom: mixin_base_t<T> has a member get(), accessible to derived classes,
            //   which perfectly-returns the contained cursor object and does not throw
            //   exceptions.

        /// \concept has_cursor_next_
        /// \brief The \c has_cursor_next_ concept
        template<typename T>
        CPP_requires(has_cursor_next_,
            requires(T & t)
            (
                range_access::next(t)
            ));
        /// \concept has_cursor_next
        /// \brief The \c has_cursor_next concept
        template<typename T>
        CPP_concept has_cursor_next = CPP_requires_ref(detail::has_cursor_next_, T);

        /// \concept sentinel_for_cursor_
        /// \brief The \c sentinel_for_cursor_ concept
        template<typename S, typename C>
        CPP_requires(sentinel_for_cursor_,
            requires(S & s, C & c) //
            (
                range_access::equal(c, s),
                concepts::requires_<convertible_to<decltype(
                    range_access::equal(c, s)), bool>>
            ));
        /// \concept sentinel_for_cursor
        /// \brief The \c sentinel_for_cursor concept
        template<typename S, typename C>
        CPP_concept sentinel_for_cursor =
            semiregular<S> &&
            cursor<C> &&
            CPP_requires_ref(detail::sentinel_for_cursor_, S, C);

        /// \concept readable_cursor_
        /// \brief The \c readable_cursor_ concept
        template<typename T>
        CPP_requires(readable_cursor_,
            requires(T & t) //
            (
                range_access::read(t)
            ));
        /// \concept readable_cursor
        /// \brief The \c readable_cursor concept
        template<typename T>
        CPP_concept readable_cursor = CPP_requires_ref(detail::readable_cursor_, T);

        /// \concept has_cursor_arrow_
        /// \brief The \c has_cursor_arrow_ concept
        template<typename T>
        CPP_requires(has_cursor_arrow_,
            requires(T const & t) //
            (
                range_access::arrow(t)
            ));
        /// \concept has_cursor_arrow
        /// \brief The \c has_cursor_arrow concept
        template<typename T>
        CPP_concept has_cursor_arrow = CPP_requires_ref(detail::has_cursor_arrow_, T);

        /// \concept writable_cursor_
        /// \brief The \c writable_cursor_ concept
        template<typename T, typename U>
        CPP_requires(writable_cursor_,
            requires(T & t, U && u) //
            (
                range_access::write(t, (U &&) u)
            ));
        /// \concept writable_cursor
        /// \brief The \c writable_cursor concept
        template<typename T, typename U>
        CPP_concept writable_cursor =
            CPP_requires_ref(detail::writable_cursor_, T, U);

        /// \concept sized_sentinel_for_cursor_
        /// \brief The \c sized_sentinel_for_cursor_ concept
        template<typename S, typename C>
        CPP_requires(sized_sentinel_for_cursor_,
            requires(S & s, C & c) //
            (
                range_access::distance_to(c, s),
                concepts::requires_<signed_integer_like_<decltype(
                    range_access::distance_to(c, s))>>
            )
        );
        /// \concept sized_sentinel_for_cursor
        /// \brief The \c sized_sentinel_for_cursor concept
        template<typename S, typename C>
        CPP_concept sized_sentinel_for_cursor =
            sentinel_for_cursor<S, C> &&
            CPP_requires_ref(detail::sized_sentinel_for_cursor_, S, C);

        /// \concept output_cursor
        /// \brief The \c output_cursor concept
        template<typename T, typename U>
        CPP_concept output_cursor =
            writable_cursor<T, U> && cursor<T>;

        /// \concept input_cursor
        /// \brief The \c input_cursor concept
        template<typename T>
        CPP_concept input_cursor =
            readable_cursor<T> && cursor<T> && has_cursor_next<T>;

        /// \concept forward_cursor
        /// \brief The \c forward_cursor concept
        template<typename T>
        CPP_concept forward_cursor =
            input_cursor<T> && sentinel_for_cursor<T, T> &&
            !range_access::single_pass_t<uncvref_t<T>>::value;

        /// \concept bidirectional_cursor_
        /// \brief The \c bidirectional_cursor_ concept
        template<typename T>
        CPP_requires(bidirectional_cursor_,
            requires(T & t) //
            (
                range_access::prev(t)
            ));
        /// \concept bidirectional_cursor
        /// \brief The \c bidirectional_cursor concept
        template<typename T>
        CPP_concept bidirectional_cursor =
            forward_cursor<T> &&
            CPP_requires_ref(detail::bidirectional_cursor_, T);

        /// \concept random_access_cursor_
        /// \brief The \c random_access_cursor_ concept
        template<typename T>
        CPP_requires(random_access_cursor_,
            requires(T & t) //
            (
                range_access::advance(t, range_access::distance_to(t, t))
            ));
        /// \concept random_access_cursor
        /// \brief The \c random_access_cursor concept
        template<typename T>
        CPP_concept random_access_cursor =
            bidirectional_cursor<T> && //
            sized_sentinel_for_cursor<T, T> && //
            CPP_requires_ref(detail::random_access_cursor_, T);

        template(class T)(
            requires std::is_lvalue_reference<T>::value)
        void is_lvalue_reference(T&&);

        /// \concept contiguous_cursor_
        /// \brief The \c contiguous_cursor_ concept
        template<typename T>
        CPP_requires(contiguous_cursor_,
            requires(T & t) //
            (
                detail::is_lvalue_reference(range_access::read(t))
            ));
        /// \concept contiguous_cursor
        /// \brief The \c contiguous_cursor concept
        template<typename T>
        CPP_concept contiguous_cursor =
            random_access_cursor<T> && //
            range_access::contiguous_t<uncvref_t<T>>::value && //
            CPP_requires_ref(detail::contiguous_cursor_, T);
        // clang-format on

        template<typename Cur, bool IsReadable>
        RANGES_INLINE_VAR constexpr bool is_writable_cursor_ = true;

        template<typename Cur>
        RANGES_INLINE_VAR constexpr bool is_writable_cursor_<Cur, true> =
            (bool) writable_cursor<Cur, range_access::cursor_value_t<Cur>>;

        template<typename Cur>
        RANGES_INLINE_VAR constexpr bool is_writable_cursor_v =
            is_writable_cursor_<Cur, (bool)readable_cursor<Cur>>;
    } // namespace detail
    /// \endcond
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
