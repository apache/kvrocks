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
#ifndef RANGES_V3_ITERATOR_COMMON_ITERATOR_HPP
#define RANGES_V3_ITERATOR_COMMON_ITERATOR_HPP

#include <cstdint>
#include <iterator>
#include <type_traits>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/detail/variant.hpp>
#include <range/v3/iterator/basic_iterator.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/utility/common_tuple.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-iterator
    /// @{

    /// \cond
    namespace detail
    {
        template<typename I, typename S>
        variant<I, S> & cidata(common_iterator<I, S> & that)
        {
            return that.data_;
        }

        template<typename I, typename S>
        variant<I, S> const & cidata(common_iterator<I, S> const & that)
        {
            return that.data_;
        }
    } // namespace detail

#if RANGES_BROKEN_CPO_LOOKUP
    namespace _common_iterator_
    {
        struct adl_hook
        {};
    } // namespace _common_iterator_
#endif
    /// \endcond

    template<typename I, typename S>
    struct common_iterator
#if RANGES_BROKEN_CPO_LOOKUP
      : private _common_iterator_::adl_hook
#endif
    {
    private:
        CPP_assert(input_or_output_iterator<I>);
        CPP_assert(sentinel_for<S, I>);
        CPP_assert(!same_as<I, S>);
        variant<I, S> data_;

        friend variant<I, S> & detail::cidata<>(common_iterator<I, S> &);
        friend variant<I, S> const & detail::cidata<>(common_iterator<I, S> const &);
        struct emplace_fn
        {
            variant<I, S> * data_;
            template<typename T, std::size_t N>
            void operator()(indexed_element<T, N> t) const
            {
                ranges::emplace<N>(*data_, t.get());
            }
        };
        struct arrow_proxy_
        {
        private:
            friend common_iterator;
            iter_value_t<I> keep_;
            arrow_proxy_(iter_reference_t<I> && x)
              : keep_(std::move(x))
            {}

        public:
            const iter_value_t<I> * operator->() const noexcept
            {
                return std::addressof(keep_);
            }
        };
        template<typename T>
        static T * operator_arrow_(T * p, int) noexcept
        {
            return p;
        }
        template<typename J, typename = detail::iter_arrow_t<J const>>
        static J operator_arrow_(J const & j, int) noexcept(noexcept(J(j)))
        {
            return j;
        }
        template(typename J, typename R = iter_reference_t<J>)(
            requires std::is_reference<R>::value) //
        static meta::_t<std::add_pointer<R>> operator_arrow_(J const & j, long) noexcept
        {
            auto && r = *j;
            return std::addressof(r);
        }
        template(typename J, typename V = iter_value_t<J>)(
            requires constructible_from<V, iter_reference_t<J>>)
        static arrow_proxy_ operator_arrow_(J const & j, ...) noexcept(noexcept(V(V(*j))))
        {
            return arrow_proxy_(*j);
        }

    public:
        using difference_type = iter_difference_t<I>;

        common_iterator() = default;
        common_iterator(I i)
          : data_(emplaced_index<0>, std::move(i))
        {}
        common_iterator(S s)
          : data_(emplaced_index<1>, std::move(s))
        {}
        template(typename I2, typename S2)(
            requires convertible_to<I2, I> AND convertible_to<S2, S>)
        common_iterator(common_iterator<I2, S2> const & that)
          : data_(detail::variant_core_access::make_empty<I, S>())
        {
            detail::cidata(that).visit_i(emplace_fn{&data_});
        }
        template(typename I2, typename S2)(
            requires convertible_to<I2, I> AND convertible_to<S2, S>)
        common_iterator & operator=(common_iterator<I2, S2> const & that)
        {
            detail::cidata(that).visit_i(emplace_fn{&data_});
            return *this;
        }
        iter_reference_t<I> operator*() //
            noexcept(noexcept(iter_reference_t<I>(*std::declval<I &>())))
        {
            return *ranges::get<0>(data_);
        }
        CPP_member
        auto operator*() const //
            noexcept(noexcept(iter_reference_t<I>(*std::declval<I const &>())))
            -> CPP_ret(iter_reference_t<I>)(
                requires indirectly_readable<I const>)
        {
            return *ranges::get<0>(data_);
        }
        template(typename J = I)(
            requires indirectly_readable<J>)
        auto operator->() const //
            noexcept(
                noexcept(common_iterator::operator_arrow_(std::declval<I const &>(), 42)))
            -> decltype(common_iterator::operator_arrow_(std::declval<J const &>(), 42))
        {
            return common_iterator::operator_arrow_(ranges::get<0>(data_), 42);
        }
        common_iterator & operator++()
        {
            ++ranges::get<0>(data_);
            return *this;
        }
#ifdef RANGES_WORKAROUND_MSVC_677925
        template(typename I2 = I)(
            requires (!forward_iterator<I2>)) //
        auto operator++(int) //
            -> decltype(std::declval<I2 &>()++)
        {
            return ranges::get<0>(data_)++;
        }
#else  // ^^^ workaround ^^^ / vvv no workaround vvv
        CPP_member
        auto operator++(int) //
            -> CPP_ret(decltype(std::declval<I &>()++))(
                requires (!forward_iterator<I>))
        {
            return ranges::get<0>(data_)++;
        }
#endif // RANGES_WORKAROUND_MSVC_677925
        CPP_member
        auto operator++(int) //
            -> CPP_ret(common_iterator)(
                requires forward_iterator<I>)
        {
            return common_iterator(ranges::get<0>(data_)++);
        }

#if !RANGES_BROKEN_CPO_LOOKUP
        template<typename I_ = I>
        friend constexpr auto iter_move(common_iterator const & i) //
            noexcept(detail::has_nothrow_iter_move_v<I>)
            -> CPP_broken_friend_ret(iter_rvalue_reference_t<I>)(
                requires input_iterator<I_>)
        {
            return ranges::iter_move(ranges::get<0>(detail::cidata(i)));
        }
        template<typename I2, typename S2>
        friend auto iter_swap(
            common_iterator const & x,
            common_iterator<I2, S2> const &
                y) noexcept(is_nothrow_indirectly_swappable<I, I2>::value)
            -> CPP_broken_friend_ret(void)(
                requires indirectly_swappable<I2, I>)
        {
            return ranges::iter_swap(ranges::get<0>(detail::cidata(x)),
                                     ranges::get<0>(detail::cidata(y)));
        }
#endif
    };

    /// \cond
#if RANGES_BROKEN_CPO_LOOKUP
    namespace _common_iterator_
    {
        template<typename I, typename S>
        constexpr auto iter_move(common_iterator<I, S> const & i) noexcept(
            detail::has_nothrow_iter_move_v<I>)
            -> CPP_broken_friend_ret(iter_rvalue_reference_t<I>)(
                requires input_iterator<I>)
        {
            return ranges::iter_move(ranges::get<0>(detail::cidata(i)));
        }
        template<typename I1, typename S1, typename I2, typename S2>
        auto iter_swap(common_iterator<I1, S1> const & x,
                       common_iterator<I2, S2> const & y) //
            noexcept(is_nothrow_indirectly_swappable<I1, I2>::value)
                -> CPP_broken_friend_ret(void)(
                    requires indirectly_swappable<I1, I2>)
        {
            return ranges::iter_swap(ranges::get<0>(detail::cidata(x)),
                                     ranges::get<0>(detail::cidata(y)));
        }
    } // namespace _common_iterator_
#endif
    /// \endcond

    template(typename I1, typename I2, typename S1, typename S2)(
        requires sentinel_for<S1, I2> AND sentinel_for<S2, I1> AND
        (!equality_comparable_with<I1, I2>)) //
    bool operator==(common_iterator<I1, S1> const & x, common_iterator<I2, S2> const & y)
    {
        return detail::cidata(x).index() == 1u ? (detail::cidata(y).index() == 1u ||
                                                  ranges::get<0>(detail::cidata(y)) ==
                                                      ranges::get<1>(detail::cidata(x)))
                                               : (detail::cidata(y).index() != 1u ||
                                                  ranges::get<0>(detail::cidata(x)) ==
                                                      ranges::get<1>(detail::cidata(y)));
    }

    template(typename I1, typename I2, typename S1, typename S2)(
        requires sentinel_for<S1, I2> AND sentinel_for<S2, I1> AND
            equality_comparable_with<I1, I2>)
    bool operator==(common_iterator<I1, S1> const & x, common_iterator<I2, S2> const & y)
    {
        return detail::cidata(x).index() == 1u
                   ? (detail::cidata(y).index() == 1u ||
                      ranges::get<0>(detail::cidata(y)) ==
                          ranges::get<1>(detail::cidata(x)))
                   : (detail::cidata(y).index() == 1u
                          ? ranges::get<0>(detail::cidata(x)) ==
                                ranges::get<1>(detail::cidata(y))
                          : ranges::get<0>(detail::cidata(x)) ==
                                ranges::get<0>(detail::cidata(y)));
    }

    template(typename I1, typename I2, typename S1, typename S2)(
        requires sentinel_for<S1, I2> AND sentinel_for<S2, I1>)
    bool operator!=(common_iterator<I1, S1> const & x, common_iterator<I2, S2> const & y)
    {
        return !(x == y);
    }

    template(typename I1, typename I2, typename S1, typename S2)(
        requires sized_sentinel_for<I1, I2> AND sized_sentinel_for<S1, I2> AND
            sized_sentinel_for<S2, I1>)
    iter_difference_t<I2> operator-(common_iterator<I1, S1> const & x,
                                    common_iterator<I2, S2> const & y)
    {
        return detail::cidata(x).index() == 1u
                   ? (detail::cidata(y).index() == 1u
                          ? 0
                          : ranges::get<1>(detail::cidata(x)) -
                                ranges::get<0>(detail::cidata(y)))
                   : (detail::cidata(y).index() == 1u
                          ? ranges::get<0>(detail::cidata(x)) -
                                ranges::get<1>(detail::cidata(y))
                          : ranges::get<0>(detail::cidata(x)) -
                                ranges::get<0>(detail::cidata(y)));
    }

    template<typename I, typename S>
    struct indirectly_readable_traits<common_iterator<I, S>>
      : meta::if_c<
        (bool)indirectly_readable<I>,
        indirectly_readable_traits<I>,
        meta::nil_>
    {};

    /// \cond
    namespace detail
    {
        template<typename I>
        auto demote_common_iter_cat(...) -> nil_;
        template<typename I>
        auto demote_common_iter_cat(long)
            -> with_iterator_category<std::input_iterator_tag>;
        template(typename I)(
            requires derived_from<typename std::iterator_traits<I>::iterator_category,
                                      std::forward_iterator_tag>)
        auto demote_common_iter_cat(int)
            -> with_iterator_category<std::forward_iterator_tag>;

        template<typename I, bool = (bool)input_iterator<I>>
        struct common_iterator_std_traits : decltype(detail::demote_common_iter_cat<I>(0))
        {
            using difference_type = iter_difference_t<I>;
            using value_type = iter_value_t<I>;
            using reference = iter_reference_t<I>;
            using pointer = detail::iter_pointer_t<I>;
            using iterator_concept =
                meta::conditional_t<(bool)forward_iterator<I>, std::forward_iterator_tag,
                          std::input_iterator_tag>;
        };

        template<typename I>
        struct common_iterator_std_traits<I, false>
        {
            using difference_type = iter_difference_t<I>;
            using value_type = void;
            using reference = void;
            using pointer = void;
            using iterator_category = std::output_iterator_tag;
        };

        // An iterator adaptor that demotes a user-defined difference_type to
        // std::intmax_t, for use when constructing containers from such
        // iterators.
        template<typename I>
        struct cpp17_iterator_cursor
        {
        private:
            friend range_access;
            I it_;
            struct mixin : basic_mixin<cpp17_iterator_cursor>
            {
                mixin() = default;
                #ifndef _MSC_VER
                using basic_mixin<cpp17_iterator_cursor>::basic_mixin;
                #else
                constexpr explicit mixin(cpp17_iterator_cursor && cur)
                  : basic_mixin<cpp17_iterator_cursor>(
                        static_cast<cpp17_iterator_cursor &&>(cur))
                {}
                constexpr explicit mixin(cpp17_iterator_cursor const & cur)
                  : basic_mixin<cpp17_iterator_cursor>(cur)
                {}
                #endif
                explicit mixin(I it)
                  : mixin{cpp17_iterator_cursor{std::move(it)}}
                {}
                I base() const
                {
                    return this->get().it_;
                }
            };

        public:
            using single_pass = meta::bool_<!forward_iterator<I>>;
            using difference_type = std::ptrdiff_t;
            using value_type = iter_value_t<I>;

            cpp17_iterator_cursor() = default;
            constexpr explicit cpp17_iterator_cursor(I i)
              : it_(static_cast<I &&>(i))
            {}

            I arrow() const
            {
                return it_;
            }
            decltype(auto) read()
            {
                return *it_;
            }
            decltype(auto) read() const
            {
                return *it_;
            }
            void next()
            {
                ++it_;
            }
            bool equal(cpp17_iterator_cursor const & that) const
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
            auto advance(std::ptrdiff_t n) //
                -> CPP_ret(void)(
                    requires random_access_iterator<I>)
            {
                it_ += static_cast<iter_difference_t<I>>(n);
            }
            CPP_member
            auto distance_to(cpp17_iterator_cursor const & that) //
                -> CPP_ret(std::ptrdiff_t)(
                    requires random_access_iterator<I>)
            {
                auto d = that.it_ - it_;
                RANGES_EXPECT(d <= PTRDIFF_MAX);
                return static_cast<std::ptrdiff_t>(d);
            }
        };
    } // namespace detail
    /// \endcond

    namespace cpp20
    {
        using ranges::common_iterator;
    }
    /// @}
} // namespace ranges

/// \cond
RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_MISMATCHED_TAGS

namespace std
{
    template<typename I, typename S>
    struct iterator_traits<::ranges::common_iterator<I, S>>
      : ::ranges::detail::common_iterator_std_traits<I>
    {};
} // namespace std

RANGES_DIAGNOSTIC_POP
/// \endcond

#include <range/v3/detail/epilogue.hpp>

#endif
