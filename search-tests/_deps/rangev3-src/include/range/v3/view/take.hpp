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

#ifndef RANGES_V3_VIEW_TAKE_HPP
#define RANGES_V3_VIEW_TAKE_HPP

#include <type_traits>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/min.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/iterator/counted_iterator.hpp>
#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{

    template<typename Rng>
    struct take_view : view_interface<take_view<Rng>, finite>
    {
    private:
        CPP_assert(view_<Rng>);
        Rng base_ = Rng();
        range_difference_t<Rng> count_ = 0;
        template<bool Const>
        struct sentinel
        {
        private:
            using Base = meta::conditional_t<Const, Rng const, Rng>;
            using CI = counted_iterator<iterator_t<Base>>;
            sentinel_t<Base> end_ = sentinel_t<Base>();

        public:
            sentinel() = default;
            constexpr explicit sentinel(sentinel_t<Base> last)
              : end_(std::move(last))
            {}
            template(bool Other)(
                requires Const AND CPP_NOT(Other) AND
                convertible_to<sentinel_t<Rng>,
                               sentinel_t<Base>>)
                constexpr sentinel(sentinel<Other> that)
              : end_(std::move(that.end_))
            {}
            constexpr sentinel_t<Base> base() const
            {
                return end_;
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator==(sentinel const & x, CI const & y)
            {
                return y.count() == 0 || y.base() == x.end_;
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator==(CI const & y, sentinel const & x)
            {
                return y.count() == 0 || y.base() == x.end_;
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator!=(sentinel const & x, CI const & y)
            {
                return y.count() != 0 && y.base() != x.end_;
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator!=(CI const & y, sentinel const & x)
            {
                return y.count() != 0 && y.base() != x.end_;
            }
        };

#if RANGES_CXX_IF_CONSTEXPR < RANGES_CXX_IF_CONSTEXPR_17
        template<typename Take>
        static auto begin_random_access_(Take & take, std::true_type)
        {
            return ranges::begin(take.base_);
        }
        template<typename Take>
        static auto begin_random_access_(Take & take, std::false_type)
        {
            auto s = static_cast<range_difference_t<Rng>>(take.size());
            return make_counted_iterator(ranges::begin(take.base_), s);
        }
        template<typename Take>
        static auto begin_sized_(Take & take, std::true_type)
        {
            return begin_random_access_(
                take, meta::bool_<random_access_range<decltype((take.base_))>>{});
        }
        template<typename Take>
        static auto begin_sized_(Take & take, std::false_type)
        {
            return make_counted_iterator(ranges::begin(take.base_), take.count_);
        }

        template<typename Take>
        static auto end_random_access_(Take & take, std::true_type)
        {
            return ranges::begin(take.base_) +
                   static_cast<range_difference_t<Rng>>(take.size());
        }
        static auto end_random_access_(detail::ignore_t, std::false_type)
        {
            return default_sentinel;
        }
        template<typename Take>
        static auto end_sized_(Take & take, std::true_type, std::false_type) // sized
        {
            return end_random_access_(
                take, meta::bool_<random_access_range<decltype((take.base_))>>{});
        }
        static auto end_sized_(detail::ignore_t, std::false_type,
                               std::true_type) // infinite
        {
            return default_sentinel;
        }
        static auto end_sized_(take_view & take, std::false_type, std::false_type)
        {
            return sentinel<false>{ranges::end(take.base_)};
        }
        static auto end_sized_(take_view const & take, std::false_type, std::false_type)
        {
            return sentinel<true>{ranges::end(take.base_)};
        }
#endif
    public:
        take_view() = default;

        constexpr take_view(Rng base, range_difference_t<Rng> cnt)
          : base_(std::move(base))
          , count_(cnt)
        {}

        constexpr Rng base() const
        {
            return base_;
        }

        CPP_auto_member
        constexpr auto CPP_fun(begin)()(
            requires(!simple_view<Rng>()))
        {
#if RANGES_CXX_IF_CONSTEXPR >= RANGES_CXX_IF_CONSTEXPR_17
            if constexpr(sized_range<Rng>)
                if constexpr(random_access_range<Rng>)
                    return ranges::begin(base_);
                else
                {
                    // cannot always delegate to size() member on GCC with ConceptsTS
#if defined(__cpp_concepts) && __cpp_concepts <= 201507
                    auto s = ranges::min(
                        static_cast<range_difference_t<Rng>>(count_),
                        static_cast<range_difference_t<Rng>>(ranges::size(base_)));
#else
                    auto s = static_cast<range_difference_t<Rng>>(size());
#endif
                    return make_counted_iterator(ranges::begin(base_), s);
                }
            else
                return make_counted_iterator(ranges::begin(base_), count_);
#else
            return begin_sized_(*this, meta::bool_<sized_range<Rng>>{});
#endif
        }

        CPP_auto_member
        constexpr auto CPP_fun(begin)()(const //
            requires range<Rng const>)
        {
#if RANGES_CXX_IF_CONSTEXPR >= RANGES_CXX_IF_CONSTEXPR_17
            if constexpr(sized_range<Rng const>)
                if constexpr(random_access_range<Rng const>)
                    return ranges::begin(base_);
                else
                {
                    auto s = static_cast<range_difference_t<Rng>>(size());
                    return make_counted_iterator(ranges::begin(base_), s);
                }
            else
                return make_counted_iterator(ranges::begin(base_), count_);
#else
            return begin_sized_(*this, meta::bool_<sized_range<Rng const>>{});
#endif
        }

        CPP_auto_member
        constexpr auto CPP_fun(end)()(
            requires(!simple_view<Rng>()))
        {
#if RANGES_CXX_IF_CONSTEXPR >= RANGES_CXX_IF_CONSTEXPR_17
            if constexpr(sized_range<Rng>)
                if constexpr(random_access_range<Rng>)
                    return ranges::begin(base_) +
                           static_cast<range_difference_t<Rng>>(size());
                else
                    return default_sentinel;
            // Not to spec: Infinite ranges:
            else if constexpr(is_infinite<Rng>::value)
                return default_sentinel;
            else
                return sentinel<false>{ranges::end(base_)};
#else
            return end_sized_(*this, meta::bool_<sized_range<Rng>>{}, is_infinite<Rng>{});
#endif
        }

        CPP_auto_member
        constexpr auto CPP_fun(end)()(const //
            requires range<Rng const>)
        {
#if RANGES_CXX_IF_CONSTEXPR >= RANGES_CXX_IF_CONSTEXPR_17
            if constexpr(sized_range<Rng const>)
                if constexpr(random_access_range<Rng const>)
                    return ranges::begin(base_) +
                           static_cast<range_difference_t<Rng>>(size());
                else
                    return default_sentinel;
            // Not to spec: Infinite ranges:
            else if constexpr(is_infinite<Rng const>::value)
                return default_sentinel;
            else
                return sentinel<true>{ranges::end(base_)};
#else
            return end_sized_(
                *this, meta::bool_<sized_range<Rng const>>{}, is_infinite<Rng const>{});
#endif
        }

        CPP_auto_member
        constexpr auto CPP_fun(size)()(
            requires sized_range<Rng>)
        {
            auto n = ranges::size(base_);
            return ranges::min(n, static_cast<decltype(n)>(count_));
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(const //
            requires sized_range<Rng const>)
        {
            auto n = ranges::size(base_);
            return ranges::min(n, static_cast<decltype(n)>(count_));
        }
    };

    template<typename Rng>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<take_view<Rng>> = //
        enable_borrowed_range<Rng>;

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename Rng>
    take_view(Rng &&, range_difference_t<Rng>)
        -> take_view<views::all_t<Rng>>;
#endif

    namespace views
    {
        struct take_base_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng>)
            take_view<all_t<Rng>> operator()(Rng && rng, range_difference_t<Rng> n) const
            {
                return {all(static_cast<Rng &&>(rng)), n};
            }
        };

        struct take_fn : take_base_fn
        {
            using take_base_fn::operator();

            template(typename Int)(
                requires detail::integer_like_<Int>)
            constexpr auto operator()(Int n) const
            {
                return make_view_closure(bind_back(take_base_fn{}, n));
            }
        };

        /// \relates take_fn
        RANGES_INLINE_VARIABLE(take_fn, take)
    } // namespace views

    namespace cpp20
    {
        namespace views
        {
            using ranges::views::take;
        }
        template(typename Rng)(
            requires view_<Rng>)
        using take_view = ranges::take_view<Rng>;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::take_view)

#endif
