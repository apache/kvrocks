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

#ifndef RANGES_V3_VIEW_SPLIT_HPP
#define RANGES_V3_VIEW_SPLIT_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/mismatch.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/interface.hpp>
#include <range/v3/view/single.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{

    /// \cond
    namespace detail
    {
        // clang-format off
#if defined(_MSC_VER) && !defined(__clang__) && \
    RANGES_CXX_VER <= RANGES_CXX_STD_17
        template<typename R, std::size_t Sz = static_cast<std::size_t>(R::size())>
        constexpr bool _is_tiny_range_(R const *) noexcept
        {
            return R::size() <= 1u;
        }
        constexpr bool _is_tiny_range_(void const*) noexcept
        {
            return false;
        }
        /// \concept tiny_range
        /// \brief The \c tiny_range concept
        template<typename R>
        CPP_concept tiny_range =
            sized_range<R> &&
            detail::_is_tiny_range_(static_cast<std::add_pointer_t<R>>(nullptr));
#else // ^^^^ workaround / no workaround vvvv
        /// \concept tiny_range_
        /// \brief The \c tiny_range_ concept
        template(typename R)(
        concept (tiny_range_)(R),
            ranges::type<
                std::integral_constant<decltype(R::size()), R::size()>> AND
            (R::size() <= 1)
        );
        /// \concept tiny_range
        /// \brief The \c tiny_range concept
        template<typename R>
        CPP_concept tiny_range =
            sized_range<R> &&
            CPP_concept_ref(detail::tiny_range_, std::remove_reference_t<R>);
#endif
        // clang-format on
    } // namespace detail

    template<typename V, typename Pattern>
#if CPP_CXX_CONCEPTS
        requires input_range<V> && forward_range<Pattern> && view_<V> && view_<
            Pattern> && indirectly_comparable<iterator_t<V>, iterator_t<Pattern>,
                                              ranges::equal_to> &&
        (forward_range<V> || detail::tiny_range<Pattern>)
#endif
            struct split_view;

    namespace detail
    {
        struct there
        {
            template<typename T>
            static decltype(auto) current_(T & t) noexcept
            {
                return (t.curr_);
            }
        };

        template<typename It>
        struct here
        {
            It curr_ = It();
            It & current_(ignore_t) noexcept
            {
                return curr_;
            }
            It const & current_(ignore_t) const noexcept
            {
                return curr_;
            }
        };

        template<bool>
        struct here_or_there_
        {
            template<typename>
            using invoke = there;
        };

        template<>
        struct here_or_there_<true>
        {
            template<typename It>
            using invoke = here<It>;
        };

        template<typename It>
        using split_view_base = meta::invoke<here_or_there_<!forward_iterator<It>>, It>;

        template<typename JoinView, bool Const>
        struct split_outer_iterator;

        template<typename JoinView, bool Const>
        struct split_inner_iterator;

        template<typename V, typename Pattern, bool Const>
        struct split_inner_iterator<split_view<V, Pattern>, Const>
        {
        private:
            using Outer = split_outer_iterator<split_view<V, Pattern>, Const>;
            using Base = meta::const_if_c<Const, V>;
            using BaseIterCategory =
                typename std::iterator_traits<iterator_t<Base>>::iterator_category;
            Outer i_ = Outer();
            bool incremented_ = false;
            constexpr decltype(auto) current_() noexcept
            {
                return i_.current_();
            }
            constexpr decltype(auto) current_() const noexcept
            {
                return i_.current_();
            }
            constexpr bool done_() const
            {
                auto cur = current_();
                auto last = ranges::end(i_.parent_->base_);
                if(cur == last)
                    return true;
                auto pcur = ranges::begin(i_.parent_->pattern_);
                auto pend = ranges::end(i_.parent_->pattern_);
                if(pcur == pend)
                    return incremented_;
                do
                {
                    if(*cur != *pcur)
                        return false;
                    if(++pcur == pend)
                        return true;
                } while(++cur != last);
                return false;
            }
#if RANGES_CXX_IF_CONSTEXPR < RANGES_CXX_IF_CONSTEXPR_17
            constexpr void pre_inc(std::true_type) // Forward
            {
                ++current_();
            }
            constexpr void pre_inc(std::false_type) // Input
            {
                if(Pattern::size() != 0)
                    ++current_();
            }
            constexpr split_inner_iterator post_inc(std::true_type) // Forward
            {
                auto tmp = *this;
                pre_inc(std::true_type{});
                return tmp;
            }
            constexpr void post_inc(std::false_type) // Input
            {
                pre_inc(std::false_type{});
            }
#endif
        public:
            using iterator_concept = typename Outer::iterator_concept;
            using iterator_category =
                meta::conditional_t<
                    derived_from<BaseIterCategory, std::forward_iterator_tag>,
                    std::forward_iterator_tag,
                    std::input_iterator_tag>;
            using value_type = range_value_t<Base>;
            using difference_type = range_difference_t<Base>;
            using reference = range_reference_t<Base>;        // Not to spec
            using pointer = iter_pointer_t<iterator_t<Base>>; // Not to spec

            split_inner_iterator() = default;

            constexpr explicit split_inner_iterator(Outer i)
              : i_(std::move(i))
            {}

            constexpr decltype(auto) operator*() const
            {
                return *current_();
            }

            constexpr split_inner_iterator & operator++()
            {
                incremented_ = true;
#if RANGES_CXX_IF_CONSTEXPR >= RANGES_CXX_IF_CONSTEXPR_17
                if constexpr(!forward_range<Base>)
                    if constexpr(Pattern::size() == 0)
                        return *this;
                ++current_();
#else
                pre_inc(meta::bool_<forward_range<Base>>{});
#endif
                return *this;
            }

            constexpr decltype(auto) operator++(int)
            {
#if RANGES_CXX_IF_CONSTEXPR >= RANGES_CXX_IF_CONSTEXPR_17
                if constexpr(forward_range<V>)
                {
                    auto tmp = *this;
                    ++*this;
                    return tmp;
                }
                else
                    ++*this;
#else
                return post_inc(meta::bool_<forward_range<V>>{});
#endif
            }

            CPP_broken_friend_member
            friend constexpr auto operator==(split_inner_iterator const & x,
                                             split_inner_iterator const & y)
                -> CPP_broken_friend_ret(bool)(
                    requires forward_range<Base>)
            {
                return x.i_.curr_ == y.i_.curr_;
            }
            CPP_broken_friend_member
            friend constexpr auto operator!=(split_inner_iterator const & x,
                                             split_inner_iterator const & y)
                -> CPP_broken_friend_ret(bool)(
                    requires forward_range<Base>)
            {
                return x.i_.curr_ != y.i_.curr_;
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator==(split_inner_iterator const & x,
                                             default_sentinel_t)
            {
                return x.done_();
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator==(default_sentinel_t,
                                             split_inner_iterator const & x)
            {
                return x.done_();
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator!=(split_inner_iterator const & x,
                                             default_sentinel_t)
            {
                return !x.done_();
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator!=(default_sentinel_t,
                                             split_inner_iterator const & x)
            {
                return !x.done_();
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr decltype(auto) iter_move(
                split_inner_iterator const &
                    i) noexcept(noexcept(ranges::iter_move(i.current_())))
            {
                return ranges::iter_move(i.current_());
            }
            CPP_broken_friend_member
            friend constexpr auto iter_swap(
                split_inner_iterator const & x,
                split_inner_iterator const &
                    y) noexcept(noexcept(ranges::iter_swap(x.current_(), y.current_())))
                -> CPP_broken_friend_ret(void)(
                    requires indirectly_swappable<iterator_t<Base>>)
            {
                ranges::iter_swap(x.current_(), y.current_());
            }
        };

        template<typename It>
        using split_outer_iterator_base =
            meta::invoke<here_or_there_<forward_iterator<It>>, It>;

        template<typename JoinView, bool Const>
        struct split_outer_iterator;

        template<typename V, typename Pattern, bool Const>
        struct split_outer_iterator<split_view<V, Pattern>, Const>
          : split_outer_iterator_base<iterator_t<meta::const_if_c<Const, V>>>
        {
        private:
            friend struct split_inner_iterator<split_view<V, Pattern>, Const>;
            using Parent = meta::const_if_c<Const, split_view<V, Pattern>>;
            using Base = meta::const_if_c<Const, V>;
            using Current = split_outer_iterator_base<iterator_t<Base>>;

            Parent * parent_ = nullptr;
            constexpr decltype(auto) current_() noexcept
            {
                return parent_->current_(*this);
            }
            constexpr decltype(auto) current_() const noexcept
            {
                return parent_->current_(*this);
            }
            constexpr decltype(auto) base_() const noexcept
            {
                return (parent_->base_);
            }
#if RANGES_CXX_IF_CONSTEXPR < RANGES_CXX_IF_CONSTEXPR_17
            constexpr split_outer_iterator post_inc(std::true_type) // Forward
            {
                auto tmp = *this;
                ++*this;
                return tmp;
            }
            constexpr void post_inc(std::false_type) // Input
            {
                ++*this;
            }
#endif

        public:
            using iterator_concept =
                meta::conditional_t<forward_range<Base>, std::forward_iterator_tag,
                          std::input_iterator_tag>;
            using iterator_category = std::input_iterator_tag;
            struct value_type : view_interface<value_type>
            {
            private:
                split_outer_iterator i_ = split_outer_iterator();

            public:
                value_type() = default;
                constexpr explicit value_type(split_outer_iterator i)
                  : i_(std::move(i))
                {}
                constexpr split_inner_iterator<split_view<V, Pattern>, Const> begin()
                    const
                {
                    return split_inner_iterator<split_view<V, Pattern>, Const>(i_);
                }
                constexpr default_sentinel_t end() const
                {
                    return default_sentinel;
                }
            };
            using difference_type = range_difference_t<Base>;
            using reference = value_type; // Not to spec
            using pointer = value_type *; // Not to spec

            split_outer_iterator() = default;

            CPP_member
            constexpr explicit CPP_ctor(split_outer_iterator)(Parent * parent)(
                requires (!forward_range<Base>))
              : parent_(parent)
            {}

            CPP_member
            constexpr CPP_ctor(split_outer_iterator)(Parent * parent,
                                                     iterator_t<Base> current)(
                requires forward_range<Base>)
              : Current{std::move(current)}
              , parent_(parent)
            {}

            template(bool Other)(
                requires Const AND CPP_NOT(Other) AND
                convertible_to<iterator_t<V>, iterator_t<Base>>)
            constexpr split_outer_iterator(
                split_outer_iterator<split_view<V, Pattern>, Other> i)
              : Current{std::move(i.curr_)}
              , parent_(i.parent_)
            {}

            constexpr value_type operator*() const
            {
                return value_type{*this};
            }

            constexpr split_outer_iterator & operator++()
            {
                auto & current = current_();
                const auto last = ranges::end(base_());
                if(current == last)
                    return *this;
                auto const pbegin = ranges::begin(parent_->pattern_);
                auto const pend = ranges::end(parent_->pattern_);
                if(pbegin == pend)
                    ++current;
                else
                    do
                    {
                        const auto ret = ranges::mismatch(current, last, pbegin, pend);
                        if(ret.in2 == pend)
                        {
                            current = ret.in1; // The pattern matched; skip it
                            break;
                        }
                    } while(++current != last);
                return *this;
            }

            constexpr decltype(auto) operator++(int)
            {
#if RANGES_CXX_IF_CONSTEXPR >= RANGES_CXX_IF_CONSTEXPR_17
                if constexpr(forward_range<Base>)
                {
                    auto tmp = *this;
                    ++*this;
                    return tmp;
                }
                else
                    ++*this;
#else
                return post_inc(meta::bool_<forward_range<Base>>{});
#endif
            }

            CPP_broken_friend_member
            friend constexpr auto operator==(split_outer_iterator const & x,
                                             split_outer_iterator const & y)
                -> CPP_broken_friend_ret(bool)(
                    requires forward_range<Base>)
            {
                return x.curr_ == y.curr_;
            }
            CPP_broken_friend_member
            friend constexpr auto operator!=(split_outer_iterator const & x,
                                             split_outer_iterator const & y)
                -> CPP_broken_friend_ret(bool)(
                    requires forward_range<Base>)
            {
                return x.curr_ != y.curr_;
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator==(split_outer_iterator const & x,
                                             default_sentinel_t)
            {
                return x.current_() == ranges::end(x.base_());
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator==(default_sentinel_t,
                                             split_outer_iterator const & x)
            {
                return x.current_() == ranges::end(x.base_());
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator!=(split_outer_iterator const & x,
                                             default_sentinel_t)
            {
                return x.current_() != ranges::end(x.base_());
            }
#ifdef RANGES_WORKAROUND_MSVC_756601
            template<typename = void>
#endif // RANGES_WORKAROUND_MSVC_756601
            friend constexpr bool operator!=(default_sentinel_t,
                                             split_outer_iterator const & x)
            {
                return x.current_() != ranges::end(x.base_());
            }
        };
    } // namespace detail
    /// \endcond

    template<typename V, typename Pattern>
#if CPP_CXX_CONCEPTS
        requires input_range<V> && forward_range<Pattern> && view_<V> && view_<
            Pattern> && indirectly_comparable<iterator_t<V>, iterator_t<Pattern>,
                                              ranges::equal_to> &&
        (forward_range<V> || detail::tiny_range<Pattern>)
#endif
    struct RANGES_EMPTY_BASES split_view
      : view_interface<split_view<V, Pattern>, is_finite<V>::value ? finite : unknown>
      , private detail::split_view_base<iterator_t<V>>
    {
    private:
        template<typename, bool>
        friend struct detail::split_outer_iterator;
        template<typename, bool>
        friend struct detail::split_inner_iterator;

        V base_ = V();
        Pattern pattern_ = Pattern();
        template<bool Const>
        using outer_iterator = detail::split_outer_iterator<split_view, Const>;

#if RANGES_CXX_IF_CONSTEXPR < RANGES_CXX_IF_CONSTEXPR_17
        outer_iterator<simple_view<V>()> begin_(std::true_type)
        {
            return outer_iterator<simple_view<V>()>{this, ranges::begin(base_)};
        }
        outer_iterator<false> begin_(std::false_type)
        {
            this->curr_ = ranges::begin(base_);
            return outer_iterator<false>{this};
        }

        outer_iterator<simple_view<V>()> end_(std::true_type) const
        {
            return outer_iterator<true>{this, ranges::end(base_)};
        }
        default_sentinel_t end_(std::false_type) const
        {
            return default_sentinel;
        }
#endif

    public:
        split_view() = default;

        constexpr split_view(V base, Pattern pattern)
          : base_((V &&) base)
          , pattern_((Pattern &&) pattern)
        {}

        CPP_member
        constexpr CPP_ctor(split_view)(V base, range_value_t<V> e)(
            requires constructible_from<Pattern, range_value_t<V>>)
          : base_(std::move(base))
          , pattern_(e)
        {}

        constexpr V base() const
        {
            return base_;
        }

        constexpr outer_iterator<forward_range<V> && simple_view<V>()> begin()
        {
#if RANGES_CXX_IF_CONSTEXPR >= RANGES_CXX_IF_CONSTEXPR_17
            if constexpr(forward_range<V>)
                return outer_iterator<simple_view<V>()>{this, ranges::begin(base_)};
            else
            {
                this->curr_ = ranges::begin(base_);
                return outer_iterator<false>{this};
            }
#else
            return begin_(meta::bool_<forward_range<V>>{});
#endif
        }
        CPP_member
        constexpr auto begin() const //
            -> CPP_ret(outer_iterator<true>)(
                requires forward_range<V> && forward_range<const V>)
        {
            return {this, ranges::begin(base_)};
        }
        CPP_member
        constexpr auto end() //
            -> CPP_ret(outer_iterator<simple_view<V>()>)(
                requires forward_range<V> && common_range<V>)
        {
            return outer_iterator<simple_view<V>()>{this, ranges::end(base_)};
        }
        constexpr auto end() const
        {
#if RANGES_CXX_IF_CONSTEXPR >= RANGES_CXX_IF_CONSTEXPR_17
            if constexpr(forward_range<V> && forward_range<const V> &&
                         common_range<const V>)
                return outer_iterator<true>{this, ranges::end(base_)};
            else
                return default_sentinel;
#else
            return end_(meta::bool_ < forward_range<V> && forward_range<const V> &&
                        common_range<const V> > {});
#endif
        }
    };

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template(typename R, typename P)(
        requires input_range<R> AND forward_range<P> AND viewable_range<R> AND
            viewable_range<P> AND
            indirectly_comparable<iterator_t<R>, iterator_t<P>, ranges::equal_to> AND
            (forward_range<R> || detail::tiny_range<P>)) //
    split_view(R &&, P &&)
            ->split_view<views::all_t<R>, views::all_t<P>>;

    template(typename R)(
        requires input_range<R>)
        split_view(R &&, range_value_t<R>)
            ->split_view<views::all_t<R>, single_view<range_value_t<R>>>;
#endif

    namespace views
    {
        struct split_base_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    indirectly_comparable<iterator_t<Rng>,
                                          range_value_t<Rng> const *,
                                          ranges::equal_to>)
            constexpr split_view<all_t<Rng>, single_view<range_value_t<Rng>>> //
            operator()(Rng && rng, range_value_t<Rng> val) const
            {
                return {all(static_cast<Rng &&>(rng)), single(std::move(val))};
            }

            template(typename Rng, typename Pattern)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    viewable_range<Pattern> AND forward_range<Pattern> AND
                    indirectly_comparable<
                        iterator_t<Rng>,
                        iterator_t<Pattern>,
                        ranges::equal_to> AND
                    (forward_range<Rng> || detail::tiny_range<Pattern>)) //
            constexpr split_view<all_t<Rng>, all_t<Pattern>> //
            operator()(Rng && rng, Pattern && pattern) const
            {
                return {all((Rng &&) rng), all((Pattern &&) pattern)};
            }
        };

        struct split_fn : split_base_fn
        {
            using split_base_fn::operator();

            template<typename T>
            constexpr auto operator()(T t) const
            {
                return make_view_closure(bind_back(split_base_fn{}, std::move(t)));
            }
        };

        /// \relates split_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(split_fn, split)
    } // namespace views

    namespace cpp20
    {
        namespace views
        {
            using ranges::views::split;
        }
        template(typename Rng, typename Pattern)(
            requires input_range<Rng> AND forward_range<Pattern> AND view_<Rng> AND
                view_<Pattern> AND
                indirectly_comparable<
                    iterator_t<Rng>,
                    iterator_t<Pattern>,
                    ranges::equal_to> AND
                (forward_range<Rng> || ranges::detail::tiny_range<Pattern>)) //
        using split_view =
            ranges::split_view<Rng, Pattern>;
    } // namespace cpp20

    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::split_view)

#endif
