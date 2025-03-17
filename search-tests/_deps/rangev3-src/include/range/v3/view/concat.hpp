/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-2014.
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_VIEW_CONCAT_HPP
#define RANGES_V3_VIEW_CONCAT_HPP

#include <tuple>
#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/arithmetic.hpp>
#include <range/v3/functional/compose.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/utility/tuple_algorithm.hpp>
#include <range/v3/utility/variant.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/facade.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename State, typename Value>
        using concat_cardinality_ = std::integral_constant<
            cardinality,
            State::value == infinite || Value::value == infinite
                ? infinite
                : State::value == unknown || Value::value == unknown
                      ? unknown
                      : State::value == finite || Value::value == finite
                            ? finite
                            : static_cast<cardinality>(State::value + Value::value)>;

        template<typename... Rngs>
        using concat_cardinality =
            meta::fold<meta::list<range_cardinality<Rngs>...>,
                       std::integral_constant<cardinality, static_cast<cardinality>(0)>,
                       meta::quote<concat_cardinality_>>;
    } // namespace detail
    /// \endcond

    /// \addtogroup group-views
    /// @{
    template<typename... Rngs>
    struct concat_view
      : view_facade<concat_view<Rngs...>, detail::concat_cardinality<Rngs...>::value>
    {
    private:
        friend range_access;
        using difference_type_ = common_type_t<range_difference_t<Rngs>...>;
        static constexpr std::size_t cranges{sizeof...(Rngs)};
        std::tuple<Rngs...> rngs_;

        template<bool IsConst>
        struct cursor;

        template<bool IsConst>
        struct sentinel
        {
        private:
            friend struct sentinel<!IsConst>;
            friend struct cursor<IsConst>;
            template<typename T>
            using constify_if = meta::const_if_c<IsConst, T>;
            using concat_view_t = constify_if<concat_view>;
            sentinel_t<constify_if<meta::back<meta::list<Rngs...>>>> end_;

        public:
            sentinel() = default;
            sentinel(concat_view_t * rng, end_tag)
              : end_(end(std::get<cranges - 1>(rng->rngs_)))
            {}
            template(bool Other)(
                requires IsConst AND CPP_NOT(Other)) //
            sentinel(sentinel<Other> that)
              : end_(std::move(that.end_))
            {}
        };

        template<bool IsConst>
        struct cursor
        {
            using difference_type = common_type_t<range_difference_t<Rngs>...>;

        private:
            friend struct cursor<!IsConst>;
            template<typename T>
            using constify_if = meta::const_if_c<IsConst, T>;
            using concat_view_t = constify_if<concat_view>;
            concat_view_t * rng_;
            variant<iterator_t<constify_if<Rngs>>...> its_;

            template<std::size_t N>
            void satisfy(meta::size_t<N>)
            {
                RANGES_EXPECT(its_.index() == N);
                if(ranges::get<N>(its_) == end(std::get<N>(rng_->rngs_)))
                {
                    ranges::emplace<N + 1>(its_, begin(std::get<N + 1>(rng_->rngs_)));
                    this->satisfy(meta::size_t<N + 1>{});
                }
            }
            void satisfy(meta::size_t<cranges - 1>)
            {
                RANGES_EXPECT(its_.index() == cranges - 1);
            }
            struct next_fun
            {
                cursor * pos;
                template(typename I, std::size_t N)(
                    requires input_iterator<I>)
                void operator()(indexed_element<I, N> it) const
                {
                    RANGES_ASSERT(it.get() != end(std::get<N>(pos->rng_->rngs_)));
                    ++it.get();
                    pos->satisfy(meta::size_t<N>{});
                }
            };
            struct prev_fun
            {
                cursor * pos;
                template(typename I)(
                    requires bidirectional_iterator<I>)
                void operator()(indexed_element<I, 0> it) const
                {
                    RANGES_ASSERT(it.get() != begin(std::get<0>(pos->rng_->rngs_)));
                    --it.get();
                }
                template(typename I, std::size_t N)(
                    requires (N != 0) AND bidirectional_iterator<I>)
                void operator()(indexed_element<I, N> it) const
                {
                    if(it.get() == begin(std::get<N>(pos->rng_->rngs_)))
                    {
                        auto && rng = std::get<N - 1>(pos->rng_->rngs_);
                        ranges::emplace<N - 1>(
                            pos->its_,
                            ranges::next(ranges::begin(rng), ranges::end(rng)));
                        pos->its_.visit_i(*this);
                    }
                    else
                        --it.get();
                }
            };
            struct advance_fwd_fun
            {
                cursor * pos;
                difference_type n;
                template(typename I)(
                    requires random_access_iterator<I>)
                void operator()(indexed_element<I, cranges - 1> it) const
                {
                    ranges::advance(it.get(), n);
                }
                template(typename I, std::size_t N)(
                    requires random_access_iterator<I>)
                void operator()(indexed_element<I, N> it) const
                {
                    auto last = ranges::end(std::get<N>(pos->rng_->rngs_));
                    // BUGBUG If distance(it, last) > n, then using bounded advance
                    // is O(n) when it need not be since the last iterator position
                    // is actually not interesting. Only the "rest" is needed, which
                    // can sometimes be O(1).
                    auto rest = ranges::advance(it.get(), n, std::move(last));
                    pos->satisfy(meta::size_t<N>{});
                    if(rest != 0)
                        pos->its_.visit_i(advance_fwd_fun{pos, rest});
                }
            };
            struct advance_rev_fun
            {
                cursor * pos;
                difference_type n;
                template(typename I)(
                    requires random_access_iterator<I>)
                void operator()(indexed_element<I, 0> it) const
                {
                    ranges::advance(it.get(), n);
                }
                template(typename I, std::size_t N)(
                    requires random_access_iterator<I>)
                void operator()(indexed_element<I, N> it) const
                {
                    auto first = ranges::begin(std::get<N>(pos->rng_->rngs_));
                    if(it.get() == first)
                    {
                        auto && rng = std::get<N - 1>(pos->rng_->rngs_);
                        ranges::emplace<N - 1>(
                            pos->its_,
                            ranges::next(ranges::begin(rng), ranges::end(rng)));
                        pos->its_.visit_i(*this);
                    }
                    else
                    {
                        auto rest = ranges::advance(it.get(), n, std::move(first));
                        if(rest != 0)
                            pos->its_.visit_i(advance_rev_fun{pos, rest});
                    }
                }
            };
            [[noreturn]] static difference_type distance_to_(meta::size_t<cranges>,
                                                             cursor const &,
                                                             cursor const &)
            {
                RANGES_EXPECT(false);
            }
            template<std::size_t N>
            static difference_type distance_to_(meta::size_t<N>, cursor const & from,
                                                cursor const & to)
            {
                if(from.its_.index() > N)
                    return cursor::distance_to_(meta::size_t<N + 1>{}, from, to);
                if(from.its_.index() == N)
                {
                    if(to.its_.index() == N)
                        return distance(ranges::get<N>(from.its_),
                                        ranges::get<N>(to.its_));
                    return distance(ranges::get<N>(from.its_),
                                    end(std::get<N>(from.rng_->rngs_))) +
                           cursor::distance_to_(meta::size_t<N + 1>{}, from, to);
                }
                if(from.its_.index() < N && to.its_.index() > N)
                    return distance(std::get<N>(from.rng_->rngs_)) +
                           cursor::distance_to_(meta::size_t<N + 1>{}, from, to);
                RANGES_EXPECT(to.its_.index() == N);
                return distance(begin(std::get<N>(from.rng_->rngs_)),
                                ranges::get<N>(to.its_));
            }

        public:
            // BUGBUG what about rvalue_reference and common_reference?
            using reference = common_reference_t<range_reference_t<constify_if<Rngs>>...>;
            using single_pass = meta::or_c<single_pass_iterator_<iterator_t<Rngs>>...>;
            cursor() = default;
            cursor(concat_view_t * rng, begin_tag)
              : rng_(rng)
              , its_{emplaced_index<0>, begin(std::get<0>(rng->rngs_))}
            {
                this->satisfy(meta::size_t<0>{});
            }
            cursor(concat_view_t * rng, end_tag)
              : rng_(rng)
              , its_{emplaced_index<cranges - 1>, end(std::get<cranges - 1>(rng->rngs_))}
            {}
            template(bool Other)(
                requires IsConst && CPP_NOT(Other)) //
            cursor(cursor<Other> that)
              : rng_(that.rng_)
              , its_(std::move(that.its_))
            {}
            reference read() const
            {
                // Kind of a dumb implementation. Surely there's a better way.
                return ranges::get<0>(unique_variant(its_.visit(
                    compose(convert_to<reference>{}, detail::dereference_fn{}))));
            }
            void next()
            {
                its_.visit_i(next_fun{this});
            }
            CPP_member
            auto equal(cursor const & pos) const //
                -> CPP_ret(bool)(
                    requires //
                        equality_comparable<variant<iterator_t<constify_if<Rngs>>...>>)
            {
                return its_ == pos.its_;
            }
            bool equal(sentinel<IsConst> const & pos) const
            {
                return its_.index() == cranges - 1 &&
                       ranges::get<cranges - 1>(its_) == pos.end_;
            }
            CPP_member
            auto prev() //
                -> CPP_ret(void)(
                    requires and_v<bidirectional_range<Rngs>...>)
            {
                its_.visit_i(prev_fun{this});
            }
            CPP_member
            auto advance(difference_type n) //
                -> CPP_ret(void)(
                    requires and_v<random_access_range<Rngs>...>)
            {
                if(n > 0)
                    its_.visit_i(advance_fwd_fun{this, n});
                else if(n < 0)
                    its_.visit_i(advance_rev_fun{this, n});
            }
            CPP_member
            auto distance_to(cursor const & that) const //
                -> CPP_ret(difference_type)(
                    requires and_v<sized_sentinel_for<iterator_t<Rngs>,
                                                      iterator_t<Rngs>>...>)
            {
                if(its_.index() <= that.its_.index())
                    return cursor::distance_to_(meta::size_t<0>{}, *this, that);
                return -cursor::distance_to_(meta::size_t<0>{}, that, *this);
            }
        };
        cursor<meta::and_c<simple_view<Rngs>()...>::value> begin_cursor()
        {
            return {this, begin_tag{}};
        }
        meta::if_<meta::and_c<(bool)common_range<Rngs>...>,
                  cursor<meta::and_c<simple_view<Rngs>()...>::value>,
                  sentinel<meta::and_c<simple_view<Rngs>()...>::value>>
        end_cursor()
        {
            return {this, end_tag{}};
        }
        CPP_member
        auto begin_cursor() const //
            -> CPP_ret(cursor<true>)(
                requires and_v<range<Rngs const>...>)
        {
            return {this, begin_tag{}};
        }
        CPP_member
        auto end_cursor() const //
            -> CPP_ret(
                meta::if_<meta::and_c<(bool)common_range<Rngs const>...>, //
                          cursor<true>, //
                          sentinel<true>>)(
            requires and_v<range<Rngs const>...>)
        {
            return {this, end_tag{}};
        }

    public:
        concat_view() = default;
        explicit concat_view(Rngs... rngs)
          : rngs_{std::move(rngs)...}
        {}
        CPP_member
        constexpr auto size() const //
            -> CPP_ret(std::size_t)(
                requires (detail::concat_cardinality<Rngs...>::value >= 0))
        {
            return static_cast<std::size_t>(detail::concat_cardinality<Rngs...>::value);
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(const //
            requires(detail::concat_cardinality<Rngs...>::value < 0) &&
                and_v<sized_range<Rngs const>...>)
        {
            using size_type = common_type_t<range_size_t<Rngs const>...>;
            return tuple_foldl(
                tuple_transform(rngs_,
                                [](auto && r) -> size_type { return ranges::size(r); }),
                size_type{0},
                plus{});
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(
            requires (detail::concat_cardinality<Rngs...>::value < 0) &&
                and_v<sized_range<Rngs>...>)
        {
            using size_type = common_type_t<range_size_t<Rngs>...>;
            return tuple_foldl(
                tuple_transform(rngs_,
                                [](auto && r) -> size_type { return ranges::size(r); }),
                size_type{0},
                plus{});
        }
    };

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename... Rng>
    concat_view(Rng &&...) //
        -> concat_view<views::all_t<Rng>...>;
#endif

    namespace views
    {
        struct concat_fn
        {
            template(typename... Rngs)(
                requires and_v<(viewable_range<Rngs> && input_range<Rngs>)...>)
            concat_view<all_t<Rngs>...> operator()(Rngs &&... rngs) const
            {
                return concat_view<all_t<Rngs>...>{all(static_cast<Rngs &&>(rngs))...};
            }
            template(typename Rng)(
                requires viewable_range<Rng> AND input_range<Rng>)
            all_t<Rng> operator()(Rng && rng) const //
            {
                return all(static_cast<Rng &&>(rng));
            }
            // MSVC doesn't like variadics in operator() for some reason
#if defined(_MSC_VER)
            template(typename Rng0, typename Rng1)(
                requires viewable_range<Rng0> AND input_range<Rng0> AND
                        viewable_range<Rng1> AND input_range<Rng1>)
            concat_view<all_t<Rng0>, all_t<Rng1>> operator()(Rng0 && rng0, Rng1 && rng1)
                const
            {
                return concat_view<all_t<Rng0>, all_t<Rng1>>{
                    all(static_cast<Rng0 &&>(rng0)),
                    all(static_cast<Rng1 &&>(rng1))};
            }
            template(typename Rng0, typename Rng1, typename Rng2)(
                requires viewable_range<Rng0> AND input_range<Rng0> AND
                    viewable_range<Rng1> AND input_range<Rng1> AND
                    viewable_range<Rng2> AND input_range<Rng2>)
            concat_view<all_t<Rng0>, all_t<Rng1>, all_t<Rng2>> //
            operator()(Rng0 && rng0, Rng1 && rng1, Rng2 && rng2) const
            {
                return concat_view<all_t<Rng0>, all_t<Rng1>, all_t<Rng2>>{
                    all(static_cast<Rng0 &&>(rng0)),
                    all(static_cast<Rng1 &&>(rng1)),
                    all(static_cast<Rng2 &&>(rng2))};
            }
#endif
        };

        /// \relates concat_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(concat_fn, concat)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>
#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::concat_view)

#endif
