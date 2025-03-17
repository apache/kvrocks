/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Tomislav Ivek 2015-2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_VIEW_SET_ALGORITHM_HPP
#define RANGES_V3_VIEW_SET_ALGORITHM_HPP

#include <algorithm>
#include <iterator>
#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/comparisons.hpp>
#include <range/v3/functional/identity.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/move.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/facade.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{

    /// \cond
    namespace detail
    {
        template<typename Rng1, typename Rng2, typename C, typename P1, typename P2,
                 template<bool, typename...> class Cursor, cardinality Cardinality>
        struct set_algorithm_view
          : view_facade<set_algorithm_view<Rng1, Rng2, C, P1, P2, Cursor, Cardinality>,
                        Cardinality>
        {
        private:
            friend range_access;
            semiregular_box_t<C> pred_;
            semiregular_box_t<P1> proj1_;
            semiregular_box_t<P2> proj2_;
            Rng1 rng1_;
            Rng2 rng2_;

            template<bool IsConst>
            using cursor = Cursor<IsConst, Rng1, Rng2, C, P1, P2>;

            cursor<simple_view<Rng1>() && simple_view<Rng2>()> begin_cursor()
            {
                return {pred_,
                        proj1_,
                        proj2_,
                        ranges::begin(rng1_),
                        ranges::end(rng1_),
                        ranges::begin(rng2_),
                        ranges::end(rng2_)};
            }
            CPP_member
            auto begin_cursor() const //
                -> CPP_ret(cursor<true>)(
                    requires range<Rng1 const> && range<Rng2 const>)
            {
                return {pred_,
                        proj1_,
                        proj2_,
                        ranges::begin(rng1_),
                        ranges::end(rng1_),
                        ranges::begin(rng2_),
                        ranges::end(rng2_)};
            }

        public:
            set_algorithm_view() = default;
            set_algorithm_view(Rng1 rng1, Rng2 rng2, C pred, P1 proj1, P2 proj2)
              : pred_(std::move(pred))
              , proj1_(std::move(proj1))
              , proj2_(std::move(proj2))
              , rng1_(std::move(rng1))
              , rng2_(std::move(rng2))
            {}
        };

        template<bool IsConst, typename Rng1, typename Rng2, typename C, typename P1,
                 typename P2>
        struct set_difference_cursor
        {
        private:
            friend struct set_difference_cursor<!IsConst, Rng1, Rng2, C, P1, P2>;
            using pred_ref_ = semiregular_box_ref_or_val_t<C, IsConst>;
            using proj1_ref_ = semiregular_box_ref_or_val_t<P1, IsConst>;
            using proj2_ref_ = semiregular_box_ref_or_val_t<P2, IsConst>;
            pred_ref_ pred_;
            proj1_ref_ proj1_;
            proj2_ref_ proj2_;

            template<typename T>
            using constify_if = meta::const_if_c<IsConst, T>;

            using R1 = constify_if<Rng1>;
            using R2 = constify_if<Rng2>;

            iterator_t<R1> it1_;
            sentinel_t<R1> end1_;

            iterator_t<R2> it2_;
            sentinel_t<R2> end2_;

            void satisfy()
            {
                while(it1_ != end1_)
                {
                    if(it2_ == end2_)
                        return;

                    if(invoke(pred_, invoke(proj1_, *it1_), invoke(proj2_, *it2_)))
                        return;

                    if(!invoke(pred_, invoke(proj2_, *it2_), invoke(proj1_, *it1_)))
                        ++it1_;

                    ++it2_;
                }
            }

        public:
            using value_type = range_value_t<constify_if<Rng1>>;
            using single_pass = meta::or_c<single_pass_iterator_<iterator_t<R1>>,
                                           single_pass_iterator_<iterator_t<R2>>>;

            set_difference_cursor() = default;
            set_difference_cursor(pred_ref_ pred, proj1_ref_ proj1, proj2_ref_ proj2,
                                  iterator_t<R1> it1, sentinel_t<R1> end1,
                                  iterator_t<R2> it2, sentinel_t<R2> end2)
              : pred_(std::move(pred))
              , proj1_(std::move(proj1))
              , proj2_(std::move(proj2))
              , it1_(std::move(it1))
              , end1_(std::move(end1))
              , it2_(std::move(it2))
              , end2_(std::move(end2))
            {
                satisfy();
            }
            template(bool Other)(
                requires IsConst && CPP_NOT(Other)) //
                set_difference_cursor(
                    set_difference_cursor<Other, Rng1, Rng2, C, P1, P2> that)
              : pred_(std::move(that.pred_))
              , proj1_(std::move(that.proj1_))
              , proj2_(std::move(that.proj2_))
              , it1_(std::move(that.it1_))
              , end1_(std::move(that.end1_))
              , it2_(std::move(that.it2_))
              , end2_(std::move(that.end2_))
            {}
            // clang-format off
            auto CPP_auto_fun(read)()(const)
            (
                return *it1_
            )
            // clang-format on
            void next()
            {
                ++it1_;
                satisfy();
            }
            CPP_member
            auto equal(set_difference_cursor const & that) const //
                -> CPP_ret(bool)(
                    requires forward_range<Rng1>)
            {
                // does not support comparing iterators from different ranges
                return it1_ == that.it1_;
            }
            bool equal(default_sentinel_t) const
            {
                return it1_ == end1_;
            }
            // clang-format off
            auto CPP_auto_fun(move)()(const)
            (
                return iter_move(it1_)
            )
            // clang-format on
        };

        constexpr cardinality set_difference_cardinality(cardinality c1, cardinality c2)
        {
            return (c1 == unknown)
                       ? unknown
                       : (c1 >= 0) || (c1 == finite) ? finite : // else, c1 == infinite
                             (c2 >= 0) || (c2 == finite) ? infinite : unknown;
        }
    } // namespace detail
    /// \endcond

    template<typename Rng1, typename Rng2, typename C, typename P1, typename P2>
    using set_difference_view =
        detail::set_algorithm_view<Rng1, Rng2, C, P1, P2, detail::set_difference_cursor,
                                   detail::set_difference_cardinality(
                                       range_cardinality<Rng1>::value,
                                       range_cardinality<Rng2>::value)>;

    namespace views
    {
        struct set_difference_base_fn
        {
            template(typename Rng1, typename Rng2, typename C = less,
                     typename P1 = identity, typename P2 = identity)(
                requires //
                    viewable_range<Rng1> AND input_range<Rng1> AND
                    viewable_range<Rng2> AND input_range<Rng2> AND
                    indirect_relation<C,
                                      projected<iterator_t<Rng1>, P1>,
                                      projected<iterator_t<Rng2>, P2>>)
            set_difference_view<all_t<Rng1>, all_t<Rng2>, C, P1, P2> //
            operator()(Rng1 && rng1,
                       Rng2 && rng2,
                       C pred = C{},
                       P1 proj1 = P1{},
                       P2 proj2 = P2{}) const
            {
                return {all(static_cast<Rng1 &&>(rng1)),
                        all(static_cast<Rng2 &&>(rng2)),
                        std::move(pred),
                        std::move(proj1),
                        std::move(proj2)};
            }
        };

        struct set_difference_fn : set_difference_base_fn
        {
            using set_difference_base_fn::operator();

            template(typename Rng2, typename C = less, typename P1 = identity,
                     typename P2 = identity)(
                requires viewable_range<Rng2> AND input_range<Rng2> AND (!range<C>))
            constexpr auto operator()(Rng2 && rng2,
                                      C && pred = C{},
                                      P1 proj1 = P1{},
                                      P2 proj2 = P2{}) const
            {
                return make_view_closure(bind_back(set_difference_base_fn{},
                                                   all(rng2),
                                                   static_cast<C &&>(pred),
                                                   std::move(proj1),
                                                   std::move(proj2)));
            }
        };

        /// \relates set_difference_fn
        RANGES_INLINE_VARIABLE(set_difference_fn, set_difference)
    } // namespace views

    /// \cond
    namespace detail
    {
        template<bool IsConst, typename Rng1, typename Rng2, typename C, typename P1,
                 typename P2>
        struct set_intersection_cursor
        {
        private:
            friend struct set_intersection_cursor<!IsConst, Rng1, Rng2, C, P1, P2>;
            using pred_ref_ = semiregular_box_ref_or_val_t<C, IsConst>;
            using proj1_ref_ = semiregular_box_ref_or_val_t<P1, IsConst>;
            using proj2_ref_ = semiregular_box_ref_or_val_t<P2, IsConst>;
            pred_ref_ pred_;
            proj1_ref_ proj1_;
            proj2_ref_ proj2_;

            template<typename T>
            using constify_if = meta::const_if_c<IsConst, T>;

            using R1 = constify_if<Rng1>;
            using R2 = constify_if<Rng2>;

            iterator_t<R1> it1_;
            sentinel_t<R1> end1_;

            iterator_t<R2> it2_;
            sentinel_t<R2> end2_;

            void satisfy()
            {
                while(it1_ != end1_ && it2_ != end2_)
                {
                    if(invoke(pred_, invoke(proj1_, *it1_), invoke(proj2_, *it2_)))
                        ++it1_;
                    else
                    {
                        if(!invoke(pred_, invoke(proj2_, *it2_), invoke(proj1_, *it1_)))
                            return;
                        ++it2_;
                    }
                }
            }

        public:
            using value_type = range_value_t<R1>;
            using single_pass = meta::or_c<single_pass_iterator_<iterator_t<R1>>,
                                           single_pass_iterator_<iterator_t<R2>>>;

            set_intersection_cursor() = default;
            set_intersection_cursor(pred_ref_ pred, proj1_ref_ proj1, proj2_ref_ proj2,
                                    iterator_t<R1> it1, sentinel_t<R1> end1,
                                    iterator_t<R2> it2, sentinel_t<R2> end2)
              : pred_(std::move(pred))
              , proj1_(std::move(proj1))
              , proj2_(std::move(proj2))
              , it1_(std::move(it1))
              , end1_(std::move(end1))
              , it2_(std::move(it2))
              , end2_(std::move(end2))
            {
                satisfy();
            }
            template(bool Other)(
                requires IsConst && CPP_NOT(Other)) //
                set_intersection_cursor(
                    set_intersection_cursor<Other, Rng1, Rng2, C, P1, P2> that)
              : pred_(std::move(that.pred_))
              , proj1_(std::move(that.proj1_))
              , proj2_(std::move(that.proj2_))
              , it1_(std::move(that.it1_))
              , end1_(std::move(that.end1_))
              , it2_(std::move(that.it2_))
              , end2_(std::move(that.end2_))
            {}
            // clang-format off
            auto CPP_auto_fun(read)()(const)
            (
                return *it1_
            )
            // clang-format on
            void next()
            {
                ++it1_;
                ++it2_;
                satisfy();
            }
            CPP_member
            auto equal(set_intersection_cursor const & that) const //
                -> CPP_ret(bool)(
                    requires forward_range<Rng1>)
            {
                // does not support comparing iterators from different ranges
                return it1_ == that.it1_;
            }
            bool equal(default_sentinel_t) const
            {
                return (it1_ == end1_) || (it2_ == end2_);
            }
            // clang-format off
            auto CPP_auto_fun(move)()(const)
            (
                return iter_move(it1_)
            )
            // clang-format on
        };

        constexpr cardinality set_intersection_cardinality(cardinality c1, cardinality c2)
        {
            return (c1 == unknown) || (c2 == unknown)
                       ? unknown
                       : (c1 >= 0 || c1 == finite) || (c2 >= 0 || c2 == finite) ? finite
                                                                                : unknown;
        }
    } // namespace detail
    /// \endcond

    template<typename Rng1, typename Rng2, typename C, typename P1, typename P2>
    using set_intersection_view =
        detail::set_algorithm_view<Rng1, Rng2, C, P1, P2, detail::set_intersection_cursor,
                                   detail::set_intersection_cardinality(
                                       range_cardinality<Rng1>::value,
                                       range_cardinality<Rng2>::value)>;

    namespace views
    {
        struct set_intersection_base_fn
        {
            template(typename Rng1, typename Rng2, typename C = less,
                     typename P1 = identity, typename P2 = identity)(
                requires viewable_range<Rng1> AND input_range<Rng1> AND
                    viewable_range<Rng2> AND input_range<Rng2> AND
                    indirect_relation<
                        C,
                        projected<iterator_t<Rng1>, P1>,
                        projected<iterator_t<Rng2>, P2>>)
            set_intersection_view<all_t<Rng1>, all_t<Rng2>, C, P1, P2>
            operator()(Rng1 && rng1,
                       Rng2 && rng2,
                       C pred = C{},
                       P1 proj1 = P1{},
                       P2 proj2 = P2{}) const
            {
                return {all(static_cast<Rng1 &&>(rng1)),
                        all(static_cast<Rng2 &&>(rng2)),
                        std::move(pred),
                        std::move(proj1),
                        std::move(proj2)};
            }
        };

        struct set_intersection_fn : set_intersection_base_fn
        {
            using set_intersection_base_fn::operator();

            template(typename Rng2, typename C = less, typename P1 = identity,
                     typename P2 = identity)(
                requires viewable_range<Rng2> AND input_range<Rng2> AND (!range<C>))
            constexpr auto operator()(Rng2 && rng2,
                                      C && pred = C{},
                                      P1 proj1 = P1{},
                                      P2 proj2 = P2{}) const
            {
                return make_view_closure(bind_back(set_intersection_base_fn{},
                                                   all(rng2),
                                                   static_cast<C &&>(pred),
                                                   std::move(proj1),
                                                   std::move(proj2)));
            }
        };

        /// \relates set_intersection_fn
        RANGES_INLINE_VARIABLE(set_intersection_fn, set_intersection)
    } // namespace views

    /// \cond
    namespace detail
    {
        template<bool IsConst, typename Rng1, typename Rng2, typename C, typename P1,
                 typename P2>
        struct set_union_cursor
        {
        private:
            friend struct set_union_cursor<!IsConst, Rng1, Rng2, C, P1, P2>;
            using pred_ref_ = semiregular_box_ref_or_val_t<C, IsConst>;
            using proj1_ref_ = semiregular_box_ref_or_val_t<P1, IsConst>;
            using proj2_ref_ = semiregular_box_ref_or_val_t<P2, IsConst>;
            pred_ref_ pred_;
            proj1_ref_ proj1_;
            proj2_ref_ proj2_;

            template<typename T>
            using constify_if = meta::const_if_c<IsConst, T>;

            using R1 = constify_if<Rng1>;
            using R2 = constify_if<Rng2>;

            iterator_t<R1> it1_;
            sentinel_t<R1> end1_;

            iterator_t<R2> it2_;
            sentinel_t<R2> end2_;

            enum class state_t
            {
                FIRST,
                SECOND
            } state;

            void satisfy()
            {
                if(it1_ == end1_)
                {
                    state = state_t::SECOND;
                    return;
                }

                if(it2_ == end2_)
                {
                    state = state_t::FIRST;
                    return;
                }

                if(invoke(pred_, invoke(proj2_, *it2_), invoke(proj1_, *it1_)))
                {
                    state = state_t::SECOND;
                    return;
                }

                if(!invoke(pred_, invoke(proj1_, *it1_), invoke(proj2_, *it2_)))
                    ++it2_;

                state = state_t::FIRST;
            }

        public:
            using value_type = common_type_t<range_value_t<R1>, range_value_t<R2>>;
            using reference_type =
                common_reference_t<range_reference_t<R1>, range_reference_t<R2>>;
            using rvalue_reference_type =
                common_reference_t<range_rvalue_reference_t<R1>,
                                   range_rvalue_reference_t<R2>>;
            using single_pass = meta::or_c<single_pass_iterator_<iterator_t<R1>>,
                                           single_pass_iterator_<iterator_t<R2>>>;

            set_union_cursor() = default;
            set_union_cursor(pred_ref_ pred, proj1_ref_ proj1, proj2_ref_ proj2,
                             iterator_t<R1> it1, sentinel_t<R1> end1, iterator_t<R2> it2,
                             sentinel_t<R2> end2)
              : pred_(std::move(pred))
              , proj1_(std::move(proj1))
              , proj2_(std::move(proj2))
              , it1_(std::move(it1))
              , end1_(std::move(end1))
              , it2_(std::move(it2))
              , end2_(std::move(end2))
            {
                satisfy();
            }
            template(bool Other)(
                requires IsConst AND CPP_NOT(Other))
                set_union_cursor(set_union_cursor<Other, Rng1, Rng2, C, P1, P2> that)
              : pred_(std::move(that.pred_))
              , proj1_(std::move(that.proj1_))
              , proj2_(std::move(that.proj2_))
              , it1_(std::move(that.it1_))
              , end1_(std::move(that.end1_))
              , it2_(std::move(that.it2_))
              , end2_(std::move(that.end2_))
            {}
            reference_type read() const noexcept(noexcept(*it1_) && noexcept(*it2_))
            {
                if(state == state_t::SECOND)
                    return *it2_;
                else
                    return *it1_;
            }
            void next()
            {
                if(state == state_t::FIRST)
                    ++it1_;
                else
                    ++it2_;
                satisfy();
            }
            CPP_member
            auto equal(set_union_cursor const & that) const //
                -> CPP_ret(bool)(
                    requires forward_range<Rng1> && forward_range<Rng2>)
            {
                // does not support comparing iterators from different ranges
                return (it1_ == that.it1_) && (it2_ == that.it2_);
            }
            bool equal(default_sentinel_t) const
            {
                return (it1_ == end1_) && (it2_ == end2_);
            }
            rvalue_reference_type move() const
                noexcept(noexcept(iter_move(it1_)) && noexcept(iter_move(it2_)))
            {
                if(state == state_t::SECOND)
                    return iter_move(it2_);
                else
                    return iter_move(it1_);
            }
        };

        constexpr cardinality set_union_cardinality(cardinality c1, cardinality c2)
        {
            return (c1 == infinite) || (c2 == infinite)
                       ? infinite
                       : (c1 == unknown) || (c2 == unknown) ? unknown : finite;
        }
    } // namespace detail
    /// \endcond

    template<typename Rng1, typename Rng2, typename C, typename P1, typename P2>
    using set_union_view =
        detail::set_algorithm_view<Rng1, Rng2, C, P1, P2, detail::set_union_cursor,
                                   detail::set_union_cardinality(
                                       range_cardinality<Rng1>::value,
                                       range_cardinality<Rng2>::value)>;

    namespace views
    {
        struct set_union_base_fn
        {
        public:
            template(typename Rng1, typename Rng2, typename C = less,
                     typename P1 = identity, typename P2 = identity)(
                requires //
                    viewable_range<Rng1> AND input_range<Rng1> AND
                    viewable_range<Rng2> AND input_range<Rng2> AND
                    common_with<range_value_t<Rng1>, range_value_t<Rng2>> AND
                    common_reference_with<range_reference_t<Rng1>,
                                          range_reference_t<Rng2>> AND
                    common_reference_with<range_rvalue_reference_t<Rng1>,
                                          range_rvalue_reference_t<Rng2>> AND
                    indirect_relation<C,
                                      projected<iterator_t<Rng1>, P1>,
                                      projected<iterator_t<Rng2>, P2>>)
            set_union_view<all_t<Rng1>, all_t<Rng2>, C, P1, P2> //
            operator()(Rng1 && rng1,
                       Rng2 && rng2,
                       C pred = C{},
                       P1 proj1 = P1{},
                       P2 proj2 = P2{}) const
            {
                return {all(static_cast<Rng1 &&>(rng1)),
                        all(static_cast<Rng2 &&>(rng2)),
                        std::move(pred),
                        std::move(proj1),
                        std::move(proj2)};
            }
        };

        struct set_union_fn : set_union_base_fn
        {
            using set_union_base_fn::operator();

            template(typename Rng2, typename C = less, typename P1 = identity,
                     typename P2 = identity)(
                requires viewable_range<Rng2> AND input_range<Rng2> AND (!range<C>))
            constexpr auto operator()(Rng2 && rng2,
                                      C && pred = C{},
                                      P1 proj1 = P1{},
                                      P2 proj2 = P2{}) const
            {
                return make_view_closure(bind_back(set_union_base_fn{},
                                                   all(rng2),
                                                   static_cast<C &&>(pred),
                                                   std::move(proj1),
                                                   std::move(proj2)));
            }
        };

        /// \relates set_union_fn
        RANGES_INLINE_VARIABLE(set_union_fn, set_union)
    } // namespace views

    /// \cond
    namespace detail
    {
        template<bool IsConst, typename Rng1, typename Rng2, typename C, typename P1,
                 typename P2>
        struct set_symmetric_difference_cursor
        {
        private:
            friend struct set_symmetric_difference_cursor<!IsConst, Rng1, Rng2, C, P1,
                                                          P2>;
            using pred_ref_ = semiregular_box_ref_or_val_t<C, IsConst>;
            using proj1_ref_ = semiregular_box_ref_or_val_t<P1, IsConst>;
            using proj2_ref_ = semiregular_box_ref_or_val_t<P2, IsConst>;
            pred_ref_ pred_;
            proj1_ref_ proj1_;
            proj2_ref_ proj2_;

            template<typename T>
            using constify_if = meta::const_if_c<IsConst, T>;

            using R1 = constify_if<Rng1>;
            using R2 = constify_if<Rng2>;

            iterator_t<R1> it1_;
            sentinel_t<R1> end1_;

            iterator_t<R2> it2_;
            sentinel_t<R2> end2_;

            enum class state_t
            {
                FIRST,
                SECOND,
                ONLY_FIRST,
                ONLY_SECOND
            } state;

            void satisfy()
            {
                while(it1_ != end1_)
                {
                    if(it2_ == end2_)
                    {
                        state = state_t::ONLY_FIRST;
                        return;
                    }

                    if(invoke(pred_, invoke(proj1_, *it1_), invoke(proj2_, *it2_)))
                    {
                        state = state_t::FIRST;
                        return;
                    }
                    else
                    {
                        if(invoke(pred_, invoke(proj2_, *it2_), invoke(proj1_, *it1_)))
                        {
                            state = state_t::SECOND;
                            return;
                        }
                        else
                        {
                            ++it1_;
                            ++it2_;
                        }
                    }
                }
                state = state_t::ONLY_SECOND;
            }

        public:
            using value_type = common_type_t<range_value_t<R1>, range_value_t<R2>>;
            using reference_type =
                common_reference_t<range_reference_t<R1>, range_reference_t<R2>>;
            using rvalue_reference_type =
                common_reference_t<range_rvalue_reference_t<R1>,
                                   range_rvalue_reference_t<R2>>;
            using single_pass = meta::or_c<single_pass_iterator_<iterator_t<R1>>,
                                           single_pass_iterator_<iterator_t<R2>>>;

            set_symmetric_difference_cursor() = default;
            set_symmetric_difference_cursor(pred_ref_ pred, proj1_ref_ proj1,
                                            proj2_ref_ proj2, iterator_t<R1> it1,
                                            sentinel_t<R1> end1, iterator_t<R2> it2,
                                            sentinel_t<R2> end2)
              : pred_(std::move(pred))
              , proj1_(std::move(proj1))
              , proj2_(std::move(proj2))
              , it1_(std::move(it1))
              , end1_(std::move(end1))
              , it2_(std::move(it2))
              , end2_(std::move(end2))
              , state()
            {
                satisfy();
            }
            template(bool Other)(
                requires IsConst && CPP_NOT(Other)) //
            set_symmetric_difference_cursor(
                    set_symmetric_difference_cursor<Other, Rng1, Rng2, C, P1, P2> that)
              : pred_(std::move(that.pred_))
              , proj1_(std::move(that.proj1_))
              , proj2_(std::move(that.proj2_))
              , it1_(std::move(that.it1_))
              , end1_(std::move(that.end1_))
              , it2_(std::move(that.it2_))
              , end2_(std::move(that.end2_))
              , state(that.state)
            {}
            reference_type read() const noexcept(noexcept(*it1_) && noexcept(*it2_))
            {
                if(state == state_t::SECOND || state == state_t::ONLY_SECOND)
                    return *it2_;
                else
                    return *it1_;
            }
            void next()
            {
                switch(state)
                {
                case state_t::FIRST:
                    ++it1_;
                    satisfy();
                    break;
                case state_t::ONLY_FIRST:
                    ++it1_;
                    break;
                case state_t::SECOND:
                    ++it2_;
                    satisfy();
                    break;
                case state_t::ONLY_SECOND:
                    ++it2_;
                    break;
                }
            }
            CPP_member
            auto equal(set_symmetric_difference_cursor const & that) const
                -> CPP_ret(bool)(
                    requires forward_range<R1> && forward_range<R2>)
            {
                // does not support comparing iterators from different ranges:
                return (it1_ == that.it1_) && (it2_ == that.it2_);
            }
            bool equal(default_sentinel_t) const
            {
                return (it1_ == end1_) && (it2_ == end2_);
            }
            rvalue_reference_type move() const
                noexcept(noexcept(iter_move(it1_)) && noexcept(iter_move(it2_)))
            {
                if(state == state_t::SECOND || state == state_t::ONLY_SECOND)
                    return iter_move(it2_);
                else
                    return iter_move(it1_);
            }
        };

        constexpr cardinality set_symmetric_difference_cardinality(cardinality c1,
                                                                   cardinality c2)
        {
            return (c1 == unknown) || (c2 == unknown)
                       ? unknown
                       : (c1 == infinite) != (c2 == infinite)
                             ? infinite
                             : (c1 == infinite) && (c2 == infinite) ? unknown : finite;
        }

    } // namespace detail
    /// \endcond

    template<typename Rng1, typename Rng2, typename C, typename P1, typename P2>
    using set_symmetric_difference_view = detail::set_algorithm_view<
        Rng1, Rng2, C, P1, P2, detail::set_symmetric_difference_cursor,
        detail::set_symmetric_difference_cardinality(range_cardinality<Rng1>::value,
                                                     range_cardinality<Rng2>::value)>;

    namespace views
    {
        struct set_symmetric_difference_base_fn
        {
            template(typename Rng1, typename Rng2, typename C = less,
                     typename P1 = identity, typename P2 = identity)(
                requires //
                    viewable_range<Rng1> AND input_range<Rng1> AND
                    viewable_range<Rng2> AND input_range<Rng2> AND
                    common_with<range_value_t<Rng1>, range_value_t<Rng2>> AND
                    common_reference_with<range_reference_t<Rng1>,
                                          range_reference_t<Rng2>> AND
                    common_reference_with<range_rvalue_reference_t<Rng1>,
                                          range_rvalue_reference_t<Rng2>> AND
                    indirect_relation<C,
                                      projected<iterator_t<Rng1>, P1>,
                                      projected<iterator_t<Rng2>, P2>>)
            set_symmetric_difference_view<all_t<Rng1>, all_t<Rng2>, C, P1, P2>
            operator()(Rng1 && rng1,
                       Rng2 && rng2,
                       C pred = C{},
                       P1 proj1 = P1{},
                       P2 proj2 = P2{}) const
            {
                return {all(static_cast<Rng1 &&>(rng1)),
                        all(static_cast<Rng2 &&>(rng2)),
                        std::move(pred),
                        std::move(proj1),
                        std::move(proj2)};
            }
        };

        struct set_symmetric_difference_fn : set_symmetric_difference_base_fn
        {
            using set_symmetric_difference_base_fn::operator();

            template(typename Rng2, typename C = less, typename P1 = identity,
                     typename P2 = identity)(
                requires viewable_range<Rng2> AND input_range<Rng2> AND (!range<C>))
            constexpr auto operator()(Rng2 && rng2,
                                      C && pred = C{},
                                      P1 proj1 = P1{},
                                      P2 proj2 = P2{}) const
            {
                return make_view_closure(bind_back(set_symmetric_difference_base_fn{},
                                                   all(rng2),
                                                   static_cast<C &&>(pred),
                                                   std::move(proj1),
                                                   std::move(proj2)));
            }
        };

        /// \relates set_symmetric_difference_fn
        RANGES_INLINE_VARIABLE(set_symmetric_difference_fn, set_symmetric_difference)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
