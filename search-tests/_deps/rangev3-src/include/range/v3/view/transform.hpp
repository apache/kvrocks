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

#ifndef RANGES_V3_VIEW_TRANSFORM_HPP
#define RANGES_V3_VIEW_TRANSFORM_HPP

#include <iterator>
#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/algorithm/max.hpp>
#include <range/v3/algorithm/min.hpp>
#include <range/v3/functional/bind_back.hpp>
#include <range/v3/functional/indirect.hpp>
#include <range/v3/functional/invoke.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/move.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/adaptor.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        constexpr cardinality transform2_cardinality(cardinality c1, cardinality c2)
        {
            return c1 >= 0 || c2 >= 0
                       ? (c1 >= 0 && c2 >= 0 ? (c1 < c2 ? c1 : c2) : finite)
                       : c1 == finite || c2 == finite
                             ? finite
                             : c1 == unknown || c2 == unknown ? unknown : infinite;
        }

        // clang-format off
        /// \concept iter_transform_1_readable_
        /// \brief The \c iter_transform_1_readable_ concept
        template(typename Fun, typename Rng)(
        concept (iter_transform_1_readable_)(Fun, Rng),
            regular_invocable<Fun &, iterator_t<Rng>> AND
            regular_invocable<Fun &, copy_tag, iterator_t<Rng>> AND
            regular_invocable<Fun &, move_tag, iterator_t<Rng>> AND
            common_reference_with<
                invoke_result_t<Fun &, iterator_t<Rng>> &&,
                invoke_result_t<Fun &, copy_tag, iterator_t<Rng>> &> AND
            common_reference_with<
                invoke_result_t<Fun &, iterator_t<Rng>> &&,
                invoke_result_t<Fun &, move_tag, iterator_t<Rng>> &&> AND
            common_reference_with<
                invoke_result_t<Fun &, move_tag, iterator_t<Rng>> &&,
                invoke_result_t<Fun &, copy_tag, iterator_t<Rng>> const &>
        );
        /// \concept iter_transform_1_readable
        /// \brief The \c iter_transform_1_readable concept
        template<typename Fun, typename Rng>
        CPP_concept iter_transform_1_readable =
            CPP_concept_ref(detail::iter_transform_1_readable_, Fun, Rng);

        /// \concept iter_transform_2_readable_
        /// \brief The \c iter_transform_2_readable_ concept
        template(typename Fun, typename Rng1, typename Rng2)(
        concept (iter_transform_2_readable_)(Fun, Rng1, Rng2),
            regular_invocable<Fun &, iterator_t<Rng1>, iterator_t<Rng2>> AND
            regular_invocable<Fun &, copy_tag, iterator_t<Rng1>, iterator_t<Rng2>> AND
            regular_invocable<Fun &, move_tag, iterator_t<Rng1>, iterator_t<Rng2>> AND
            common_reference_with<
                invoke_result_t<Fun &, iterator_t<Rng1>, iterator_t<Rng2>> &&,
                invoke_result_t<Fun &, copy_tag, iterator_t<Rng1>, iterator_t<Rng2>> &> AND
            common_reference_with<
                invoke_result_t<Fun &, iterator_t<Rng1>, iterator_t<Rng2>> &&,
                invoke_result_t<Fun &, move_tag, iterator_t<Rng1>, iterator_t<Rng2>> &&> AND
            common_reference_with<
                invoke_result_t<Fun &, move_tag, iterator_t<Rng1>, iterator_t<Rng2>> &&,
                invoke_result_t<Fun &, copy_tag, iterator_t<Rng1>, iterator_t<Rng2>> const &>
        );
        /// \concept iter_transform_2_readable
        /// \brief The \c iter_transform_2_readable concept
        template<typename Fun, typename Rng1, typename Rng2>
        CPP_concept iter_transform_2_readable =
            CPP_concept_ref(detail::iter_transform_2_readable_, Fun, Rng1, Rng2);
        // clang-format on
    } // namespace detail
    /// \endcond

    /// \addtogroup group-views
    /// @{
    template<typename Rng, typename Fun>
    struct iter_transform_view : view_adaptor<iter_transform_view<Rng, Fun>, Rng>
    {
    private:
        friend range_access;
        RANGES_NO_UNIQUE_ADDRESS semiregular_box_t<Fun> fun_;
        template<bool Const>
        using use_sentinel_t =
            meta::bool_<!common_range<meta::const_if_c<Const, Rng>> ||
                        single_pass_iterator_<iterator_t<meta::const_if_c<Const, Rng>>>>;

        template<bool IsConst>
        struct adaptor : adaptor_base
        {
        private:
            friend struct adaptor<!IsConst>;
            using CRng = meta::const_if_c<IsConst, Rng>;
            using fun_ref_ = semiregular_box_ref_or_val_t<Fun, IsConst>;
            RANGES_NO_UNIQUE_ADDRESS fun_ref_ fun_;

        public:
            using value_type =
                detail::decay_t<invoke_result_t<Fun &, copy_tag, iterator_t<CRng>>>;
            adaptor() = default;
            adaptor(fun_ref_ fun)
              : fun_(std::move(fun))
            {}
            template(bool Other)(
                requires IsConst AND CPP_NOT(Other)) //
            adaptor(adaptor<Other> that)
              : fun_(std::move(that.fun_))
            {}

            // clang-format off
            auto CPP_auto_fun(read)(iterator_t<CRng> it)(const)
            (
                return invoke(fun_, it)
            )
            auto CPP_auto_fun(iter_move)(iterator_t<CRng> it)(const)
            (
                return invoke(fun_, move_tag{}, it)
            )
            // clang-format on
        };

        adaptor<false> begin_adaptor()
        {
            return {fun_};
        }
        template(bool Const = true)(
            requires Const AND range<meta::const_if_c<Const, Rng>> AND
                detail::iter_transform_1_readable<Fun const,
                                                  meta::const_if_c<Const, Rng>>)
        adaptor<Const> begin_adaptor() const
        {
            return {fun_};
        }
        meta::if_<use_sentinel_t<false>, adaptor_base, adaptor<false>> end_adaptor()
        {
            return {fun_};
        }
        template(bool Const = true)(
            requires Const AND range<meta::const_if_c<Const, Rng>> AND
                    detail::iter_transform_1_readable<Fun const,
                                                      meta::const_if_c<Const, Rng>>)
        meta::if_<use_sentinel_t<Const>, adaptor_base, adaptor<Const>> end_adaptor() const
        {
            return {fun_};
        }

    public:
        iter_transform_view() = default;
        iter_transform_view(Rng rng, Fun fun)
          : iter_transform_view::view_adaptor{std::move(rng)}
          , fun_(std::move(fun))
        {}
        CPP_auto_member
        constexpr auto CPP_fun(size)()(
            requires sized_range<Rng>)
        {
            return ranges::size(this->base());
        }
        CPP_auto_member
        constexpr auto CPP_fun(size)()(const //
            requires sized_range<Rng const>)
        {
            return ranges::size(this->base());
        }
    };

    template<typename Rng, typename Fun>
    struct transform_view : iter_transform_view<Rng, indirected<Fun>>
    {
        transform_view() = default;
        transform_view(Rng rng, Fun fun)
          : iter_transform_view<Rng, indirected<Fun>>{std::move(rng),
                                                      indirect(std::move(fun))}
        {}
    };

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template(typename Rng, typename Fun)(
        requires copy_constructible<Fun>)
    transform_view(Rng &&, Fun)
        -> transform_view<views::all_t<Rng>, Fun>;
#endif

    template<typename Rng1, typename Rng2, typename Fun>
    struct iter_transform2_view
      : view_facade<iter_transform2_view<Rng1, Rng2, Fun>,
                    detail::transform2_cardinality(range_cardinality<Rng1>::value,
                                                   range_cardinality<Rng2>::value)>
    {
    private:
        friend range_access;
        RANGES_NO_UNIQUE_ADDRESS semiregular_box_t<Fun> fun_;
        Rng1 rng1_;
        Rng2 rng2_;
        using difference_type_ =
            common_type_t<range_difference_t<Rng1>, range_difference_t<Rng2>>;

        static constexpr cardinality my_cardinality = detail::transform2_cardinality(
            range_cardinality<Rng1>::value, range_cardinality<Rng2>::value);

        template<bool>
        struct cursor;

        template<bool Const>
        struct sentinel
        {
        private:
            friend struct cursor<Const>;
            sentinel_t<meta::const_if_c<Const, Rng1>> end1_;
            sentinel_t<meta::const_if_c<Const, Rng2>> end2_;

        public:
            sentinel() = default;
            sentinel(meta::const_if_c<Const, iter_transform2_view> * parent,
                     decltype(ranges::end))
              : end1_(end(parent->rng1_))
              , end2_(end(parent->rng2_))
            {}
            template(bool Other)(
                requires Const AND CPP_NOT(Other)) //
            sentinel(sentinel<Other> that)
              : end1_(std::move(that.end1_))
              , end2_(std::move(that.end2_))
            {}
        };

        template<bool Const>
        struct cursor
        {
        private:
            using fun_ref_ = semiregular_box_ref_or_val_t<Fun, Const>;
            using R1 = meta::const_if_c<Const, Rng1>;
            using R2 = meta::const_if_c<Const, Rng2>;
            fun_ref_ fun_;
            iterator_t<R1> it1_;
            iterator_t<R2> it2_;

        public:
            using difference_type = difference_type_;
            using single_pass = meta::or_c<(bool)single_pass_iterator_<iterator_t<R1>>,
                                           (bool)single_pass_iterator_<iterator_t<R2>>>;
            using value_type =
                detail::decay_t<invoke_result_t<meta::const_if_c<Const, Fun> &, copy_tag,
                                                iterator_t<R1>, iterator_t<R2>>>;

            cursor() = default;
            template<typename BeginEndFn>
            cursor(meta::const_if_c<Const, iter_transform2_view> * parent,
                   BeginEndFn begin_end)
              : fun_(parent->fun_)
              , it1_(begin_end(parent->rng1_))
              , it2_(begin_end(parent->rng2_))
            {}
            template(bool Other)(
                requires Const AND CPP_NOT(Other)) //
            cursor(cursor<Other> that)
              : fun_(std::move(that.fun_))
              , it1_(std::move(that.end1_))
              , it2_(std::move(that.end2_))
            {}
            // clang-format off
            auto CPP_auto_fun(read)()(const)
            (
                return invoke(fun_, it1_, it2_)
            )
                // clang-format on
                void next()
            {
                ++it1_;
                ++it2_;
            }
            CPP_member
            auto equal(cursor const & that) const //
                -> CPP_ret(bool)(
                    requires forward_range<Rng1> && forward_range<Rng2>)
            {
                // By returning true if *any* of the iterators are equal, we allow
                // transformed ranges to be of different lengths, stopping when the first
                // one reaches the last.
                return it1_ == that.it1_ || it2_ == that.it2_;
            }
            bool equal(sentinel<Const> const & s) const
            {
                // By returning true if *any* of the iterators are equal, we allow
                // transformed ranges to be of different lengths, stopping when the first
                // one reaches the last.
                return it1_ == s.end1_ || it2_ == s.end2_;
            }
            CPP_member
            auto prev() //
                -> CPP_ret(void)(
                    requires bidirectional_range<R1> && bidirectional_range<R2>)
            {
                --it1_;
                --it2_;
            }
            CPP_member
            auto advance(difference_type n) -> CPP_ret(void)(
                requires random_access_range<R1> && random_access_range<R2>)
            {
                ranges::advance(it1_, n);
                ranges::advance(it2_, n);
            }
            CPP_member
            auto distance_to(cursor const & that) const //
                -> CPP_ret(difference_type)(
                    requires sized_sentinel_for<iterator_t<R1>, iterator_t<R1>> &&
                        sized_sentinel_for<iterator_t<R2>, iterator_t<R2>>)
            {
                // Return the smallest distance (in magnitude) of any of the iterator
                // pairs. This is to accommodate zippers of sequences of different length.
                difference_type d1 = that.it1_ - it1_, d2 = that.it2_ - it2_;
                return 0 < d1 ? ranges::min(d1, d2) : ranges::max(d1, d2);
            }
            // clang-format off
            auto CPP_auto_fun(move)()(const)
            (
                return invoke(fun_, move_tag{}, it1_, it2_)
            )
            // clang-format on
        };

        template<bool Const>
        using end_cursor_t = meta::if_c<
            common_range<meta::const_if_c<Const, Rng1>> &&
                common_range<meta::const_if_c<Const, Rng2>> &&
                !single_pass_iterator_<iterator_t<meta::const_if_c<Const, Rng1>>> &&
                !single_pass_iterator_<iterator_t<meta::const_if_c<Const, Rng2>>>,
            cursor<Const>, sentinel<Const>>;

        cursor<simple_view<Rng1>() && simple_view<Rng2>()> begin_cursor()
        {
            return {this, ranges::begin};
        }
        end_cursor_t<simple_view<Rng1>() && simple_view<Rng2>()> end_cursor()
        {
            return {this, ranges::end};
        }
        template(bool Const = true)(
            requires Const AND range<meta::const_if_c<Const, Rng1>> AND
                range<meta::const_if_c<Const, Rng2>> AND
                detail::iter_transform_2_readable< //
                    Fun const, //
                    meta::const_if_c<Const, Rng1>, //
                    meta::const_if_c<Const, Rng2>>)
        cursor<true> begin_cursor() const
        {
            return {this, ranges::begin};
        }
        template(bool Const = true)(
            requires Const AND range<meta::const_if_c<Const, Rng1>> AND
                range<meta::const_if_c<Const, Rng2>> AND
                detail::iter_transform_2_readable< //
                    Fun const, //
                    meta::const_if_c<Const, Rng1>, //
                    meta::const_if_c<Const, Rng2>>)
        end_cursor_t<Const> end_cursor() const
        {
            return {this, ranges::end};
        }
        template<typename Self>
        static constexpr auto size_(Self & self)
        {
            using size_type = common_type_t<range_size_t<Rng1>, range_size_t<Rng2>>;
            return ranges::min(static_cast<size_type>(ranges::size(self.rng1_)),
                               static_cast<size_type>(ranges::size(self.rng2_)));
        }

        template<bool B>
        using R1 = meta::invoke<detail::dependent_<B>, Rng1>;
        template<bool B>
        using R2 = meta::invoke<detail::dependent_<B>, Rng2>;

    public:
        iter_transform2_view() = default;
        constexpr iter_transform2_view(Rng1 rng1, Rng2 rng2, Fun fun)
          : fun_(std::move(fun))
          , rng1_(std::move(rng1))
          , rng2_(std::move(rng2))
        {}
        CPP_member
        static constexpr auto size() //
            -> CPP_ret(std::size_t)(
                requires (my_cardinality >= 0))
        {
            return static_cast<std::size_t>(my_cardinality);
        }
        template(bool True = true)(
            requires (my_cardinality < 0) AND sized_range<Rng1 const> AND
            sized_range<Rng2 const> AND
            common_with<range_size_t<R1<True>>, range_size_t<R2<True>>>)
            constexpr auto size() const
        {
            return size_(*this);
        }
        template(bool True = true)(
            requires (my_cardinality < 0) AND sized_range<Rng1> AND sized_range<Rng2> AND
            common_with<range_size_t<R1<True>>, range_size_t<R2<True>>>)
            constexpr auto size()
        {
            return size_(*this);
        }
    };

    template<typename Rng1, typename Rng2, typename Fun>
    struct transform2_view : iter_transform2_view<Rng1, Rng2, indirected<Fun>>
    {
        transform2_view() = default;
        constexpr transform2_view(Rng1 rng1, Rng2 rng2, Fun fun)
          : iter_transform2_view<Rng1, Rng2, indirected<Fun>>{std::move(rng1),
                                                              std::move(rng2),
                                                              indirect(std::move(fun))}
        {}
    };

    namespace views
    {
        struct iter_transform_base_fn
        {
            template(typename Rng, typename Fun)(
                requires viewable_range<Rng> AND input_range<Rng> AND
                    copy_constructible<Fun> AND
                    detail::iter_transform_1_readable<Fun, Rng>)
            constexpr iter_transform_view<all_t<Rng>, Fun> //
            operator()(Rng && rng, Fun fun) const
            {
                return {all(static_cast<Rng &&>(rng)), std::move(fun)};
            }

            template(typename Rng1, typename Rng2, typename Fun)(
                requires viewable_range<Rng1> AND input_range<Rng1> AND
                    viewable_range<Rng2> AND input_range<Rng2> AND
                    copy_constructible<Fun> AND
                    common_with<range_difference_t<Rng1>, range_difference_t<Rng1>> AND
                    detail::iter_transform_2_readable<Fun, Rng1, Rng2>)
            constexpr iter_transform2_view<all_t<Rng1>, all_t<Rng2>, Fun> //
            operator()(Rng1 && rng1, Rng2 && rng2, Fun fun) const
            {
                return {all(static_cast<Rng1 &&>(rng1)),
                        all(static_cast<Rng2 &&>(rng2)),
                        std::move(fun)};
            }
        };

        struct iter_transform_fn : iter_transform_base_fn
        {
            using iter_transform_base_fn::operator();

            template<typename Fun>
            constexpr auto operator()(Fun fun) const
            {
                return make_view_closure(
                    bind_back(iter_transform_base_fn{}, std::move(fun)));
            }
        };

        /// \relates iter_transform_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(iter_transform_fn, iter_transform)

        // Don't forget to update views::for_each whenever this set
        // of concepts changes
        // clang-format off
        /// \concept transformable_range_
        /// \brief The \c transformable_range_ concept
        template(typename Rng, typename Fun)(
        concept (transformable_range_)(Rng, Fun),
            regular_invocable<Fun &, range_reference_t<Rng>> AND
            (!std::is_void<indirect_result_t<Fun &, iterator_t<Rng>>>::value)
        );
        /// \concept transformable_range
        /// \brief The \c transformable_range concept
        template<typename Rng, typename Fun>
        CPP_concept transformable_range =
            viewable_range<Rng> && input_range<Rng> &&
            copy_constructible<Fun> &&
            CPP_concept_ref(views::transformable_range_, Rng, Fun);

        /// \concept transformable_ranges_
        /// \brief The \c transformable_ranges_ concept
        template(typename Rng1, typename Rng2, typename Fun)(
        concept (transformable_ranges_)(Rng1, Rng2, Fun),
            regular_invocable<Fun &, range_reference_t<Rng1>, range_reference_t<Rng2>> AND
            (!std::is_void<
                indirect_result_t<Fun &, iterator_t<Rng1>, iterator_t<Rng2>>>::value)
        );
        /// \concept transformable_ranges
        /// \brief The \c transformable_ranges concept
        template<typename Rng1, typename Rng2, typename Fun>
        CPP_concept transformable_ranges =
            viewable_range<Rng1> && input_range<Rng1> &&
            viewable_range<Rng2> && input_range<Rng2> &&
            copy_constructible<Fun> &&
            CPP_concept_ref(views::transformable_ranges_, Rng1, Rng2, Fun);
        // clang-format on

        struct transform_base_fn
        {
            template(typename Rng, typename Fun)(
                requires transformable_range<Rng, Fun>)
            constexpr transform_view<all_t<Rng>, Fun> operator()(Rng && rng, Fun fun)
                const
            {
                return {all(static_cast<Rng &&>(rng)), std::move(fun)};
            }

            template(typename Rng1, typename Rng2, typename Fun)(
                requires transformable_ranges<Rng1, Rng2, Fun>)
            constexpr transform2_view<all_t<Rng1>, all_t<Rng2>, Fun> //
            operator()(Rng1 && rng1, Rng2 && rng2, Fun fun) const
            {
                return {all(static_cast<Rng1 &&>(rng1)),
                        all(static_cast<Rng2 &&>(rng2)),
                        std::move(fun)};
            }
        };

        /// # ranges::views::transform
        /// The transform view takes in a function `T -> U` and converts an input
        /// range of `T` into an output range of `U` by calling the function on every
        /// element of the input range.
        ///
        /// ## Example
        /// \snippet example/view/transform.cpp transform example
        ///
        /// ### Output
        /// \include example/view/transform_golden.txt
        ///
        /// ## Syntax
        /// ```cpp
        /// auto output_range = input_range | ranges::views::transform(transform_func);
        /// ```
        ///
        /// ## Parameters
        /// <pre><b>transform_func</b></pre>
        ///   - Maps an input value to an output value (`transform_func(T) -> U`)
        ///
        /// <pre><b>input_range</b></pre>
        ///   - The range of elements to transform
        ///   - Reference type: `T`
        ///
        /// <pre><b>output_range</b></pre>
        ///   - The range of output values
        ///   - Reference type: `U`
        ///   - Value type: `decay_t<U>`
        ///   - This range will have the same category as the input range (excluding
        ///   contiguous ranges). Contiguous ranges are reduced to random access ranges.
        ///
        struct transform_fn : transform_base_fn
        {
            using transform_base_fn::operator();

            template<typename Fun>
            constexpr auto operator()(Fun fun) const
            {
                return make_view_closure(bind_back(transform_base_fn{}, std::move(fun)));
            }
        };

        /// \relates transform_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(transform_fn, transform)
    } // namespace views

    namespace cpp20
    {
        namespace views
        {
            using ranges::views::transform;
        }
        template(typename Rng, typename F)(
            requires input_range<Rng> AND copy_constructible<F> AND view_<Rng> AND
                std::is_object<F>::value AND
                    regular_invocable<F &, iter_reference_t<iterator_t<Rng>>>)
            using transform_view = ranges::transform_view<Rng, F>;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::iter_transform_view)
RANGES_SATISFY_BOOST_RANGE(::ranges::transform_view)

#endif
