/// \file
// Range v3 library
//
// Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_VIEW_ADAPTOR_HPP
#define RANGES_V3_VIEW_ADAPTOR_HPP

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/compressed_pair.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/facade.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename Derived>
        using begin_adaptor_t = detail::decay_t<decltype(
            range_access::begin_adaptor(std::declval<Derived &>()))>;

        template<typename Derived>
        using end_adaptor_t = detail::decay_t<decltype(
            range_access::end_adaptor(std::declval<Derived &>()))>;

        template<typename Derived>
        using adapted_iterator_t = detail::decay_t<decltype(
            std::declval<begin_adaptor_t<Derived>>().begin(std::declval<Derived &>()))>;

        template<typename Derived>
        using adapted_sentinel_t = detail::decay_t<decltype(
            std::declval<end_adaptor_t<Derived>>().end(std::declval<Derived &>()))>;

        struct adaptor_base_current_mem_fn
        {};

        template<typename BaseIter, typename Adapt>
        constexpr int which_adaptor_value_(priority_tag<0>)
        {
            return 0;
        }
        template<typename BaseIter, typename Adapt>
        constexpr always_<int, decltype(Adapt::read(std::declval<BaseIter const &>(),
                                                    adaptor_base_current_mem_fn{}))> //
        which_adaptor_value_(priority_tag<1>)
        {
            return 1;
        }
        template<typename BaseIter, typename Adapt>
        constexpr always_<int, typename Adapt::value_type> //
        which_adaptor_value_(priority_tag<2>)
        {
            return 2;
        }

        template<typename BaseIter, typename Adapt,
                 int = detail::which_adaptor_value_<BaseIter, Adapt>(priority_tag<2>{})>
        struct adaptor_value_type_
        {
            compressed_pair<BaseIter, Adapt> data_;
        };
        template<typename BaseIter, typename Adapt>
        struct adaptor_value_type_<BaseIter, Adapt, 1>
        {
            using value_type = iter_value_t<BaseIter>;
            compressed_pair<BaseIter, Adapt> data_;
        };
        template<typename BaseIter, typename Adapt>
        struct adaptor_value_type_<BaseIter, Adapt, 2>
        {
#ifdef RANGES_WORKAROUND_MSVC_688606
            using value_type = typename indirectly_readable_traits<Adapt>::value_type;
#else  // ^^^ workaround ^^^ / vvv no workaround vvv
            using value_type = typename Adapt::value_type;
#endif // RANGES_WORKAROUND_MSVC_688606
            compressed_pair<BaseIter, Adapt> data_;
        };
    } // namespace detail
    /// \endcond

    /// \addtogroup group-views
    /// @{
    template<typename BaseIt, typename Adapt>
    struct adaptor_cursor;

    template<typename BaseSent, typename Adapt>
    struct base_adaptor_sentinel;

    struct adaptor_base
    {
        adaptor_base() = default;
        adaptor_base(adaptor_base &&) = default;
        adaptor_base(adaptor_base const &) = default;
        adaptor_base & operator=(adaptor_base &&) = default;
        adaptor_base & operator=(adaptor_base const &) = default;

        adaptor_base(detail::ignore_t, detail::ignore_t = {}, detail::ignore_t = {})
        {}
        // clang-format off
        template<typename Rng>
        static constexpr auto CPP_auto_fun(begin)(Rng &rng)
        (
            return ranges::begin(rng.base())
        )
        template<typename Rng>
        static constexpr auto CPP_auto_fun(end)(Rng &rng)
        (
            return ranges::end(rng.base())
        )
            // clang-format on
            template(typename I)(
                requires equality_comparable<I>)
            static bool equal(I const & it0, I const & it1)
        {
            return it0 == it1;
        }
        template(typename I)(
            requires input_or_output_iterator<I>)
        static iter_reference_t<I> read(I const & it,
                                        detail::adaptor_base_current_mem_fn = {})
            noexcept(noexcept(iter_reference_t<I>(*it)))
        {
            return *it;
        }
        template(typename I)(
            requires input_or_output_iterator<I>)
        static void next(I & it)
        {
            ++it;
        }
        template(typename I)(
            requires bidirectional_iterator<I>)
        static void prev(I & it)
        {
            --it;
        }
        template(typename I)(
            requires random_access_iterator<I>)
        static void advance(I & it, iter_difference_t<I> n)
        {
            it += n;
        }
        template(typename I)(
            requires sized_sentinel_for<I, I>)
        static iter_difference_t<I> distance_to(I const & it0, I const & it1)
        {
            return it1 - it0;
        }
        template(typename I, typename S)(
            requires sentinel_for<S, I>)
        static constexpr bool empty(I const & it, S const & last)
        {
            return it == last;
        }
    };

    // Build a sentinel out of a sentinel into the adapted range, and an
    // adaptor that customizes behavior.
    template<typename BaseSent, typename Adapt>
    struct base_adaptor_sentinel
    {
    private:
        template<typename, typename>
        friend struct adaptor_cursor;
        RANGES_NO_UNIQUE_ADDRESS compressed_pair<BaseSent, Adapt> data_;

    public:
        base_adaptor_sentinel() = default;
        base_adaptor_sentinel(BaseSent sent, Adapt adapt)
          : data_{std::move(sent), std::move(adapt)}
        {}

        // All sentinels into adapted ranges have a base() member for fetching
        // the underlying sentinel.
        BaseSent base() const
        {
            return data_.first();
        }

    protected:
        // Adaptor accessor
        Adapt & get()
        {
            return data_.second();
        }
        Adapt const & get() const
        {
            return data_.second();
        }
    };

    /// \cond
    namespace detail
    {
        template<typename BaseSent, typename Adapt>
        meta::id<base_adaptor_sentinel<BaseSent, Adapt>> base_adaptor_sentinel_2_(long);

        template<typename BaseSent, typename Adapt>
        meta::id<typename Adapt::template mixin<base_adaptor_sentinel<BaseSent, Adapt>>>
        base_adaptor_sentinel_2_(int);

        template<typename BaseSent, typename Adapt>
        struct base_adaptor_sentinel_
          : decltype(base_adaptor_sentinel_2_<BaseSent, Adapt>(42))
        {};

        template<typename BaseSent, typename Adapt>
        using adaptor_sentinel_ = meta::_t<base_adaptor_sentinel_<BaseSent, Adapt>>;
    } // namespace detail
    /// \endcond

    template<typename BaseSent, typename Adapt>
    struct adaptor_sentinel : detail::adaptor_sentinel_<BaseSent, Adapt>
    {
        using detail::adaptor_sentinel_<BaseSent, Adapt>::adaptor_sentinel_;
    };

    // Build a cursor out of an iterator into the adapted range, and an
    // adaptor that customizes behavior.
    template<typename BaseIter, typename Adapt>
    struct adaptor_cursor : private detail::adaptor_value_type_<BaseIter, Adapt>
    {
    private:
        friend range_access;
        template<typename, typename>
        friend struct adaptor_cursor;
        using base_t = detail::adaptor_value_type_<BaseIter, Adapt>;
        using single_pass = meta::bool_<(bool)range_access::single_pass_t<Adapt>() ||
                                        (bool)single_pass_iterator_<BaseIter>>;

        struct basic_adaptor_mixin : basic_mixin<adaptor_cursor>
        {
            basic_adaptor_mixin() = default;
            #ifndef _MSC_VER
            using basic_mixin<adaptor_cursor>::basic_mixin;
            #else
            constexpr explicit basic_adaptor_mixin(adaptor_cursor && cur)
              : basic_mixin<adaptor_cursor>(static_cast<adaptor_cursor &&>(cur))
            {}
            constexpr explicit basic_adaptor_mixin(adaptor_cursor const & cur)
              : basic_mixin<adaptor_cursor>(cur)
            {}
            #endif
            // All iterators into adapted ranges have a base() member for fetching
            // the underlying iterator.
            BaseIter base() const
            {
                return basic_adaptor_mixin::basic_mixin::get().data_.first();
            }

        protected:
            // Adaptor accessor
            Adapt & get()
            {
                return basic_adaptor_mixin::basic_mixin::get().data_.second();
            }
            const Adapt & get() const
            {
                return basic_adaptor_mixin::basic_mixin::get().data_.second();
            }
        };

        template<typename Adapt_>
        static meta::id<basic_adaptor_mixin> basic_adaptor_mixin_2_(long);

        template<typename Adapt_>
        static meta::id<typename Adapt_::template mixin<basic_adaptor_mixin>>
        basic_adaptor_mixin_2_(int);

        using mixin = meta::_t<decltype(basic_adaptor_mixin_2_<Adapt>(42))>;

        template<typename A = Adapt, typename R = decltype(std::declval<A const &>().read(
                                         std::declval<BaseIter const &>()))>
        R read() const noexcept(
            noexcept(std::declval<A const &>().read(std::declval<BaseIter const &>())))
        {
            using V = range_access::cursor_value_t<adaptor_cursor>;
            static_assert(common_reference_with<R &&, V &>,
                          "In your adaptor, you've specified a value type that does not "
                          "share a common reference type with the return type of read.");
            return this->data_.second().read(this->data_.first());
        }
        template<typename A = Adapt, typename = decltype(std::declval<A &>().next(
                                         std::declval<BaseIter &>()))>
        void next()
        {
            this->data_.second().next(this->data_.first());
        }
        template<typename A = Adapt,
                 typename = decltype(std::declval<A const &>().equal(
                     std::declval<BaseIter const &>(), std::declval<BaseIter const &>(),
                     std::declval<A const &>()))>
        bool equal_(adaptor_cursor const & that, int) const
        {
            return this->data_.second().equal(
                this->data_.first(), that.data_.first(), that.data_.second());
        }
        template<typename A = Adapt,
                 typename = decltype(std::declval<A const &>().equal(
                     std::declval<BaseIter const &>(), std::declval<BaseIter const &>()))>
        bool equal_(adaptor_cursor const & that, long) const
        {
            return this->data_.second().equal(this->data_.first(), that.data_.first());
        }
        template<typename C = adaptor_cursor>
        auto equal(adaptor_cursor const & that) const
            -> decltype(std::declval<C const &>().equal_(that, 42))
        {
            return this->equal_(that, 42);
        }
        template<typename S, typename A,
                 typename = decltype(std::declval<A const &>().empty(
                     std::declval<BaseIter const &>(), std::declval<Adapt const &>(),
                     std::declval<S const &>()))>
        constexpr bool equal_(adaptor_sentinel<S, A> const & that, int) const
        {
            return that.data_.second().empty(
                this->data_.first(), this->data_.second(), that.data_.first());
        }
        template<typename S, typename A,
                 typename = decltype(std::declval<A const &>().empty(
                     std::declval<BaseIter const &>(), std::declval<S const &>()))>
        constexpr bool equal_(adaptor_sentinel<S, A> const & that, long) const
        {
            return that.data_.second().empty(this->data_.first(), that.data_.first());
        }
        template<typename S, typename A>
        constexpr auto equal(adaptor_sentinel<S, A> const & that) const
            -> decltype(std::declval<adaptor_cursor const &>().equal_(that, 42))
        {
            return this->equal_(that, 42);
        }
        template<typename A = Adapt, typename = decltype(std::declval<A &>().prev(
                                         std::declval<BaseIter &>()))>
        void prev()
        {
            this->data_.second().prev(this->data_.first());
        }
        template<typename A = Adapt, typename = decltype(std::declval<A &>().advance(
                                         std::declval<BaseIter &>(), 0))>
        void advance(iter_difference_t<BaseIter> n)
        {
            this->data_.second().advance(this->data_.first(), n);
        }
        template<typename A = Adapt,
                 typename R = decltype(std::declval<A const &>().distance_to(
                     std::declval<BaseIter const &>(), std::declval<BaseIter const &>(),
                     std::declval<A const &>()))>
        R distance_to_(adaptor_cursor const & that, int) const
        {
            return this->data_.second().distance_to(
                this->data_.first(), that.data_.first(), that.data_.second());
        }
        template<typename A = Adapt,
                 typename R = decltype(std::declval<A const &>().distance_to(
                     std::declval<BaseIter const &>(), std::declval<BaseIter const &>()))>
        R distance_to_(adaptor_cursor const & that, long) const
        {
            return this->data_.second().distance_to(this->data_.first(),
                                                    that.data_.first());
        }
        template<typename C = adaptor_cursor>
        auto distance_to(adaptor_cursor const & that) const
            -> decltype(std::declval<C const &>().distance_to_(that, 42))
        {
            return this->distance_to_(that, 42);
        }
        // If the adaptor has an iter_move function, use it.
        template<typename A = Adapt,
                 typename X = decltype(std::declval<A const &>().iter_move(
                     std::declval<BaseIter const &>()))>
        X iter_move_(int) const noexcept(noexcept(
            std::declval<A const &>().iter_move(std::declval<BaseIter const &>())))
        {
            using V = range_access::cursor_value_t<adaptor_cursor>;
            using R = decltype(this->data_.second().read(this->data_.first()));
            static_assert(
                common_reference_with<X &&, V const &>,
                "In your adaptor, the result of your iter_move member function does "
                "not share a common reference with your value type.");
            static_assert(
                common_reference_with<R &&, X &&>,
                "In your adaptor, the result of your iter_move member function does "
                "not share a common reference with the result of your read member "
                "function.");
            return this->data_.second().iter_move(this->data_.first());
        }
        // If there is no iter_move member and the adaptor has not overridden the read
        // member function, then dispatch to the base iterator's iter_move function.
        template<typename A = Adapt,
                 typename R = decltype(std::declval<A const &>().read(
                     std::declval<BaseIter const &>(),
                     detail::adaptor_base_current_mem_fn{})),
                 typename X = iter_rvalue_reference_t<BaseIter>>
        X iter_move_(long) const
            noexcept(noexcept(X(ranges::iter_move(std::declval<BaseIter const &>()))))
        {
            return ranges::iter_move(this->data_.first());
        }
        // If the adaptor does not have an iter_move function but overrides the read
        // member function, apply std::move to the result of calling read.
        template<typename A = Adapt,
                 typename R = decltype(
                     std::declval<A const &>().read(std::declval<BaseIter const &>())),
                 typename X = aux::move_t<R>>
        X iter_move_(detail::ignore_t) const noexcept(noexcept(X(static_cast<X &&>(
            std::declval<A const &>().read(std::declval<BaseIter const &>())))))
        {
            using V = range_access::cursor_value_t<adaptor_cursor>;
            static_assert(
                common_reference_with<X &&, V const &>,
                "In your adaptor, you've specified a value type that does not share a "
                "common "
                "reference type with the result of moving the result of the read member "
                "function. Consider defining an iter_move function in your adaptor.");
            return static_cast<X &&>(this->data_.second().read(this->data_.first()));
        }
        // Gives users a way to override the default iter_move function in their adaptors.
        auto move() const
            noexcept(noexcept(std::declval<const adaptor_cursor &>().iter_move_(42)))
                -> decltype(std::declval<const adaptor_cursor &>().iter_move_(42))
        {
            return iter_move_(42);
        }

    public:
        adaptor_cursor() = default;
        adaptor_cursor(BaseIter iter, Adapt adapt)
          : base_t{{std::move(iter), std::move(adapt)}}
        {}
        template(typename OtherIter, typename OtherAdapt)(
            requires //
                (!same_as<adaptor_cursor<OtherIter, OtherAdapt>, adaptor_cursor>) AND
                convertible_to<OtherIter, BaseIter> AND
                convertible_to<OtherAdapt, Adapt>)
        adaptor_cursor(adaptor_cursor<OtherIter, OtherAdapt> that)
          : base_t{{std::move(that.data_.first()), std::move(that.data_.second())}}
        {}
    };

    template<typename D>
    using adaptor_cursor_t =
        adaptor_cursor<detail::adapted_iterator_t<D>, detail::begin_adaptor_t<D>>;

    template<typename D>
    using adaptor_sentinel_t = meta::if_c<
        same_as<detail::adapted_iterator_t<D>, detail::adapted_sentinel_t<D>> &&
            same_as<detail::begin_adaptor_t<D>, detail::end_adaptor_t<D>>,
        adaptor_cursor_t<D>,
        adaptor_sentinel<detail::adapted_sentinel_t<D>, detail::end_adaptor_t<D>>>;

    template<typename Derived, typename BaseRng,
             cardinality Cardinality /*= range_cardinality<BaseRng>::value*/>
    struct view_adaptor : view_facade<Derived, Cardinality>
    {
    private:
        friend Derived;
        friend range_access;
        friend adaptor_base;
        CPP_assert(viewable_range<BaseRng>);
        using base_range_t = views::all_t<BaseRng>;
        using view_facade<Derived, Cardinality>::derived;

        base_range_t rng_;

        constexpr adaptor_base begin_adaptor() const noexcept
        {
            return {};
        }
        constexpr adaptor_base end_adaptor() const noexcept
        {
            return {};
        }

        template<typename D>
        static constexpr adaptor_cursor_t<D> begin_cursor_(D & d) noexcept(noexcept(
            adaptor_cursor_t<D>{std::declval<detail::begin_adaptor_t<D> &>().begin(d),
                                range_access::begin_adaptor(d)}))
        {
            auto adapt = range_access::begin_adaptor(d);
            auto pos = adapt.begin(d);
            return {std::move(pos), std::move(adapt)};
        }
        template(typename D = Derived)(
            requires same_as<D, Derived>)
        constexpr auto begin_cursor() noexcept(
            noexcept(view_adaptor::begin_cursor_(std::declval<D &>())))
            -> decltype(view_adaptor::begin_cursor_(std::declval<D &>()))
        {
            return view_adaptor::begin_cursor_(derived());
        }
        template(typename D = Derived)(
            requires same_as<D, Derived> AND range<base_range_t const>)
        constexpr auto begin_cursor() const
            noexcept(noexcept(view_adaptor::begin_cursor_(std::declval<D const &>())))
                -> decltype(view_adaptor::begin_cursor_(std::declval<D const &>()))
        {
            return view_adaptor::begin_cursor_(derived());
        }

        template<typename D>
        static constexpr adaptor_sentinel_t<D> end_cursor_(D & d) noexcept(noexcept(
            adaptor_sentinel_t<D>{std::declval<detail::end_adaptor_t<D> &>().end(d),
                                  range_access::end_adaptor(d)}))
        {
            auto adapt = range_access::end_adaptor(d);
            auto pos = adapt.end(d);
            return {std::move(pos), std::move(adapt)};
        }
        template(typename D = Derived)(
            requires same_as<D, Derived>)
        constexpr auto end_cursor() noexcept(
            noexcept(view_adaptor::end_cursor_(std::declval<D &>())))
            -> decltype(view_adaptor::end_cursor_(std::declval<D &>()))
        {
            return view_adaptor::end_cursor_(derived());
        }
        template(typename D = Derived)(
            requires same_as<D, Derived> AND range<base_range_t const>)
        constexpr auto end_cursor() const noexcept(
            noexcept(view_adaptor::end_cursor_(std::declval<D const &>())))
            -> decltype(view_adaptor::end_cursor_(std::declval<D const &>()))
        {
            return view_adaptor::end_cursor_(derived());
        }

    protected:
        ~view_adaptor() = default;

    public:
        view_adaptor() = default;
        view_adaptor(view_adaptor &&) = default;
        view_adaptor(view_adaptor const &) = default;
        view_adaptor & operator=(view_adaptor &&) = default;
        view_adaptor & operator=(view_adaptor const &) = default;
        constexpr explicit view_adaptor(BaseRng && rng)
          : rng_(views::all(static_cast<BaseRng &&>(rng)))
        {}
        constexpr base_range_t & base() noexcept
        {
            return rng_;
        }
        /// \overload
        constexpr base_range_t const & base() const noexcept
        {
            return rng_;
        }
    };

    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
