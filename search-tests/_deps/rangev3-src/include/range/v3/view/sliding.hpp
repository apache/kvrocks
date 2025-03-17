/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Tobias Mayer 2016
//  Copyright Casey Carter 2016
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_VIEW_SLIDING_HPP
#define RANGES_V3_VIEW_SLIDING_HPP

#include <utility>

#include <meta/meta.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/bind_back.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/optional.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/adaptor.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/counted.hpp>
#include <range/v3/view/view.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \cond
    namespace sliding_view_detail
    {
        enum class cache
        {
            none,
            first,
            last
        };

        template<typename Rng>
        using caching = std::integral_constant<
            cache, random_access_range<Rng> && sized_range<Rng>
                       ? cache::none
                       : bidirectional_range<Rng> && common_range<Rng> ? cache::last
                                                                       : cache::first>;
    } // namespace sliding_view_detail
    /// \endcond

    template<typename Rng,
             sliding_view_detail::cache = sliding_view_detail::caching<Rng>::value>
    struct sliding_view;

    /// \cond
    namespace sliding_view_detail
    {
        template<typename Rng>
        using uncounted_t =
            decltype(ranges::uncounted(std::declval<iterator_t<Rng> &>()));

        template<typename Rng, bool = (bool)random_access_range<Rng>>
        struct trailing
        {
            trailing() = default;
            constexpr trailing(Rng & rng)
              : it_{uncounted(ranges::begin(rng))}
            {}
            constexpr uncounted_t<Rng> get(iterator_t<Rng> const &,
                                           range_difference_t<Rng>) const
            {
                return it_;
            }
            void next()
            {
                ++it_;
            }
            CPP_member
            auto prev() //
                -> CPP_ret(void)(
                    requires bidirectional_range<Rng>)
            {
                --it_;
            }

        private:
            uncounted_t<Rng> it_;
        };

        template<typename Rng>
        struct trailing<Rng, true>
        {
            trailing() = default;
            constexpr trailing(Rng &) noexcept
            {}
            constexpr uncounted_t<Rng> get(iterator_t<Rng> const & it,
                                           range_difference_t<Rng> n) const
            {
                return uncounted(it - (n - 1));
            }
            void next()
            {}
            void prev()
            {}
        };

        template<typename Rng>
        struct RANGES_EMPTY_BASES sv_base
          : view_adaptor<sliding_view<Rng>, Rng,
                         is_finite<Rng>::value ? finite : range_cardinality<Rng>::value>
          , private detail::non_propagating_cache<iterator_t<Rng>, sv_base<Rng>,
                                                  caching<Rng>::value != cache::none>
        {
            CPP_assert(forward_range<Rng>);
            sv_base() = default;
            sv_base(Rng rng, range_difference_t<Rng> n)
              : sv_base::view_adaptor(std::move(rng))
              , n_(n)
            {
                RANGES_ASSERT(0 < n_);
            }
            CPP_auto_member
            auto CPP_fun(size)()(const //
                requires sized_range<Rng const>)
            {
                auto const count = ranges::size(this->base());
                auto const n = static_cast<range_size_t<Rng const>>(n_);
                return count < n ? 0 : count - n + 1;
            }
            CPP_auto_member
            auto CPP_fun(size)()(
                requires sized_range<Rng>)
            {
                auto const count = ranges::size(this->base());
                auto const n = static_cast<range_size_t<Rng>>(n_);
                return count < n ? 0 : count - n + 1;
            }

        protected:
            range_difference_t<Rng> n_;

            optional<iterator_t<Rng>> & cache() &
            {
                return static_cast<cache_t &>(*this);
            }
            optional<iterator_t<Rng>> const & cache() const &
            {
                return static_cast<cache_t const &>(*this);
            }

        private:
            using cache_t = detail::non_propagating_cache<iterator_t<Rng>, sv_base<Rng>>;
        };
    } // namespace sliding_view_detail
    /// \endcond

    /// \addtogroup group-views
    /// @{
    template<typename Rng>
    struct sliding_view<Rng, sliding_view_detail::cache::first>
      : sliding_view_detail::sv_base<Rng>
    {
    private:
        friend range_access;

        iterator_t<Rng> get_first()
        {
            auto & first = this->cache();
            if(!first)
            {
                first = ranges::next(
                    ranges::begin(this->base()), this->n_ - 1, ranges::end(this->base()));
            }
            return *first;
        }

        struct RANGES_EMPTY_BASES adaptor
          : adaptor_base
          , sliding_view_detail::trailing<Rng>
        {
        private:
            using base_t = sliding_view_detail::trailing<Rng>;
            range_difference_t<Rng> n_ = {};

        public:
            adaptor() = default;
            adaptor(sliding_view * v)
              : base_t{v->base()}
              , n_{v->n_}
            {}
            iterator_t<Rng> begin(sliding_view & v)
            {
                return v.get_first();
            }
            auto read(iterator_t<Rng> const & it) const
                -> decltype(views::counted(uncounted(it), n_))
            {
                return views::counted(base_t::get(it, n_), n_);
            }
            void next(iterator_t<Rng> & it)
            {
                ++it;
                base_t::next();
            }
            CPP_member
            auto prev(iterator_t<Rng> & it) //
                -> CPP_ret(void)(
                    requires bidirectional_range<Rng>)
            {
                base_t::prev();
                --it;
            }
            CPP_member
            auto advance(iterator_t<Rng> & it, range_difference_t<Rng> n)
                -> CPP_ret(void)(
                    requires random_access_range<Rng>)
            {
                it += n;
            }
        };

        adaptor begin_adaptor()
        {
            return {this};
        }
        meta::if_c<common_range<Rng>, adaptor, adaptor_base> end_adaptor()
        {
            return {this};
        }

    public:
        using sliding_view::sv_base::sv_base;
    };

    template<typename Rng>
    struct sliding_view<Rng, sliding_view_detail::cache::last>
      : sliding_view_detail::sv_base<Rng>
    {
    private:
        friend range_access;

        iterator_t<Rng> get_last()
        {
            auto & last = this->cache();
            if(!last)
            {
                last = ranges::prev(
                    ranges::end(this->base()), this->n_ - 1, ranges::begin(this->base()));
            }
            return *last;
        }

        struct adaptor : adaptor_base
        {
        private:
            range_difference_t<Rng> n_ = {};

        public:
            adaptor() = default;
            adaptor(sliding_view * v)
              : n_{v->n_}
            {}
            iterator_t<Rng> end(sliding_view & v)
            {
                return v.get_last();
            }
            auto read(iterator_t<Rng> const & it) const
                -> decltype(views::counted(uncounted(it), n_))
            {
                return views::counted(uncounted(it), n_);
            }
        };

        adaptor begin_adaptor()
        {
            return {this};
        }
        adaptor end_adaptor()
        {
            return {this};
        }

    public:
        using sliding_view::sv_base::sv_base;
    };

    template<typename Rng>
    struct sliding_view<Rng, sliding_view_detail::cache::none>
      : sliding_view_detail::sv_base<Rng>
    {
    private:
        friend range_access;

        template<bool Const>
        struct adaptor : adaptor_base
        {
        private:
            friend adaptor<!Const>;
            using CRng = meta::const_if_c<Const, Rng>;
            range_difference_t<Rng> n_ = 0;

        public:
            adaptor() = default;
            adaptor(range_difference_t<Rng> n)
              : n_(n)
            {}
            template(bool Other)(
                requires Const AND CPP_NOT(Other)) //
            adaptor(adaptor<Other> that)
              : n_(that.n_)
            {}
            iterator_t<CRng> end(meta::const_if_c<Const, sliding_view> & v) const
            {
                auto const sz = ranges::distance(v.base());
                auto const offset = n_ - 1 < sz ? n_ - 1 : sz;
                return ranges::begin(v.base()) + (sz - offset);
            }
            auto read(iterator_t<CRng> const & it) const
                -> decltype(views::counted(uncounted(it), n_))
            {
                return views::counted(uncounted(it), n_);
            }
        };

        adaptor<simple_view<Rng>()> begin_adaptor()
        {
            return {this->n_};
        }
        CPP_member
        auto begin_adaptor() const //
            -> CPP_ret(adaptor<true>)(
                requires range<Rng const>)
        {
            return {this->n_};
        }
        adaptor<simple_view<Rng>()> end_adaptor()
        {
            return {this->n_};
        }
        CPP_member
        auto end_adaptor() const //
            -> CPP_ret(adaptor<true>)(
                requires range<Rng const>)
        {
            return {this->n_};
        }

    public:
        using sliding_view::sv_base::sv_base;
    };

    template<typename Rng>
    RANGES_INLINE_VAR constexpr bool enable_borrowed_range<sliding_view<Rng>> = //
        enable_borrowed_range<Rng>;

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template<typename Rng>
    sliding_view(Rng &&, range_difference_t<Rng>)
        -> sliding_view<views::all_t<Rng>>;
#endif

    namespace views
    {
        // In:  range<T>
        // Out: range<range<T>>, where each inner range has $n$ elements.
        struct sliding_base_fn
        {
            template(typename Rng)(
                requires viewable_range<Rng> AND forward_range<Rng>)
            constexpr sliding_view<all_t<Rng>> //
            operator()(Rng && rng, range_difference_t<Rng> n) const
            {
                return {all(static_cast<Rng &&>(rng)), n};
            }
        };

        struct sliding_fn : sliding_base_fn
        {
            using sliding_base_fn::operator();

            template<typename Int>
            constexpr auto CPP_fun(operator())(Int n)(const //
                                                      requires detail::integer_like_<Int>)
            {
                return make_view_closure(bind_back(sliding_base_fn{}, n));
            }
        };

        /// \relates sliding_fn
        /// \ingroup group-views
        RANGES_INLINE_VARIABLE(sliding_fn, sliding)
    } // namespace views
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
