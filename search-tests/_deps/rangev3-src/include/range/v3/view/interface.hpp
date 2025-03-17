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
#ifndef RANGES_V3_VIEW_INTERFACE_HPP
#define RANGES_V3_VIEW_INTERFACE_HPP

#include <iosfwd>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/common_iterator.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>

#include <range/v3/detail/prologue.hpp>

#if defined(RANGES_WORKAROUND_GCC_91525)
#define CPP_template_gcc_workaround CPP_template_sfinae
#else
#define CPP_template_gcc_workaround template
#endif

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename From, typename To = From>
        struct slice_bounds
        {
            From from;
            To to;
            template(typename F, typename T)(
                requires convertible_to<F, From> AND convertible_to<T, To>)
            constexpr slice_bounds(F f, T t)
              : from(static_cast<From>(f))
              , to(static_cast<To>(t))
            {}
        };

        template<typename Int>
        struct from_end_
        {
            Int dist_;

            constexpr explicit from_end_(Int dist)
              : dist_(dist)
            {}

            template(typename Other)(
                requires integer_like_<Other> AND explicitly_convertible_to<Other, Int>)
            constexpr operator from_end_<Other>() const
            {
                return from_end_<Other>{static_cast<Other>(dist_)};
            }
        };

        template<typename Rng>
        using from_end_of_t = from_end_<range_difference_t<Rng>>;

        // clang-format off
        /// \concept _can_empty_
        /// \brief The \c _can_empty_ concept
        template<typename Rng>
        CPP_requires(_can_empty_,
            requires(Rng & rng) //
            (
                ranges::empty(rng)
            ));
        /// \concept can_empty_
        /// \brief The \c can_empty_ concept
        template<typename Rng>
        CPP_concept can_empty_ = //
            CPP_requires_ref(detail::_can_empty_, Rng);
        // clang-format on

        template<cardinality C>
        RANGES_INLINE_VAR constexpr bool has_fixed_size_ = (C >= 0 || C == infinite);

        template<bool>
        struct dependent_
        {
            template<typename T>
            using invoke = T;
        };

        template<typename Stream, typename Rng>
        Stream & print_rng_(Stream & sout, Rng & rng)
        {
            sout << '[';
            auto it = ranges::begin(rng);
            auto const e = ranges::end(rng);
            if(it != e)
            {
                for(;;)
                {
                    sout << *it;
                    if(++it == e)
                        break;
                    sout << ',';
                }
            }
            sout << ']';
            return sout;
        }
    } // namespace detail
    /// \endcond

    /// \addtogroup group-views
    /// @{
    template<typename Derived, cardinality Cardinality /* = finite*/>
    struct view_interface : basic_view<Cardinality>
    {
    protected:
        template<bool B>
        using D = meta::invoke<detail::dependent_<B>, Derived>;

        constexpr Derived & derived() noexcept
        {
            CPP_assert(derived_from<Derived, view_interface>);
            return static_cast<Derived &>(*this);
        }
        /// \overload
        constexpr Derived const & derived() const noexcept
        {
            CPP_assert(derived_from<Derived, view_interface>);
            return static_cast<Derived const &>(*this);
        }

    public:
        view_interface() = default;
        view_interface(view_interface &&) = default;
        view_interface(view_interface const &) = default;
        view_interface & operator=(view_interface &&) = default;
        view_interface & operator=(view_interface const &) = default;
        /// \brief Test whether a range can be empty:
        CPP_member
        constexpr auto empty() const noexcept //
            -> CPP_ret(bool)(
                requires (detail::has_fixed_size_<Cardinality>))
        {
            return Cardinality == 0;
        }
        /// \overload
        template(bool True = true)(
            requires True AND (Cardinality < 0) AND (Cardinality != infinite) AND
                (!forward_range<D<True>>) AND sized_range<D<True>>)
        constexpr bool empty() //
            noexcept(noexcept(bool(ranges::size(std::declval<D<True> &>()) == 0)))
        {
            return ranges::size(derived()) == 0;
        }
        /// \overload
        template(bool True = true)(
            requires True AND (Cardinality < 0) AND (Cardinality != infinite) AND
                (!forward_range<D<True> const>) AND sized_range<D<True> const>)
        constexpr bool empty() const //
            noexcept(noexcept(bool(ranges::size(std::declval<D<True> const &>()) == 0)))
        {
            return ranges::size(derived()) == 0;
        }
        /// \overload
        template(bool True = true)(
            requires True AND (!detail::has_fixed_size_<Cardinality>) AND
                forward_range<D<True>>)
        constexpr bool empty() noexcept(
            noexcept(bool(ranges::begin(std::declval<D<True> &>()) ==
                          ranges::end(std::declval<D<True> &>()))))
        {
            return bool(ranges::begin(derived()) == ranges::end(derived()));
        }
        /// \overload
        template(bool True = true)(
            requires True AND (!detail::has_fixed_size_<Cardinality>) AND
                forward_range<D<True> const>)
        constexpr bool empty() const
            noexcept(noexcept(bool(ranges::begin(std::declval<D<True> const &>()) ==
                                   ranges::end(std::declval<D<True> const &>()))))
        {
            return bool(ranges::begin(derived()) == ranges::end(derived()));
        }
        CPP_template_gcc_workaround(bool True = true)(
            requires True && detail::can_empty_<D<True>>) // clang-format off
        constexpr explicit operator bool()
            noexcept(noexcept(ranges::empty(std::declval<D<True> &>())))
        {
            return !ranges::empty(derived());
        }
        // clang-format on
        /// \overload
        CPP_template_gcc_workaround(bool True = true)(
            requires True && detail::can_empty_<D<True> const>) // clang-format off
        constexpr explicit operator bool() const
            noexcept(noexcept(ranges::empty(std::declval<D<True> const &>())))
        {
            return !ranges::empty(derived());
        }
        // clang-format on
        /// If the size of the range is known at compile-time and finite,
        /// return it.
        template(bool True = true, int = 42)(
            requires True AND (Cardinality >= 0)) //
        static constexpr std::size_t size() noexcept
        {
            return static_cast<std::size_t>(Cardinality);
        }
        /// If `sized_sentinel_for<sentinel_t<Derived>, iterator_t<Derived>>` is
        /// satisfied, and if `Derived` is a `forward_range`, then return
        /// `end - begin` cast to an unsigned integer.
        template(bool True = true)(
            requires True AND (Cardinality < 0) AND
                sized_sentinel_for<sentinel_t<D<True>>, iterator_t<D<True>>> AND
                forward_range<D<True>>)
        constexpr detail::iter_size_t<iterator_t<D<True>>> size()
        {
            using size_type = detail::iter_size_t<iterator_t<D<True>>>;
            return static_cast<size_type>(derived().end() - derived().begin());
        }
        /// \overload
        template(bool True = true)(
            requires True AND (Cardinality < 0) AND
                sized_sentinel_for<sentinel_t<D<True> const>,
                                   iterator_t<D<True> const>> AND
                forward_range<D<True> const>)
        constexpr detail::iter_size_t<iterator_t<D<True>>> size() const //
        {
            using size_type = detail::iter_size_t<iterator_t<D<True>>>;
            return static_cast<size_type>(derived().end() - derived().begin());
        }
        /// Access the first element in a range:
        template(bool True = true)(
            requires True AND forward_range<D<True>>)
        constexpr range_reference_t<D<True>> front()
        {
            return *derived().begin();
        }
        /// \overload
        template(bool True = true)(
            requires True AND forward_range<D<True> const>)
        constexpr range_reference_t<D<True> const> front() const
        {
            return *derived().begin();
        }
        /// Access the last element in a range:
        template(bool True = true)(
            requires True AND common_range<D<True>> AND bidirectional_range<D<True>>)
        constexpr range_reference_t<D<True>> back()
        {
            return *prev(derived().end());
        }
        /// \overload
        template(bool True = true)(
            requires True AND common_range<D<True> const> AND
                bidirectional_range<D<True> const>)
        constexpr range_reference_t<D<True> const> back() const
        {
            return *prev(derived().end());
        }
        /// Simple indexing:
        template(bool True = true)(
            requires True AND random_access_range<D<True>>)
        constexpr range_reference_t<D<True>> operator[](range_difference_t<D<True>> n)
        {
            return derived().begin()[n];
        }
        /// \overload
        template(bool True = true)(
            requires True AND random_access_range<D<True> const>)
        constexpr range_reference_t<D<True> const> //
        operator[](range_difference_t<D<True>> n) const
        {
            return derived().begin()[n];
        }
        /// Returns a pointer to the block of memory
        /// containing the elements of a contiguous range:
        template(bool True = true)(
            requires True AND contiguous_iterator<iterator_t<D<True>>>)
        constexpr std::add_pointer_t<range_reference_t<D<True>>> data() //
        {
            return std::addressof(*ranges::begin(derived()));
        }
        /// \overload
        template(bool True = true)(
            requires True AND contiguous_iterator<iterator_t<D<True> const>>)
        constexpr std::add_pointer_t<range_reference_t<D<True> const>> data() const //
        {
            return std::addressof(*ranges::begin(derived()));
        }
        /// Returns a reference to the element at specified location pos, with bounds
        /// checking.
        template(bool True = true)(
            requires True AND random_access_range<D<True>> AND sized_range<D<True>>)
        constexpr range_reference_t<D<True>> at(range_difference_t<D<True>> n)
        {
            using size_type = range_size_t<Derived>;
            if(n < 0 || size_type(n) >= ranges::size(derived()))
            {
                throw std::out_of_range("view_interface::at");
            }
            return derived().begin()[n];
        }
        /// \overload
        template(bool True = true)(
            requires True AND random_access_range<D<True> const> AND
                sized_range<D<True> const>)
        constexpr range_reference_t<D<True> const> at(range_difference_t<D<True>> n) const
        {
            using size_type = range_size_t<Derived const>;
            if(n < 0 || size_type(n) >= ranges::size(derived()))
            {
                throw std::out_of_range("view_interface::at");
            }
            return derived().begin()[n];
        }
        /// Python-ic slicing:
        //      rng[{4,6}]
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND input_range<D<True> &>)
        constexpr auto
            operator[](detail::slice_bounds<range_difference_t<D<True>>> offs) &
        {
            return Slice{}(derived(), offs.from, offs.to);
        }
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND input_range<D<True> const &>)
        constexpr auto
            operator[](detail::slice_bounds<range_difference_t<D<True>>> offs) const &
        {
            return Slice{}(derived(), offs.from, offs.to);
        }
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND input_range<D<True>>)
        constexpr auto
            operator[](detail::slice_bounds<range_difference_t<D<True>>> offs) &&
        {
            return Slice{}(detail::move(derived()), offs.from, offs.to);
        }
        //      rng[{4,end-2}]
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND input_range<D<True> &> AND sized_range<D<True> &>)
        constexpr auto //
        operator[](detail::slice_bounds<range_difference_t<D<True>>,
                                        detail::from_end_of_t<D<True>>> offs) &
        {
            return Slice{}(derived(), offs.from, offs.to);
        }
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND input_range<D<True> const &> AND
                sized_range<D<True> const &>)
        constexpr auto //
        operator[](detail::slice_bounds<range_difference_t<D<True>>,
                                        detail::from_end_of_t<D<True>>> offs) const &
        {
            return Slice{}(derived(), offs.from, offs.to);
        }
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND input_range<D<True>> AND sized_range<D<True>>)
        constexpr auto //
        operator[](detail::slice_bounds<range_difference_t<D<True>>,
                                        detail::from_end_of_t<D<True>>> offs) &&
        {
            return Slice{}(detail::move(derived()), offs.from, offs.to);
        }
        //      rng[{end-4,end-2}]
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND (forward_range<D<True> &> ||
                              (input_range<D<True> &> && sized_range<D<True> &>))) //
        constexpr auto //
        operator[](detail::slice_bounds<detail::from_end_of_t<D<True>>,
                                        detail::from_end_of_t<D<True>>> offs) &
        {
            return Slice{}(derived(), offs.from, offs.to);
        }
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND
            (forward_range<D<True> const &> ||
             (input_range<D<True> const &> && sized_range<D<True> const &>))) //
        constexpr auto //
        operator[](detail::slice_bounds<detail::from_end_of_t<D<True>>,
                                        detail::from_end_of_t<D<True>>> offs) const &
        {
            return Slice{}(derived(), offs.from, offs.to);
        }
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND
                (forward_range<D<True>> ||
                    (input_range<D<True>> && sized_range<D<True>>))) //
        constexpr auto //
        operator[](detail::slice_bounds<detail::from_end_of_t<D<True>>,
                                        detail::from_end_of_t<D<True>>> offs) &&
        {
            return Slice{}(detail::move(derived()), offs.from, offs.to);
        }
        //      rng[{4,end}]
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND input_range<D<True> &>)
        constexpr auto //
        operator[](detail::slice_bounds<range_difference_t<D<True>>, end_fn> offs) &
        {
            return Slice{}(derived(), offs.from, offs.to);
        }
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND input_range<D<True> const &>)
        constexpr auto //
        operator[](detail::slice_bounds<range_difference_t<D<True>>, end_fn> offs) const &
        {
            return Slice{}(derived(), offs.from, offs.to);
        }
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND input_range<D<True>>)
        constexpr auto //
        operator[](detail::slice_bounds<range_difference_t<D<True>>, end_fn> offs) &&
        {
            return Slice{}(detail::move(derived()), offs.from, offs.to);
        }
        //      rng[{end-4,end}]
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND
                (forward_range<D<True> &> ||
                    (input_range<D<True> &> && sized_range<D<True> &>))) //
        constexpr auto //
        operator[](detail::slice_bounds<detail::from_end_of_t<D<True>>, end_fn> offs) &
        {
            return Slice{}(derived(), offs.from, offs.to);
        }
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND
                (forward_range<D<True> const &> ||
                    (input_range<D<True> const &> && sized_range<D<True> const &>))) //
        constexpr auto //
        operator[](
            detail::slice_bounds<detail::from_end_of_t<D<True>>, end_fn> offs) const &
        {
            return Slice{}(derived(), offs.from, offs.to);
        }
        /// \overload
        template(bool True = true, typename Slice = views::slice_fn)(
            requires True AND
                (forward_range<D<True>> ||
                    (input_range<D<True>> && sized_range<D<True>>))) //
        constexpr auto //
        operator[](detail::slice_bounds<detail::from_end_of_t<D<True>>, end_fn> offs) &&
        {
            return Slice{}(detail::move(derived()), offs.from, offs.to);
        }
    private:
#ifndef RANGES_V3_DISABLE_IO
        /// \brief Print a range to an ostream
        template<bool True = true>
        friend auto operator<<(std::ostream & sout, Derived const & rng)
            -> CPP_broken_friend_ret(std::ostream &)(
                requires True && input_range<D<True> const>)
        {
            return detail::print_rng_(sout, rng);
        }
        /// \overload
        template<bool True = true>
        friend auto operator<<(std::ostream & sout, Derived & rng)
            -> CPP_broken_friend_ret(std::ostream &)(
                requires True && (!range<D<True> const>) && input_range<D<True>>)
        {
            return detail::print_rng_(sout, rng);
        }
        /// \overload
        template<bool True = true>
        friend auto operator<<(std::ostream & sout, Derived && rng)
            -> CPP_broken_friend_ret(std::ostream &)(
                requires True && (!range<D<True> const>) && input_range<D<True>>)
        {
            return detail::print_rng_(sout, rng);
        }
#endif
    };
    namespace cpp20
    {
        template(typename Derived)(
            requires std::is_class<Derived>::value AND
                same_as<Derived, meta::_t<std::remove_cv<Derived>>>)
        using view_interface = ranges::view_interface<Derived, ranges::unknown>;
    }
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
