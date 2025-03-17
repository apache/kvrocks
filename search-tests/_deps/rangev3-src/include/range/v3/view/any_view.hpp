/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Casey Carter 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

#ifndef RANGES_V3_VIEW_ANY_VIEW_HPP
#define RANGES_V3_VIEW_ANY_VIEW_HPP

#include <type_traits>
#include <typeinfo>
#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/addressof.hpp>
#include <range/v3/utility/memory.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/facade.hpp>

#include <range/v3/detail/prologue.hpp>

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_INCONSISTENT_OVERRIDE
RANGES_DIAGNOSTIC_SUGGEST_OVERRIDE

namespace ranges
{
    /// \brief An enum that denotes the supported subset of range concepts supported by a
    /// range.
    enum class category
    {
        none = 0,             ///<\brief No concepts met.
        input = 1,            ///<\brief satisfies ranges::concepts::input_range
        forward = 3,          ///<\brief satisfies ranges::concepts::forward_range
        bidirectional = 7,    ///<\brief satisfies ranges::concepts::bidirectional_range
        random_access = 15,   ///<\brief satisfies ranges::concepts::random_access_range
        mask = random_access, ///<\brief Mask away any properties other than iterator
                              ///< category
        sized = 16,           ///<\brief satisfies ranges::concepts::sized_range
    };

    /** \name Binary operators for ranges::category
     *  \relates ranges::category
     *  \{
     */
    constexpr category operator&(category lhs, category rhs) noexcept
    {
        return static_cast<category>(
            static_cast<meta::_t<std::underlying_type<category>>>(lhs) &
            static_cast<meta::_t<std::underlying_type<category>>>(rhs));
    }

    constexpr category operator|(category lhs, category rhs) noexcept
    {
        return static_cast<category>(
            static_cast<meta::_t<std::underlying_type<category>>>(lhs) |
            static_cast<meta::_t<std::underlying_type<category>>>(rhs));
    }

    constexpr category operator^(category lhs, category rhs) noexcept
    {
        return static_cast<category>(
            static_cast<meta::_t<std::underlying_type<category>>>(lhs) ^
            static_cast<meta::_t<std::underlying_type<category>>>(rhs));
    }

    constexpr category operator~(category lhs) noexcept
    {
        return static_cast<category>(
            ~static_cast<meta::_t<std::underlying_type<category>>>(lhs));
    }

    constexpr category & operator&=(category & lhs, category rhs) noexcept
    {
        return (lhs = lhs & rhs);
    }

    constexpr category & operator|=(category & lhs, category rhs) noexcept
    {
        return (lhs = lhs | rhs);
    }

    constexpr category & operator^=(category & lhs, category rhs) noexcept
    {
        return (lhs = lhs ^ rhs);
    }
    //!\}

    /// \brief For a given range, return a ranges::category enum with the satisfied
    /// concepts.
    template<typename Rng>
    constexpr category get_categories() noexcept
    {
        return (input_range<Rng> ? category::input : category::none) |
               (forward_range<Rng> ? category::forward : category::none) |
               (bidirectional_range<Rng> ? category::bidirectional : category::none) |
               (random_access_range<Rng> ? category::random_access : category::none) |
               (sized_range<Rng> ? category::sized : category::none);
    }

    /// \cond
    namespace detail
    {
        // workaround the fact that typeid ignores cv-qualifiers
        template<typename>
        struct rtti_tag
        {};

        struct any_ref
        {
            any_ref() = default;
            template<typename T>
            constexpr any_ref(T & obj) noexcept
              : obj_(detail::addressof(obj))
#ifndef NDEBUG
              , info_(&typeid(rtti_tag<T>))
#endif
            {}
            template<typename T>
            T & get() const noexcept
            {
                RANGES_ASSERT(obj_ && info_ && *info_ == typeid(rtti_tag<T>));
                return *const_cast<T *>(static_cast<T const volatile *>(obj_));
            }

        private:
            void const volatile * obj_ = nullptr;
#ifndef NDEBUG
            std::type_info const * info_ = nullptr;
#endif
        };

        template<typename Base>
        struct cloneable : Base
        {
            using Base::Base;
            virtual ~cloneable() override = default;
            cloneable() = default;
            cloneable(cloneable const &) = delete;
            cloneable & operator=(cloneable const &) = delete;
            virtual std::unique_ptr<cloneable> clone() const = 0;
        };

        // clang-format off
        /// \concept any_compatible_range_
        /// \brief The \c any_compatible_range_ concept
        template(typename Rng, typename Ref)(
        concept (any_compatible_range_)(Rng, Ref),
            convertible_to<range_reference_t<Rng>, Ref>
        );
        /// \concept any_compatible_range
        /// \brief The \c any_compatible_range concept
        template<typename Rng, typename Ref>
        CPP_concept any_compatible_range =
            CPP_concept_ref(detail::any_compatible_range_, Rng, Ref);
        // clang-format on

        template<typename Rng, typename = void>
        struct any_view_sentinel_impl
          : private box<sentinel_t<Rng>, any_view_sentinel_impl<Rng>>
        {
        private:
            using box_t = typename any_view_sentinel_impl::box;

        public:
            any_view_sentinel_impl() = default;
            any_view_sentinel_impl(Rng & rng)
              : box_t(ranges::end(rng))
            {}
            void init(Rng & rng) noexcept
            {
                box_t::get() = ranges::end(rng);
            }
            sentinel_t<Rng> const & get(Rng &) const noexcept
            {
                return box_t::get();
            }
        };

        template<typename Rng>
        struct any_view_sentinel_impl<
            Rng, meta::void_<decltype(ranges::end(std::declval<Rng const &>()))>>
        {
            any_view_sentinel_impl() = default;
            any_view_sentinel_impl(Rng &) noexcept
            {}
            void init(Rng &) noexcept
            {}
            sentinel_t<Rng> get(Rng & rng) const noexcept
            {
                return ranges::end(rng);
            }
        };

        template<typename Ref, bool Sized = false>
        struct any_input_view_interface
        {
            virtual ~any_input_view_interface() = default;
            virtual void init() = 0;
            virtual bool done() = 0;
            virtual Ref read() const = 0;
            virtual void next() = 0;
        };
        template<typename Ref>
        struct any_input_view_interface<Ref, true> : any_input_view_interface<Ref, false>
        {
            virtual std::size_t size() = 0;
        };

        template<typename Ref>
        struct any_input_cursor
        {
            using single_pass = std::true_type;

            any_input_cursor() = default;
            constexpr any_input_cursor(any_input_view_interface<Ref> & view) noexcept
              : view_{detail::addressof(view)}
            {}
            Ref read() const
            {
                return view_->read();
            }
            void next()
            {
                view_->next();
            }
            bool equal(any_input_cursor const &) const noexcept
            {
                return true;
            }
            bool equal(default_sentinel_t) const
            {
                return !view_ || view_->done();
            }

        private:
            any_input_view_interface<Ref> * view_ = nullptr;
        };

        template<typename Rng, typename Ref, bool Sized = false>
        struct RANGES_EMPTY_BASES any_input_view_impl
          : any_input_view_interface<Ref, Sized>
          , private any_view_sentinel_impl<Rng>
        {
            CPP_assert(any_compatible_range<Rng, Ref>);
            CPP_assert(!Sized || (bool)sized_range<Rng>);

            explicit any_input_view_impl(Rng rng)
              : rng_{std::move(rng)}
            {}
            any_input_view_impl(any_input_view_impl const &) = delete;
            any_input_view_impl & operator=(any_input_view_impl const &) = delete;

        private:
            using sentinel_box_t = any_view_sentinel_impl<Rng>;

            virtual void init() override
            {
                sentinel_box_t::init(rng_);
                current_ = ranges::begin(rng_);
            }
            virtual bool done() override
            {
                return current_ == sentinel_box_t::get(rng_);
            }
            virtual Ref read() const override
            {
                return *current_;
            }
            virtual void next() override
            {
                ++current_;
            }
            std::size_t size() // override-ish
            {
                return static_cast<std::size_t>(ranges::size(rng_));
            }

            RANGES_NO_UNIQUE_ADDRESS Rng rng_;
            RANGES_NO_UNIQUE_ADDRESS iterator_t<Rng> current_{};
        };

        template<typename Ref, category Cat = category::forward, typename enable = void>
        struct any_cursor_interface;

        template<typename Ref, category Cat>
        struct any_cursor_interface<
            Ref, Cat, meta::if_c<(Cat & category::mask) == category::forward>>
        {
            virtual ~any_cursor_interface() = default;
            virtual any_ref iter()
                const = 0; // returns a const ref to the cursor's wrapped iterator
            virtual Ref read() const = 0;
            virtual bool equal(any_cursor_interface const &) const = 0;
            virtual void next() = 0;
        };

        template<typename Ref, category Cat>
        struct any_cursor_interface<
            Ref, Cat, meta::if_c<(Cat & category::mask) == category::bidirectional>>
          : any_cursor_interface<Ref, (Cat & ~category::mask) | category::forward>
        {
            virtual void prev() = 0;
        };

        template<typename Ref, category Cat>
        struct any_cursor_interface<
            Ref, Cat, meta::if_c<(Cat & category::mask) == category::random_access>>
          : any_cursor_interface<Ref, (Cat & ~category::mask) | category::bidirectional>
        {
            virtual void advance(std::ptrdiff_t) = 0;
            virtual std::ptrdiff_t distance_to(any_cursor_interface const &) const = 0;
        };

        template<typename Ref, category Cat>
        using any_cloneable_cursor_interface = cloneable<any_cursor_interface<Ref, Cat>>;

        template<typename I, typename Ref, category Cat>
        struct any_cursor_impl : any_cloneable_cursor_interface<Ref, Cat>
        {
            CPP_assert(convertible_to<iter_reference_t<I>, Ref>);
            CPP_assert((Cat & category::forward) == category::forward);

            any_cursor_impl() = default;
            any_cursor_impl(I it)
              : it_{std::move(it)}
            {}

        private:
            using Forward =
                any_cursor_interface<Ref, (Cat & ~category::mask) | category::forward>;

            I it_;

            any_ref iter() const override
            {
                return it_;
            }
            Ref read() const override
            {
                return *it_;
            }
            bool equal(Forward const & that_) const override
            {
                auto & that = polymorphic_downcast<any_cursor_impl const &>(that_);
                return that.it_ == it_;
            }
            void next() override
            {
                ++it_;
            }
            std::unique_ptr<any_cloneable_cursor_interface<Ref, Cat>> clone()
                const override
            {
                return detail::make_unique<any_cursor_impl>(it_);
            }
            void prev() // override (sometimes; it's complicated)
            {
                --it_;
            }
            void advance(std::ptrdiff_t n) // override-ish
            {
                it_ += n;
            }
            std::ptrdiff_t distance_to(
                any_cursor_interface<Ref, Cat> const & that_) const // override-ish
            {
                auto & that = polymorphic_downcast<any_cursor_impl const &>(that_);
                return static_cast<std::ptrdiff_t>(that.it_ - it_);
            }
        };

        struct fully_erased_view
        {
            virtual bool at_end(
                any_ref) = 0; // any_ref is a const ref to a wrapped iterator
                              // to be compared to the erased view's last sentinel
        protected:
            ~fully_erased_view() = default;
        };

        struct any_sentinel
        {
            any_sentinel() = default;
            constexpr explicit any_sentinel(fully_erased_view & view) noexcept
              : view_{&view}
            {}

        private:
            template<typename, category>
            friend struct any_cursor;

            fully_erased_view * view_ = nullptr;
        };

        template<typename Ref, category Cat>
        struct any_cursor
        {
        private:
            CPP_assert((Cat & category::forward) == category::forward);

            std::unique_ptr<any_cloneable_cursor_interface<Ref, Cat>> ptr_;

            template<typename Rng>
            using impl_t = any_cursor_impl<iterator_t<Rng>, Ref, Cat>;

        public:
            any_cursor() = default;
            template(typename Rng)(
                requires (!same_as<detail::decay_t<Rng>, any_cursor>) AND
                    forward_range<Rng> AND
                    any_compatible_range<Rng, Ref>)
            explicit any_cursor(Rng && rng)
              : ptr_{detail::make_unique<impl_t<Rng>>(begin(rng))}
            {}
            any_cursor(any_cursor &&) = default;
            any_cursor(any_cursor const & that)
              : ptr_{that.ptr_ ? that.ptr_->clone() : nullptr}
            {}
            any_cursor & operator=(any_cursor &&) = default;
            any_cursor & operator=(any_cursor const & that)
            {
                ptr_ = (that.ptr_ ? that.ptr_->clone() : nullptr);
                return *this;
            }
            Ref read() const
            {
                RANGES_EXPECT(ptr_);
                return ptr_->read();
            }
            bool equal(any_cursor const & that) const
            {
                RANGES_EXPECT(!ptr_ == !that.ptr_);
                return !ptr_ || ptr_->equal(*that.ptr_);
            }
            bool equal(any_sentinel const & that) const
            {
                RANGES_EXPECT(!ptr_ == !that.view_);
                return !ptr_ || that.view_->at_end(ptr_->iter());
            }
            void next()
            {
                RANGES_EXPECT(ptr_);
                ptr_->next();
            }
            CPP_member
            auto prev() //
                -> CPP_ret(void)(
                    requires (category::bidirectional == (Cat & category::bidirectional)))
            {
                RANGES_EXPECT(ptr_);
                ptr_->prev();
            }
            CPP_member
            auto advance(std::ptrdiff_t n) //
                -> CPP_ret(void)(
                    requires (category::random_access == (Cat & category::random_access)))
            {
                RANGES_EXPECT(ptr_);
                ptr_->advance(n);
            }
            CPP_member
            auto distance_to(any_cursor const & that) const //
                -> CPP_ret(std::ptrdiff_t)(
                    requires (category::random_access == (Cat & category::random_access)))
            {
                RANGES_EXPECT(!ptr_ == !that.ptr_);
                return !ptr_ ? 0 : ptr_->distance_to(*that.ptr_);
            }
        };

        template<typename Ref, category Cat,
                 bool = (Cat & category::sized) == category::sized>
        struct any_view_interface : fully_erased_view
        {
            CPP_assert((Cat & category::forward) == category::forward);

            virtual ~any_view_interface() = default;
            virtual any_cursor<Ref, Cat> begin_cursor() = 0;
        };
        template<typename Ref, category Cat>
        struct any_view_interface<Ref, Cat, true> : any_view_interface<Ref, Cat, false>
        {
            virtual std::size_t size() = 0;
        };

        template<typename Ref, category Cat>
        using any_cloneable_view_interface = cloneable<any_view_interface<Ref, Cat>>;

        template<typename Rng, typename Ref, category Cat>
        struct RANGES_EMPTY_BASES any_view_impl
          : any_cloneable_view_interface<Ref, Cat>
          , private box<Rng, any_view_impl<Rng, Ref, Cat>>
          , private any_view_sentinel_impl<Rng>
        {
            CPP_assert((Cat & category::forward) == category::forward);
            CPP_assert(any_compatible_range<Rng, Ref>);
            CPP_assert((Cat & category::sized) == category::none ||
                       (bool)sized_range<Rng>);

            any_view_impl() = default;
            any_view_impl(Rng rng)
              : range_box_t{std::move(rng)}
              , sentinel_box_t{range_box_t::get()}
            // NB: initialization order dependence
            {}

        private:
            using range_box_t = box<Rng, any_view_impl>;
            using sentinel_box_t = any_view_sentinel_impl<Rng>;

            any_cursor<Ref, Cat> begin_cursor() override
            {
                return any_cursor<Ref, Cat>{range_box_t::get()};
            }
            bool at_end(any_ref it_) override
            {
                auto & it = it_.get<iterator_t<Rng> const>();
                return it == sentinel_box_t::get(range_box_t::get());
            }
            std::unique_ptr<any_cloneable_view_interface<Ref, Cat>> clone() const override
            {
                return detail::make_unique<any_view_impl>(range_box_t::get());
            }
            std::size_t size() // override-ish
            {
                return static_cast<std::size_t>(ranges::size(range_box_t::get()));
            }
        };
    } // namespace detail
    /// \endcond

    /// \brief A type-erased view
    /// \ingroup group-views
    template<typename Ref, category Cat = category::input, typename enable = void>
    struct any_view
      : view_facade<any_view<Ref, Cat>,
                    (Cat & category::sized) == category::sized ? finite : unknown>
    {
        friend range_access;
        CPP_assert((Cat & category::forward) == category::forward);

        any_view() = default;
        template(typename Rng)(
            requires //
                (!same_as<detail::decay_t<Rng>, any_view>) AND
                input_range<Rng> AND
                detail::any_compatible_range<Rng, Ref>)
        any_view(Rng && rng)
          : any_view(static_cast<Rng &&>(rng),
                     meta::bool_<(get_categories<Rng>() & Cat) == Cat>{})
        {}
        any_view(any_view &&) = default;
        any_view(any_view const & that)
          : ptr_{that.ptr_ ? that.ptr_->clone() : nullptr}
        {}
        any_view & operator=(any_view &&) = default;
        any_view & operator=(any_view const & that)
        {
            ptr_ = (that.ptr_ ? that.ptr_->clone() : nullptr);
            return *this;
        }

        CPP_member
        auto size() //
            -> CPP_ret(std::size_t)(
                requires (category::sized == (Cat & category::sized)))
        {
            return ptr_ ? ptr_->size() : 0;
        }

    private:
        template<typename Rng>
        using impl_t = detail::any_view_impl<views::all_t<Rng>, Ref, Cat>;
        template<typename Rng>
        any_view(Rng && rng, std::true_type)
          : ptr_{detail::make_unique<impl_t<Rng>>(views::all(static_cast<Rng &&>(rng)))}
        {}
        template<typename Rng>
        any_view(Rng &&, std::false_type)
        {
            static_assert(
                (get_categories<Rng>() & Cat) == Cat,
                "The range passed to any_view() does not model the requested category");
        }

        detail::any_cursor<Ref, Cat> begin_cursor()
        {
            return ptr_ ? ptr_->begin_cursor() : detail::value_init{};
        }
        detail::any_sentinel end_cursor() noexcept
        {
            return detail::any_sentinel{*ptr_};
        }

        std::unique_ptr<detail::any_cloneable_view_interface<Ref, Cat>> ptr_;
    };

    // input and not forward
    template<typename Ref, category Cat>
    struct any_view<Ref, Cat, meta::if_c<(Cat & category::forward) == category::input>>
      : view_facade<any_view<Ref, Cat, void>,
                    (Cat & category::sized) == category::sized ? finite : unknown>
    {
        friend range_access;

        any_view() = default;
        template(typename Rng)(
            requires //
                (!same_as<detail::decay_t<Rng>, any_view>) AND
                input_range<Rng> AND
                detail::any_compatible_range<Rng, Ref>)
        any_view(Rng && rng)
          : ptr_{std::make_shared<impl_t<Rng>>(views::all(static_cast<Rng &&>(rng)))}
        {}

        CPP_member
        auto size() //
            -> CPP_ret(std::size_t)(
                requires (category::sized == (Cat & category::sized)))
        {
            return ptr_ ? ptr_->size() : 0;
        }

    private:
        template<typename Rng>
        using impl_t =
            detail::any_input_view_impl<views::all_t<Rng>, Ref,
                                        (Cat & category::sized) == category::sized>;

        detail::any_input_cursor<Ref> begin_cursor()
        {
            if(!ptr_)
                return {};

            ptr_->init();
            return detail::any_input_cursor<Ref>{*ptr_};
        }

        std::shared_ptr<detail::any_input_view_interface<Ref, (Cat & category::sized) ==
                                                                  category::sized>>
            ptr_;
    };

#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
    template(typename Rng)(
        requires view_<Rng>)
        any_view(Rng &&)
            ->any_view<range_reference_t<Rng>, get_categories<Rng>()>;
#endif

    template<typename Ref>
    using any_input_view RANGES_DEPRECATED(
        "Use any_view<Ref, category::input> instead.") = any_view<Ref, category::input>;

    template<typename Ref>
    using any_forward_view RANGES_DEPRECATED(
        "Use any_view<Ref, category::forward> instead.") =
        any_view<Ref, category::forward>;

    template<typename Ref>
    using any_bidirectional_view RANGES_DEPRECATED(
        "Use any_view<Ref, category::bidirectional> instead.") =
        any_view<Ref, category::bidirectional>;

    template<typename Ref>
    using any_random_access_view RANGES_DEPRECATED(
        "Use any_view<Ref, category::random_access> instead.") =
        any_view<Ref, category::random_access>;
} // namespace ranges

#include <range/v3/detail/satisfy_boost_range.hpp>
RANGES_SATISFY_BOOST_RANGE(::ranges::any_view)

RANGES_DIAGNOSTIC_POP

#include <range/v3/detail/epilogue.hpp>

#endif
