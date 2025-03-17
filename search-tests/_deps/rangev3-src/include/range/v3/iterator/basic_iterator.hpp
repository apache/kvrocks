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
#ifndef RANGES_V3_ITERATOR_BASIC_ITERATOR_HPP
#define RANGES_V3_ITERATOR_BASIC_ITERATOR_HPP

#include <type_traits>
#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/detail/range_access.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/utility/addressof.hpp>
#include <range/v3/utility/box.hpp>
#include <range/v3/utility/move.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>

#include <range/v3/detail/prologue.hpp>

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_MULTIPLE_ASSIGNMENT_OPERATORS

namespace ranges
{
    /// \addtogroup group-iterator Iterator
    /// @{
    ///
    template<typename T>
    struct basic_mixin : private box<T>
    {
        CPP_member
        constexpr CPP_ctor(basic_mixin)()(                            //
            noexcept(std::is_nothrow_default_constructible<T>::value) //
                requires default_constructible<T>)
          : box<T>{}
        {}
        CPP_member
        constexpr explicit CPP_ctor(basic_mixin)(T && t)(          //
            noexcept(std::is_nothrow_move_constructible<T>::value) //
                requires move_constructible<T>)
          : box<T>(detail::move(t))
        {}
        CPP_member
        constexpr explicit CPP_ctor(basic_mixin)(T const & t)(     //
            noexcept(std::is_nothrow_copy_constructible<T>::value) //
                requires copy_constructible<T>)
          : box<T>(t)
        {}

    protected:
        using box<T>::get;
    };

    /// \cond
    namespace detail
    {
        template<typename Cur>
        using cursor_reference_t =
            decltype(range_access::read(std::declval<Cur const &>()));

        // Compute the rvalue reference type of a cursor
        template<typename Cur>
        auto cursor_move(Cur const & cur, int) -> decltype(range_access::move(cur));
        template<typename Cur>
        auto cursor_move(Cur const & cur, long) -> aux::move_t<cursor_reference_t<Cur>>;

        template<typename Cur>
        using cursor_rvalue_reference_t =
            decltype(detail::cursor_move(std::declval<Cur const &>(), 42));

        // Define conversion operators from the proxy reference type
        // to the common reference types, so that basic_iterator can model readable
        // even with getters/setters.
        template<typename Derived, typename Head>
        struct proxy_reference_conversion
        {
            operator Head() const
                noexcept(noexcept(Head(Head(std::declval<Derived const &>().read_()))))
            {
                return Head(static_cast<Derived const *>(this)->read_());
            }
        };

        // Collect the reference types associated with cursors
        template<typename Cur, bool IsReadable>
        struct cursor_traits_
        {
        private:
            struct private_
            {};

        public:
            using value_t_ = private_;
            using reference_t_ = private_;
            using rvalue_reference_t_ = private_;
            using common_refs = meta::list<>;
        };

        template<typename Cur>
        struct cursor_traits_<Cur, true>
        {
            using value_t_ = range_access::cursor_value_t<Cur>;
            using reference_t_ = cursor_reference_t<Cur>;
            using rvalue_reference_t_ = cursor_rvalue_reference_t<Cur>;

        private:
            using R1 = reference_t_;
            using R2 = common_reference_t<reference_t_, value_t_ &>;
            using R3 = common_reference_t<reference_t_, rvalue_reference_t_>;
            using tmp1 = meta::list<value_t_, R1>;
            using tmp2 =
                meta::if_<meta::in<tmp1, uncvref_t<R2>>, tmp1, meta::push_back<tmp1, R2>>;
            using tmp3 =
                meta::if_<meta::in<tmp2, uncvref_t<R3>>, tmp2, meta::push_back<tmp2, R3>>;

        public:
            using common_refs = meta::unique<meta::pop_front<tmp3>>;
        };

        template<typename Cur>
        using cursor_traits = cursor_traits_<Cur, (bool)readable_cursor<Cur>>;

        template<typename Cur>
        using cursor_value_t = typename cursor_traits<Cur>::value_t_;

        template<typename Cur, bool IsReadable>
        struct basic_proxy_reference_;
        template<typename Cur>
        using basic_proxy_reference =
            basic_proxy_reference_<Cur, (bool)readable_cursor<Cur>>;

        // The One Proxy Reference type to rule them all. basic_iterator uses this
        // as the return type of operator* when the cursor type has a set() member
        // function of the correct signature (i.e., if it can accept a value_type &&).
        template<typename Cur, bool IsReadable /*= (bool) readable_cursor<Cur>*/>
        struct RANGES_EMPTY_BASES basic_proxy_reference_
          : cursor_traits<Cur>
          // The following adds conversion operators to the common reference
          // types, so that basic_proxy_reference can model readable
          , meta::inherit<meta::transform<
                typename cursor_traits<Cur>::common_refs,
                meta::bind_front<meta::quote<proxy_reference_conversion>,
                                 basic_proxy_reference_<Cur, IsReadable>>>>
        {
        private:
            Cur * cur_;
            template<typename, bool>
            friend struct basic_proxy_reference_;
            template<typename, typename>
            friend struct proxy_reference_conversion;
            using typename cursor_traits<Cur>::value_t_;
            using typename cursor_traits<Cur>::reference_t_;
            using typename cursor_traits<Cur>::rvalue_reference_t_;
            static_assert((bool)common_reference_with<value_t_ &, reference_t_>,
                          "Your readable and writable cursor must have a value type and "
                          "a reference type that share a common reference type. See the "
                          "ranges::common_reference type trait.");
            // BUGBUG make these private:
        public:
            constexpr reference_t_ read_() const noexcept(
                noexcept(reference_t_(range_access::read(std::declval<Cur const &>()))))
            {
                return range_access::read(*cur_);
            }
            template<typename T>
            constexpr void write_(T && t) const
            {
                range_access::write(*cur_, (T &&) t);
            }
            // public:
            basic_proxy_reference_() = default;
            basic_proxy_reference_(basic_proxy_reference_ const &) = default;
            template(typename OtherCur)(
                requires convertible_to<OtherCur *, Cur *>)
            constexpr basic_proxy_reference_(
                basic_proxy_reference<OtherCur> const & that) noexcept
              : cur_(that.cur_)
            {}
            constexpr explicit basic_proxy_reference_(Cur & cur) noexcept
              : cur_(&cur)
            {}
            CPP_member
            constexpr auto operator=(basic_proxy_reference_ && that)
                -> CPP_ret(basic_proxy_reference_ &)(
                    requires readable_cursor<Cur>)
            {
                return *this = that;
            }
            CPP_member
            constexpr auto operator=(basic_proxy_reference_ const & that)
                -> CPP_ret(basic_proxy_reference_ &)(
                    requires readable_cursor<Cur>)
            {
                this->write_(that.read_());
                return *this;
            }
            CPP_member
            constexpr auto operator=(basic_proxy_reference_ && that) const
                -> CPP_ret(basic_proxy_reference_ const &)(
                    requires readable_cursor<Cur>)
            {
                return *this = that;
            }
            CPP_member
            constexpr auto operator=(basic_proxy_reference_ const & that) const
                -> CPP_ret(basic_proxy_reference_ const &)(
                    requires readable_cursor<Cur>)
            {
                this->write_(that.read_());
                return *this;
            }
            template(typename OtherCur)(
                requires readable_cursor<OtherCur> AND
                    writable_cursor<Cur, cursor_reference_t<OtherCur>>)
            constexpr basic_proxy_reference_ & //
            operator=(basic_proxy_reference<OtherCur> && that)
            {
                return *this = that;
            }
            template(typename OtherCur)(
                requires readable_cursor<OtherCur> AND
                    writable_cursor<Cur, cursor_reference_t<OtherCur>>)
            constexpr basic_proxy_reference_ & //
            operator=(basic_proxy_reference<OtherCur> const & that)
            {
                this->write_(that.read_());
                return *this;
            }
            template(typename OtherCur)(
                requires readable_cursor<OtherCur> AND
                    writable_cursor<Cur, cursor_reference_t<OtherCur>>)
            constexpr basic_proxy_reference_ const & //
            operator=(basic_proxy_reference<OtherCur> && that) const
            {
                return *this = that;
            }
            template(typename OtherCur)(
                requires readable_cursor<OtherCur> AND
                    writable_cursor<Cur, cursor_reference_t<OtherCur>>)
            constexpr basic_proxy_reference_ const & //
            operator=(basic_proxy_reference<OtherCur> const & that) const
            {
                this->write_(that.read_());
                return *this;
            }
            template(typename T)(
                requires writable_cursor<Cur, T>)
            constexpr basic_proxy_reference_ & operator=(T && t) //
            {
                this->write_((T &&) t);
                return *this;
            }
            template(typename T)(
                requires writable_cursor<Cur, T>)
            constexpr basic_proxy_reference_ const & operator=(T && t) const
            {
                this->write_((T &&) t);
                return *this;
            }
        };

        template(typename Cur, bool IsReadable)(
            requires readable_cursor<Cur> AND equality_comparable<cursor_value_t<Cur>>)
        constexpr bool operator==(basic_proxy_reference_<Cur, IsReadable> const & x,
                                  cursor_value_t<Cur> const & y)
        {
            return x.read_() == y;
        }
        template(typename Cur, bool IsReadable)(
            requires readable_cursor<Cur> AND equality_comparable<cursor_value_t<Cur>>)
        constexpr bool operator!=(basic_proxy_reference_<Cur, IsReadable> const & x,
                                  cursor_value_t<Cur> const & y)
        {
            return !(x == y);
        }
        template(typename Cur, bool IsReadable)(
            requires readable_cursor<Cur> AND equality_comparable<cursor_value_t<Cur>>)
        constexpr bool operator==(cursor_value_t<Cur> const & x,
                                  basic_proxy_reference_<Cur, IsReadable> const & y)
        {
            return x == y.read_();
        }
        template(typename Cur, bool IsReadable)(
            requires readable_cursor<Cur> AND equality_comparable<cursor_value_t<Cur>>)
        constexpr bool operator!=(cursor_value_t<Cur> const & x,
                                  basic_proxy_reference_<Cur, IsReadable> const & y)
        {
            return !(x == y);
        }
        template(typename Cur, bool IsReadable)(
            requires readable_cursor<Cur> AND equality_comparable<cursor_value_t<Cur>>)
        constexpr bool operator==(basic_proxy_reference_<Cur, IsReadable> const & x,
                                  basic_proxy_reference_<Cur, IsReadable> const & y)
        {
            return x.read_() == y.read_();
        }
        template(typename Cur, bool IsReadable)(
            requires readable_cursor<Cur> AND equality_comparable<cursor_value_t<Cur>>)
        constexpr bool operator!=(basic_proxy_reference_<Cur, IsReadable> const & x,
                                  basic_proxy_reference_<Cur, IsReadable> const & y)
        {
            return !(x == y);
        }

        template<typename Cur>
        using cpp20_iter_cat_of_t =                             //
            std::enable_if_t<                                   //
                input_cursor<Cur>,                              //
                meta::conditional_t<                            //
                    contiguous_cursor<Cur>,                     //
                    ranges::contiguous_iterator_tag,            //
                    meta::conditional_t<                        //
                        random_access_cursor<Cur>,              //
                        std::random_access_iterator_tag,        //
                        meta::conditional_t<                    //
                            bidirectional_cursor<Cur>,          //
                            std::bidirectional_iterator_tag,    //
                            meta::conditional_t<                //
                                forward_cursor<Cur>,            //
                                std::forward_iterator_tag,      //
                                std::input_iterator_tag>>>>>;

        // clang-format off
        /// \concept cpp17_input_cursor_
        /// \brief The \c cpp17_input_cursor_ concept
        template(typename C)(
        concept (cpp17_input_cursor_)(C),
            // Either it is not single-pass, or else we can create a
            // proxy for postfix increment.
            !range_access::single_pass_t<uncvref_t<C>>::value ||
            (move_constructible<range_access::cursor_value_t<C>> &&
             constructible_from<range_access::cursor_value_t<C>, cursor_reference_t<C>>)
        );

        /// \concept cpp17_input_cursor
        /// \brief The \c cpp17_input_cursor concept
        template<typename C>
        CPP_concept cpp17_input_cursor =
            input_cursor<C> &&
            sentinel_for_cursor<C, C> &&
            CPP_concept_ref(cpp17_input_cursor_, C);

        /// \concept cpp17_forward_cursor_
        /// \brief The \c cpp17_forward_cursor_ concept
        template(typename C)(
        concept (cpp17_forward_cursor_)(C),
            std::is_reference<cursor_reference_t<C>>::value
        );

        /// \concept cpp17_forward_cursor
        /// \brief The \c cpp17_forward_cursor concept
        template<typename C>
        CPP_concept cpp17_forward_cursor =
            forward_cursor<C> &&
            CPP_concept_ref(cpp17_forward_cursor_, C);
        // clang-format on

        template<typename Category, typename Base = void>
        struct with_iterator_category : Base
        {
            using iterator_category = Category;
        };

        template<typename Category>
        struct with_iterator_category<Category>
        {
            using iterator_category = Category;
        };

        template<typename Cur>
        using cpp17_iter_cat_of_t =                      //
            std::enable_if_t<                            //
                cpp17_input_cursor<Cur>,                 //
                meta::conditional_t<                       //
                    random_access_cursor<Cur>,           //
                    std::random_access_iterator_tag,     //
                    meta::conditional_t<                   //
                        bidirectional_cursor<Cur>,       //
                        std::bidirectional_iterator_tag, //
                        meta::conditional_t<               //
                            cpp17_forward_cursor<Cur>,   //
                            std::forward_iterator_tag,   //
                            std::input_iterator_tag>>>>;

        template<typename Cur, typename = void>
        struct readable_iterator_associated_types_base : range_access::mixin_base_t<Cur>
        {
            readable_iterator_associated_types_base() = default;
            using range_access::mixin_base_t<Cur>::mixin_base_t;
            readable_iterator_associated_types_base(Cur && cur)
              : range_access::mixin_base_t<Cur>(static_cast<Cur &&>(cur))
            {}
            readable_iterator_associated_types_base(Cur const & cur)
              : range_access::mixin_base_t<Cur>(cur)
            {}
        };

        template<typename Cur>
        struct readable_iterator_associated_types_base<
            Cur, always_<void, cpp17_iter_cat_of_t<Cur>>>
          : range_access::mixin_base_t<Cur>
        {
            using iterator_category = cpp17_iter_cat_of_t<Cur>;
            readable_iterator_associated_types_base() = default;
            using range_access::mixin_base_t<Cur>::mixin_base_t;
            readable_iterator_associated_types_base(Cur && cur)
              : range_access::mixin_base_t<Cur>(static_cast<Cur &&>(cur))
            {}
            readable_iterator_associated_types_base(Cur const & cur)
              : range_access::mixin_base_t<Cur>(cur)
            {}
        };

        template<typename Cur, bool IsReadable /*= (bool) readable_cursor<Cur>*/>
        struct iterator_associated_types_base_ : range_access::mixin_base_t<Cur>
        {
            // BUGBUG
            // protected:
            using iter_reference_t = basic_proxy_reference<Cur>;
            using const_reference_t = basic_proxy_reference<Cur const>;

        public:
            using reference = void;
            using difference_type = range_access::cursor_difference_t<Cur>;

            iterator_associated_types_base_() = default;
            using range_access::mixin_base_t<Cur>::mixin_base_t;
            iterator_associated_types_base_(Cur && cur)
              : range_access::mixin_base_t<Cur>(static_cast<Cur &&>(cur))
            {}
            iterator_associated_types_base_(Cur const & cur)
              : range_access::mixin_base_t<Cur>(cur)
            {}
        };

        template<typename Cur>
        using cursor_arrow_t = decltype(range_access::arrow(std::declval<Cur const &>()));

        template<typename Cur>
        struct iterator_associated_types_base_<Cur, true>
          : readable_iterator_associated_types_base<Cur>
        {
            // BUGBUG
            // protected:
            using iter_reference_t =
                meta::conditional_t<is_writable_cursor_v<Cur const>,
                          basic_proxy_reference<Cur const>,
                          meta::conditional_t<is_writable_cursor_v<Cur>,
                                    basic_proxy_reference<Cur>, cursor_reference_t<Cur>>>;
            using const_reference_t =
                meta::conditional_t<is_writable_cursor_v<Cur const>,
                          basic_proxy_reference<Cur const>, cursor_reference_t<Cur>>;

        public:
            using difference_type = range_access::cursor_difference_t<Cur>;
            using value_type = range_access::cursor_value_t<Cur>;
            using reference = iter_reference_t;
            using iterator_concept = cpp20_iter_cat_of_t<Cur>;
            using pointer = meta::_t<
                meta::conditional_t<
                    (bool)has_cursor_arrow<Cur>,
                    meta::defer<cursor_arrow_t, Cur>,
                    std::add_pointer<reference>>>;
            using common_reference = common_reference_t<reference, value_type &>;

            iterator_associated_types_base_() = default;
            using readable_iterator_associated_types_base<
                Cur>::readable_iterator_associated_types_base;
            iterator_associated_types_base_(Cur && cur)
              : readable_iterator_associated_types_base<Cur>(static_cast<Cur &&>(cur))
            {}
            iterator_associated_types_base_(Cur const & cur)
              : readable_iterator_associated_types_base<Cur>(cur)
            {}
        };

        template<typename Cur>
        using iterator_associated_types_base =
            iterator_associated_types_base_<Cur, (bool)readable_cursor<Cur>>;

        template<typename Value>
        struct postfix_increment_proxy
        {
        private:
            Value cache_;

        public:
            template<typename T>
            constexpr postfix_increment_proxy(T && t)
              : cache_(static_cast<T &&>(t))
            {}
            constexpr Value const & operator*() const noexcept
            {
                return cache_;
            }
        };
    } // namespace detail
    /// \endcond

#if RANGES_BROKEN_CPO_LOOKUP
    namespace _basic_iterator_
    {
        template<typename>
        struct adl_hook
        {};
    } // namespace _basic_iterator_
#endif

    template<typename Cur>
    struct RANGES_EMPTY_BASES basic_iterator
      : detail::iterator_associated_types_base<Cur>
#if RANGES_BROKEN_CPO_LOOKUP
      , private _basic_iterator_::adl_hook<basic_iterator<Cur>>
#endif
    {
    private:
        template<typename>
        friend struct basic_iterator;
        friend range_access;
        using base_t = detail::iterator_associated_types_base<Cur>;
        using mixin_t = range_access::mixin_base_t<Cur>;
        static_assert((bool)detail::cursor<Cur>, "");
        using assoc_types_ = detail::iterator_associated_types_base<Cur>;
        using typename assoc_types_::const_reference_t;
        using typename assoc_types_::iter_reference_t;
        constexpr Cur & pos() noexcept
        {
            return this->mixin_t::basic_mixin::get();
        }
        constexpr Cur const & pos() const noexcept
        {
            return this->mixin_t::basic_mixin::get();
        }

    public:
        using typename assoc_types_::difference_type;
        constexpr basic_iterator() = default;
        template(typename OtherCur)(
            requires (!same_as<OtherCur, Cur>) AND convertible_to<OtherCur, Cur> AND
            constructible_from<mixin_t, OtherCur>)
        constexpr basic_iterator(basic_iterator<OtherCur> that)
          : base_t{std::move(that.pos())}
        {}
        // Mix in any additional constructors provided by the mixin
        using base_t::base_t;

        explicit basic_iterator(Cur && cur)
          : base_t(static_cast<Cur &&>(cur))
        {}

        explicit basic_iterator(Cur const & cur)
          : base_t(cur)
        {}

        template(typename OtherCur)(
            requires (!same_as<OtherCur, Cur>) AND convertible_to<OtherCur, Cur>)
        constexpr basic_iterator & operator=(basic_iterator<OtherCur> that)
        {
            pos() = std::move(that.pos());
            return *this;
        }

        CPP_member
        constexpr auto operator*() const
            noexcept(noexcept(range_access::read(std::declval<Cur const &>())))
            -> CPP_ret(const_reference_t)(
                requires detail::readable_cursor<Cur> &&
                    (!detail::is_writable_cursor_v<Cur>))
        {
            return range_access::read(pos());
        }
        CPP_member
        constexpr auto operator*() //
            noexcept(noexcept(iter_reference_t{std::declval<Cur &>()})) //
            -> CPP_ret(iter_reference_t)(
                requires detail::has_cursor_next<Cur> &&
                    detail::is_writable_cursor_v<Cur>)
        {
            return iter_reference_t{pos()};
        }
        CPP_member
        constexpr auto operator*() const
            noexcept(noexcept(const_reference_t{std::declval<Cur const &>()}))
            -> CPP_ret(const_reference_t)(
                requires detail::has_cursor_next<Cur> &&
                    detail::is_writable_cursor_v<Cur const>)
        {
            return const_reference_t{pos()};
        }
        CPP_member
        constexpr auto operator*() noexcept //
            -> CPP_ret(basic_iterator &)(
                requires (!detail::has_cursor_next<Cur>))
        {
            return *this;
        }

        // Use cursor's arrow() member, if any.
        template(typename C = Cur)(
            requires detail::has_cursor_arrow<C>)
        constexpr detail::cursor_arrow_t<C> operator-> () const
            noexcept(noexcept(range_access::arrow(std::declval<C const &>())))
        {
            return range_access::arrow(pos());
        }
        // Otherwise, if iter_reference_t is an lvalue reference to cv-qualified
        // iter_value_t, return the address of **this.
        template(typename C = Cur)(
            requires (!detail::has_cursor_arrow<C>) AND detail::readable_cursor<C> AND
                std::is_lvalue_reference<const_reference_t>::value AND
                same_as<typename detail::iterator_associated_types_base<C>::value_type,
                        uncvref_t<const_reference_t>>)
        constexpr std::add_pointer_t<const_reference_t> operator-> () const
            noexcept(noexcept(*std::declval<basic_iterator const &>()))
        {
            return detail::addressof(**this);
        }

        CPP_member
        constexpr auto operator++() //
            -> CPP_ret(basic_iterator &)(
                requires detail::has_cursor_next<Cur>)
        {
            range_access::next(pos());
            return *this;
        }
        CPP_member
        constexpr auto operator++() noexcept //
            -> CPP_ret(basic_iterator &)(
                requires (!detail::has_cursor_next<Cur>))
        {
            return *this;
        }

    private:
        constexpr basic_iterator post_increment_(std::false_type, int)
        {
            basic_iterator tmp{*this};
            ++*this;
            return tmp;
        }
        // Attempt to satisfy the C++17 iterator requirements by returning a
        // proxy from postfix increment:
        template(typename A = assoc_types_, typename V = typename A::value_type)(
            requires constructible_from<V, typename A::reference> AND
                move_constructible<V>)
        constexpr auto post_increment_(std::true_type, int) //
            -> detail::postfix_increment_proxy<V>
        {
            detail::postfix_increment_proxy<V> p{**this};
            ++*this;
            return p;
        }
        constexpr void post_increment_(std::true_type, long)
        {
            ++*this;
        }

    public:
        CPP_member
        constexpr auto operator++(int)
        {
            return this->post_increment_(meta::bool_ < detail::input_cursor<Cur> &&
                                             !detail::forward_cursor<Cur>> {},
                                         0);
        }

        CPP_member
        constexpr auto operator--()
            -> CPP_ret(basic_iterator &)(
                requires detail::bidirectional_cursor<Cur>)
        {
            range_access::prev(pos());
            return *this;
        }
        CPP_member
        constexpr auto operator--(int) //
            -> CPP_ret(basic_iterator)(
                requires detail::bidirectional_cursor<Cur>)
        {
            basic_iterator tmp(*this);
            --*this;
            return tmp;
        }
        CPP_member
        constexpr auto operator+=(difference_type n) //
            -> CPP_ret(basic_iterator &)(
                requires detail::random_access_cursor<Cur>)
        {
            range_access::advance(pos(), n);
            return *this;
        }
        CPP_member
        constexpr auto operator-=(difference_type n) //
            -> CPP_ret(basic_iterator &)(
                requires detail::random_access_cursor<Cur>)
        {
            range_access::advance(pos(), (difference_type)-n);
            return *this;
        }
        CPP_member
        constexpr auto operator[](difference_type n) const //
            -> CPP_ret(const_reference_t)(
                requires detail::random_access_cursor<Cur>)
        {
            return *(*this + n);
        }

#if !RANGES_BROKEN_CPO_LOOKUP
        // Optionally support hooking iter_move when the cursor sports a
        // move() member function.
        template<typename C = Cur>
        friend constexpr auto iter_move(basic_iterator const & it) noexcept(
            noexcept(range_access::move(std::declval<C const &>())))
            -> CPP_broken_friend_ret(
                decltype(range_access::move(std::declval<C const &>())))(
                requires same_as<C, Cur> && detail::input_cursor<Cur>)
        {
            return range_access::move(it.pos());
        }
#endif
    };

    template(typename Cur, typename Cur2)(
        requires detail::sentinel_for_cursor<Cur2, Cur>)
    constexpr bool operator==(basic_iterator<Cur> const & left,
                              basic_iterator<Cur2> const & right)
    {
        return range_access::equal(range_access::pos(left), range_access::pos(right));
    }
    template(typename Cur, typename Cur2)(
        requires detail::sentinel_for_cursor<Cur2, Cur>)
    constexpr bool operator!=(basic_iterator<Cur> const & left,
                              basic_iterator<Cur2> const & right)
    {
        return !(left == right);
    }
    template(typename Cur, typename S)(
        requires detail::sentinel_for_cursor<S, Cur>)
    constexpr bool operator==(basic_iterator<Cur> const & left,
                              S const & right)
    {
        return range_access::equal(range_access::pos(left), right);
    }
    template(typename Cur, typename S)(
        requires detail::sentinel_for_cursor<S, Cur>)
    constexpr bool operator!=(basic_iterator<Cur> const & left,
                              S const & right)
    {
        return !(left == right);
    }
    template(typename S, typename Cur)(
        requires detail::sentinel_for_cursor<S, Cur>)
    constexpr bool operator==(S const & left,
                              basic_iterator<Cur> const & right)
    {
        return right == left;
    }
    template(typename S, typename Cur)(
        requires detail::sentinel_for_cursor<S, Cur>)
    constexpr bool operator!=(S const & left,
                              basic_iterator<Cur> const & right)
    {
        return right != left;
    }

    template(typename Cur)(
        requires detail::random_access_cursor<Cur>)
    constexpr basic_iterator<Cur> //
    operator+(basic_iterator<Cur> left, typename basic_iterator<Cur>::difference_type n)
    {
        left += n;
        return left;
    }
    template(typename Cur)(
        requires detail::random_access_cursor<Cur>)
    constexpr basic_iterator<Cur> //
    operator+(typename basic_iterator<Cur>::difference_type n, basic_iterator<Cur> right)
    {
        right += n;
        return right;
    }
    template(typename Cur)(
        requires detail::random_access_cursor<Cur>)
    constexpr basic_iterator<Cur> //
    operator-(basic_iterator<Cur> left, typename basic_iterator<Cur>::difference_type n)
    {
        left -= n;
        return left;
    }
    template(typename Cur2, typename Cur)(
        requires detail::sized_sentinel_for_cursor<Cur2, Cur>)
    constexpr typename basic_iterator<Cur>::difference_type //
    operator-(basic_iterator<Cur2> const & left, basic_iterator<Cur> const & right)
    {
        return range_access::distance_to(range_access::pos(right),
                                         range_access::pos(left));
    }
    template(typename S, typename Cur)(
        requires detail::sized_sentinel_for_cursor<S, Cur>)
    constexpr typename basic_iterator<Cur>::difference_type //
    operator-(S const & left, basic_iterator<Cur> const & right)
    {
        return range_access::distance_to(range_access::pos(right), left);
    }
    template(typename Cur, typename S)(
        requires detail::sized_sentinel_for_cursor<S, Cur>)
    constexpr typename basic_iterator<Cur>::difference_type //
    operator-(basic_iterator<Cur> const & left, S const & right)
    {
        return -(right - left);
    }
    // Asymmetric comparisons
    template(typename Left, typename Right)(
        requires detail::sized_sentinel_for_cursor<Right, Left>)
    constexpr bool operator<(basic_iterator<Left> const & left,
                             basic_iterator<Right> const & right)
    {
        return 0 < (right - left);
    }
    template(typename Left, typename Right)(
        requires detail::sized_sentinel_for_cursor<Right, Left>)
    constexpr bool operator<=(basic_iterator<Left> const & left,
                              basic_iterator<Right> const & right)
    {
        return 0 <= (right - left);
    }
    template(typename Left, typename Right)(
        requires detail::sized_sentinel_for_cursor<Right, Left>)
    constexpr bool operator>(basic_iterator<Left> const & left,
                             basic_iterator<Right> const & right)
    {
        return (right - left) < 0;
    }
    template(typename Left, typename Right)(
        requires detail::sized_sentinel_for_cursor<Right, Left>)
    constexpr bool operator>=(basic_iterator<Left> const & left,
                              basic_iterator<Right> const & right)
    {
        return (right - left) <= 0;
    }

#if RANGES_BROKEN_CPO_LOOKUP
    namespace _basic_iterator_
    {
        // Optionally support hooking iter_move when the cursor sports a
        // move() member function.
        template<typename Cur>
        constexpr auto iter_move(basic_iterator<Cur> const & it) noexcept(
            noexcept(range_access::move(std::declval<Cur const &>())))
            -> CPP_broken_friend_ret(
                decltype(range_access::move(std::declval<Cur const &>())))(
                requires detail::input_cursor<Cur>)
        {
            return range_access::move(range_access::pos(it));
        }
    } // namespace _basic_iterator_
#endif

    /// Get a cursor from a basic_iterator
    struct get_cursor_fn
    {
        template<typename Cur>
        constexpr Cur & operator()(basic_iterator<Cur> & it) const noexcept
        {
            return range_access::pos(it);
        }
        template<typename Cur>
        constexpr Cur const & operator()(basic_iterator<Cur> const & it) const noexcept
        {
            return range_access::pos(it);
        }
        template<typename Cur>
        constexpr Cur operator()(basic_iterator<Cur> && it) const
            noexcept(std::is_nothrow_move_constructible<Cur>::value)
        {
            return range_access::pos(std::move(it));
        }
    };

    /// \sa `get_cursor_fn`
    RANGES_INLINE_VARIABLE(get_cursor_fn, get_cursor)
    /// @}
} // namespace ranges

/// \cond
namespace concepts
{
    // common_reference specializations for basic_proxy_reference
    template<typename Cur, typename U, template<typename> class TQual,
             template<typename> class UQual>
    struct basic_common_reference<::ranges::detail::basic_proxy_reference_<Cur, true>, U,
                                  TQual, UQual>
      : basic_common_reference<::ranges::detail::cursor_reference_t<Cur>, U, TQual, UQual>
    {};
    template<typename T, typename Cur, template<typename> class TQual,
             template<typename> class UQual>
    struct basic_common_reference<T, ::ranges::detail::basic_proxy_reference_<Cur, true>,
                                  TQual, UQual>
      : basic_common_reference<T, ::ranges::detail::cursor_reference_t<Cur>, TQual, UQual>
    {};
    template<typename Cur1, typename Cur2, template<typename> class TQual,
             template<typename> class UQual>
    struct basic_common_reference<::ranges::detail::basic_proxy_reference_<Cur1, true>,
                                  ::ranges::detail::basic_proxy_reference_<Cur2, true>,
                                  TQual, UQual>
      : basic_common_reference<::ranges::detail::cursor_reference_t<Cur1>,
                               ::ranges::detail::cursor_reference_t<Cur2>, TQual, UQual>
    {};

    // common_type specializations for basic_proxy_reference
    template<typename Cur, typename U>
    struct common_type<::ranges::detail::basic_proxy_reference_<Cur, true>, U>
      : common_type<::ranges::range_access::cursor_value_t<Cur>, U>
    {};
    template<typename T, typename Cur>
    struct common_type<T, ::ranges::detail::basic_proxy_reference_<Cur, true>>
      : common_type<T, ::ranges::range_access::cursor_value_t<Cur>>
    {};
    template<typename Cur1, typename Cur2>
    struct common_type<::ranges::detail::basic_proxy_reference_<Cur1, true>,
                       ::ranges::detail::basic_proxy_reference_<Cur2, true>>
      : common_type<::ranges::range_access::cursor_value_t<Cur1>,
                    ::ranges::range_access::cursor_value_t<Cur2>>
    {};
} // namespace concepts

#if RANGES_CXX_VER > RANGES_CXX_STD_17
RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_MISMATCHED_TAGS
RANGES_BEGIN_NAMESPACE_STD
RANGES_BEGIN_NAMESPACE_VERSION
    template<typename, typename, template <typename> class, template<typename> class>
    struct basic_common_reference;

    // common_reference specializations for basic_proxy_reference
    template<typename Cur, typename U, template<typename> class TQual,
             template<typename> class UQual>
    struct basic_common_reference<::ranges::detail::basic_proxy_reference_<Cur, true>, U,
                                  TQual, UQual>
      : basic_common_reference<::ranges::detail::cursor_reference_t<Cur>, U, TQual, UQual>
    {};
    template<typename T, typename Cur, template<typename> class TQual,
             template<typename> class UQual>
    struct basic_common_reference<T, ::ranges::detail::basic_proxy_reference_<Cur, true>,
                                  TQual, UQual>
      : basic_common_reference<T, ::ranges::detail::cursor_reference_t<Cur>, TQual, UQual>
    {};
    template<typename Cur1, typename Cur2, template<typename> class TQual,
             template<typename> class UQual>
    struct basic_common_reference<::ranges::detail::basic_proxy_reference_<Cur1, true>,
                                  ::ranges::detail::basic_proxy_reference_<Cur2, true>,
                                  TQual, UQual>
      : basic_common_reference<::ranges::detail::cursor_reference_t<Cur1>,
                               ::ranges::detail::cursor_reference_t<Cur2>, TQual, UQual>
    {};

    template<typename...>
    struct common_type;

    // common_type specializations for basic_proxy_reference
    template<typename Cur, typename U>
    struct common_type<::ranges::detail::basic_proxy_reference_<Cur, true>, U>
      : common_type<::ranges::range_access::cursor_value_t<Cur>, U>
    {};
    template<typename T, typename Cur>
    struct common_type<T, ::ranges::detail::basic_proxy_reference_<Cur, true>>
      : common_type<T, ::ranges::range_access::cursor_value_t<Cur>>
    {};
    template<typename Cur1, typename Cur2>
    struct common_type<::ranges::detail::basic_proxy_reference_<Cur1, true>,
                       ::ranges::detail::basic_proxy_reference_<Cur2, true>>
      : common_type<::ranges::range_access::cursor_value_t<Cur1>,
                    ::ranges::range_access::cursor_value_t<Cur2>>
    {};
RANGES_END_NAMESPACE_VERSION
RANGES_END_NAMESPACE_STD
RANGES_DIAGNOSTIC_POP
#endif // RANGES_CXX_VER > RANGES_CXX_STD_17

namespace ranges
{
    /// \cond
    namespace detail
    {
        template<typename Cur, bool IsReadable>
        struct std_iterator_traits_
        {
            using difference_type =
                typename iterator_associated_types_base<Cur>::difference_type;
            using value_type = void;
            using reference = void;
            using pointer = void;
            using iterator_category = std::output_iterator_tag;
            using iterator_concept = std::output_iterator_tag;
        };

        template<typename Cur>
        struct std_iterator_traits_<Cur, true> : iterator_associated_types_base<Cur>
        {};

        template<typename Cur>
        using std_iterator_traits = std_iterator_traits_<Cur, (bool)readable_cursor<Cur>>;
    } // namespace detail
    /// \endcond
} // namespace ranges

namespace std
{
    template<typename Cur>
    struct iterator_traits<::ranges::basic_iterator<Cur>>
      : ::ranges::detail::std_iterator_traits<Cur>
    {};
} // namespace std
/// \endcond

RANGES_DIAGNOSTIC_POP

#include <range/v3/detail/epilogue.hpp>

#endif
