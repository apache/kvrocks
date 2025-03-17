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

#include <cstring>
#include <tuple>

#include <range/v3/iterator/basic_iterator.hpp>
#include <range/v3/utility/common_tuple.hpp>

#include "../simple_test.hpp"
#include "../test_utils.hpp"

template<typename T>
using iter_cat_t = typename T::iterator_category;
template<typename T>
using has_iter_cat = meta::is_trait<meta::defer<iter_cat_t, T>>;

namespace test_weak_input
{
    template<typename I>
    struct cursor
    {
        I it_;
        struct mixin : ranges::basic_mixin<cursor>
        {
            mixin() = default;
            //using ranges::basic_mixin<cursor>::basic_mixin;
            explicit mixin(cursor && cur)
              : ranges::basic_mixin<cursor>(static_cast<cursor &&>(cur))
            {}
            explicit mixin(cursor const & cur)
              : ranges::basic_mixin<cursor>(cur)
            {}
            mixin(I i)
              : mixin(cursor{i})
            {}
        };
        cursor() = default;
        explicit cursor(I i)
          : it_(i)
        {}
        CPP_template(class J)(
            /// \pre
            requires ranges::convertible_to<J, I>)  //
        cursor(cursor<J> that)
          : it_(std::move(that.it_))
        {}

        auto read() const -> decltype(*it_)
        {
            return *it_;
        }
        void next()
        {
            ++it_;
        }
    };

    CPP_assert(ranges::detail::input_cursor<cursor<char *>>);
    CPP_assert(!ranges::detail::sentinel_for_cursor<cursor<char *>, cursor<char *>>);

    template<class I>
    using iterator = ranges::basic_iterator<cursor<I>>;
    CPP_assert(ranges::indirectly_readable<iterator<char *>>);
    CPP_assert(ranges::input_iterator<iterator<char *>>);

    static_assert(!has_iter_cat<iterator<char *>>::value, "");
    static_assert(!has_iter_cat<std::iterator_traits<iterator<char *>>>::value, "");
    static_assert(
        std::is_same<iterator<char *>::iterator_concept, std::input_iterator_tag>::value,
        "");
    static_assert(!ranges::equality_comparable<iterator<char *>>, "");

    void test()
    {
        using namespace ranges;
        using I = iterator<char const *>;
        CPP_assert(std::is_same<std::iterator_traits<I>::pointer, char const *>{});

        static char const sz[] = "hello world";
        I i{sz};
        CHECK(*i == 'h');
        CHECK(&*i == i.operator->());
        ++i;
        CHECK(*i == 'e');
        CHECK(&*i == i.operator->());
    }
} // namespace test_weak_input

namespace test_random_access
{
    template<typename I>
    struct cursor
    {
        I it_;
        struct mixin : ranges::basic_mixin<cursor>
        {
            mixin() = default;
            //using ranges::basic_mixin<cursor>::basic_mixin;
            explicit mixin(cursor && cur)
              : ranges::basic_mixin<cursor>(static_cast<cursor &&>(cur))
            {}
            explicit mixin(cursor const & cur)
              : ranges::basic_mixin<cursor>(cur)
            {}
            mixin(I i)
              : mixin(cursor{i})
            {}
        };
        cursor() = default;
        explicit cursor(I i)
          : it_(i)
        {}
        CPP_template(class J)(
            /// \pre
            requires ranges::convertible_to<J, I>)  //
        cursor(cursor<J> that)
          : it_(std::move(that.it_))
        {}

        auto read() const -> decltype(*it_)
        {
            return *it_;
        }
        CPP_template(class J)(
            /// \pre
            requires ranges::sentinel_for<J, I>)    //
        bool equal(cursor<J> const & that) const
        {
            return that.it_ == it_;
        }
        void next()
        {
            ++it_;
        }
        void prev()
        {
            --it_;
        }
        void advance(ranges::iter_difference_t<I> n)
        {
            it_ += n;
        }
        CPP_template(class J)(
            /// \pre
            requires ranges::sized_sentinel_for<J, I>)  //
        ranges::iter_difference_t<I> distance_to(cursor<J> const & that) const
        {
            return that.it_ - it_;
        }
    };

    CPP_assert(ranges::detail::random_access_cursor<cursor<char *>>);

    template<class I>
    using iterator = ranges::basic_iterator<cursor<I>>;

    static_assert(std::is_same<iterator<char *>::iterator_category,
                               std::random_access_iterator_tag>::value,
                  "");

    void test()
    {
        using namespace ranges;

        iterator<char *> a(nullptr);
        iterator<char const *> b(nullptr);
        iterator<char const *> c(a);

        CPP_assert(
            std::is_same<std::iterator_traits<iterator<char *>>::pointer, char *>{});

        b = a;
        bool d = a == b;
        d = (a != b);

        detail::ignore_unused(
            d,      //
            a < b,  //
            a <= b, //
            a > b,  //
            a >= b, //
            (a-b),  //
            (b-a),  //
            (a-a),  //
            (b-b)); //
    }
} // namespace test_random_access

 namespace test_weak_output
{
    template<typename I>
    struct cursor
    {
        struct mixin : ranges::basic_mixin<cursor>
        {
            mixin() = default;
            // using ranges::basic_mixin<cursor>::basic_mixin;
            explicit mixin(cursor && cur)
              : ranges::basic_mixin<cursor>(static_cast<cursor &&>(cur))
            {}
            explicit mixin(cursor const & cur)
              : ranges::basic_mixin<cursor>(cur)
            {}
            explicit mixin(I i)
              : mixin(cursor{i})
            {}
        };

        cursor() = default;
        explicit cursor(I i)
          : it_(i)
        {}

        void write(ranges::iter_value_t<I> v) const
        {
            *it_ = v;
        }
        void next()
        {
            ++it_;
        }

    private:
        I it_;
    };

    CPP_assert(ranges::detail::output_cursor<cursor<char *>, char>);
    CPP_assert(!ranges::detail::sentinel_for_cursor<cursor<char *>, cursor<char *>>);

    template<class I>
    using iterator = ranges::basic_iterator<cursor<I>>;

    CPP_assert(ranges::output_iterator<iterator<char *>, char>);
    CPP_assert(!ranges::equality_comparable<iterator<char *>>);

    void test()
    {
        char buf[10];
        iterator<char *> i(buf);
        *i = 'h';
        ++i;
        *i = 'e';
        ++i;
        *i = 'l';
        ++i;
        *i = 'l';
        ++i;
        *i = 'o';
        ++i;
        *i = '\0';
        CHECK(0 == std::strcmp(buf, "hello"));
    }
} // namespace test_weak_output

 namespace test_output
{
    template<typename I>
    struct cursor
    {
        I it_;
        struct mixin : ranges::basic_mixin<cursor>
        {
            mixin() = default;
            //using ranges::basic_mixin<cursor>::basic_mixin;
            explicit mixin(cursor && cur)
              : ranges::basic_mixin<cursor>(static_cast<cursor &&>(cur))
            {}
            explicit mixin(cursor const & cur)
              : ranges::basic_mixin<cursor>(cur)
            {}
            mixin(I i)
              : mixin(cursor{i})
            {}
        };
        cursor() = default;
        explicit cursor(I i)
          : it_(i)
        {}
        CPP_template(class J)(
            /// \pre
            requires ranges::convertible_to<J, I>)  //
        cursor(cursor<J> that)
          : it_(std::move(that.it_))
        {}

        using value_type = ranges::iter_value_t<I>;
        value_type read() const
        {
            return *it_;
        }
        void write(value_type v) const
        {
            *it_ = v;
        }
        I arrow() const
        {
            return it_;
        }
        void next()
        {
            ++it_;
        }
        bool equal(cursor const & that) const
        {
            return it_ == that.it_;
        }
    };

    CPP_assert(ranges::detail::output_cursor<cursor<char *>, char>);
    CPP_assert(ranges::detail::forward_cursor<cursor<char *>>);

    template<class I>
    using iterator = ranges::basic_iterator<cursor<I>>;

    CPP_assert(ranges::output_iterator<iterator<char *>, char>);
    CPP_assert(ranges::forward_iterator<iterator<char *>>);

    CPP_assert(std::is_same<std::iterator_traits<iterator<char *>>::pointer, char *>());

    void test()
    {
        char buf[10];
        iterator<char *> i(buf);
        *i = 'h';
        CHECK(*i == 'h');
        CHECK(*i == *i);
        ++i;
        *i = 'e';
        CHECK('e' == *i);
        ++i;
        *i = 'l';
        ++i;
        *i = 'l';
        ++i;
        *i = 'o';
        ++i;
        *i = '\0';
        CHECK(0 == std::strcmp(buf, "hello"));
        CHECK(i == iterator<char *>{buf + 5});
        ++i;
        CHECK(i != iterator<char *>{buf + 5});
        CHECK(i == iterator<char *>{buf + 6});
    }
} // namespace test_output

 namespace test_move_only
{
    struct MoveOnly
    {
        MoveOnly() = default;
        MoveOnly(MoveOnly &&) = default;
        MoveOnly(MoveOnly const &) = delete;
        MoveOnly & operator=(MoveOnly &&) = default;
        MoveOnly & operator=(MoveOnly const &) = delete;
    };

    template<typename I>
    struct zip1_cursor
    {
        I it_;
        struct mixin : ranges::basic_mixin<zip1_cursor>
        {
            mixin() = default;
            // using ranges::basic_mixin<zip1_cursor>::basic_mixin;
            explicit mixin(zip1_cursor && cur)
              : ranges::basic_mixin<zip1_cursor>(static_cast<zip1_cursor &&>(cur))
            {}
            explicit mixin(zip1_cursor const & cur)
              : ranges::basic_mixin<zip1_cursor>(cur)
            {}
            mixin(I i)
              : mixin(zip1_cursor{i})
            {}
        };
        zip1_cursor() = default;
        explicit zip1_cursor(I i)
          : it_(i)
        {}
        CPP_template(class J)(
            /// \pre
            requires ranges::convertible_to<J, I>)  //
        zip1_cursor(zip1_cursor<J> that)
          : it_(std::move(that.it_))
        {}

        using value_type = std::tuple<ranges::iter_value_t<I>>;
        using reference = ranges::common_tuple<ranges::iter_reference_t<I>>;
        using rvalue_reference = ranges::common_tuple<ranges::iter_rvalue_reference_t<I>>;
        reference read() const
        {
            return reference{*it_};
        }
        rvalue_reference move() const
        {
            return rvalue_reference{ranges::iter_move(it_)};
        }
        void write(reference const & v) const
        {
            reference{ *it_} = v;
        }
        void write(value_type && v) const
        {
            reference{ *it_} = std::move(v);
        }
        void next()
        {
            ++it_;
        }
        bool equal(zip1_cursor const & that) const
        {
            return it_ == that.it_;
        }
    };

    CPP_assert(
        ranges::detail::output_cursor<zip1_cursor<MoveOnly *>, std::tuple<MoveOnly>
        &&>);
    CPP_assert(ranges::detail::forward_cursor<zip1_cursor<MoveOnly *>>);

    template<class I>
    using iterator = ranges::basic_iterator<zip1_cursor<I>>;

    CPP_assert(ranges::output_iterator<iterator<MoveOnly *>, std::tuple<MoveOnly> &&>);
    CPP_assert(ranges::forward_iterator<iterator<MoveOnly *>>);

    void test()
    {
        MoveOnly buf[10] = {};
        iterator<MoveOnly *> i(buf);
        *i = std::tuple<MoveOnly>{};
        ranges::common_tuple<MoveOnly &> x = *i;
        (void)x;
        std::tuple<MoveOnly> v = ranges::iter_move(i);
        *i = std::move(v);
    }
} // namespace test_move_only

 namespace test_forward_sized
{
    template<typename I>
    struct cursor
    {
        I it_;
        struct mixin : ranges::basic_mixin<cursor>
        {
            mixin() = default;
            // using ranges::basic_mixin<cursor>::basic_mixin;
            explicit mixin(cursor && cur)
              : ranges::basic_mixin<cursor>(static_cast<cursor &&>(cur))
            {}
            explicit mixin(cursor const & cur)
              : ranges::basic_mixin<cursor>(cur)
            {}
            mixin(I i)
              : mixin(cursor{i})
            {}
        };
        cursor() = default;
        explicit cursor(I i)
          : it_(i)
        {}
        CPP_template(class J)(
            /// \pre
            requires ranges::convertible_to<J, I>)  //
        cursor(cursor<J> that)
          : it_(std::move(that.it_))
        {}

        auto read() const -> decltype(*it_)
        {
            return *it_;
        }
        CPP_template(class J)(
            /// \pre
            requires ranges::sentinel_for<J, I>)    //
        bool equal(cursor<J> const & that) const
        {
            return that.it_ == it_;
        }
        void next()
        {
            ++it_;
        }
        CPP_template(class J)(
            /// \pre
            requires ranges::sized_sentinel_for<J, I>)  //
        ranges::iter_difference_t<I> distance_to(cursor<J> const & that) const
        {
            return that.it_ - it_;
        }
    };

    CPP_assert(ranges::detail::sized_sentinel_for_cursor<cursor<char *>, cursor<char *>>);
    CPP_assert(ranges::detail::forward_cursor<cursor<char *>>);

    template<class I>
    using iterator = ranges::basic_iterator<cursor<I>>;

    static_assert(std::is_same<iterator<char *>::iterator_category,
                               std::forward_iterator_tag>::value,
                  "");

    void test()
    {
        using namespace ranges;

        iterator<char *> a(nullptr);
        iterator<char const *> b(nullptr);
        iterator<char const *> c(a);

        b = a;
        bool d = a == b;
        d = (a != b);

        detail::ignore_unused(
            d,      //
            a < b,  //
            a <= b, //
            a > b,  //
            a >= b, //
            (a-b),  //
            (b-a),  //
            (a-a),  //
            (b-b)); //
    }
} // namespace test_forward_sized

RANGES_DIAGNOSTIC_IGNORE_UNNEEDED_MEMBER

void test_box()
{
    struct A : ranges::box<int>
    {};
    CHECK(sizeof(A) == sizeof(int));
    struct empty
    {};
    struct B : ranges::box<empty>
    {
        int i;
    };
    CHECK(sizeof(B) == sizeof(int));
    B b1, b2;
    if(ranges::detail::box_compression<empty>() == ranges::detail::box_compress::coalesce)
        CHECK((&b1.get() == &b2.get()));
    struct nontrivial
    {
        nontrivial()
        {}
    };
    struct C : ranges::box<nontrivial>
    {
        int i;
    };
    CHECK(sizeof(C) == sizeof(int));
    C c1, c2;
    CHECK((&c1.get() != &c2.get()));

    {
        // empty but not trivial cursor that defines value_type:
        struct cursor
        {
            using value_type = int;
            cursor()
            {}
            int read() const
            {
                return 42;
            }
            void next()
            {}
        };
        CPP_assert(ranges::detail::box_compression<cursor>() ==
                   ranges::detail::box_compress::ebo);
        CPP_assert(ranges::same_as<int, ranges::basic_iterator<cursor>::value_type>);
    }
}

int main()
{
    using namespace ranges;
    std::cout << "\nTesting basic_iterator\n";

    ::test_weak_input::test();
    ::test_random_access::test();
    ::test_weak_output::test();
    ::test_output::test();
    ::test_move_only::test();
    ::test_forward_sized::test();
    ::test_box();

    return ::test_result();
}
