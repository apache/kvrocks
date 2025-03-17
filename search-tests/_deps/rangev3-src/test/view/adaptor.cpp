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

#include <list>
#include <vector>
#include <sstream>
#include <range/v3/core.hpp>
#include <range/v3/utility/copy.hpp>
#include <range/v3/view/delimit.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

template<typename BidiRange>
struct my_reverse_view
  : ranges::view_adaptor<my_reverse_view<BidiRange>, BidiRange>
{
private:
    CPP_assert(ranges::bidirectional_range<BidiRange>);
    CPP_assert(ranges::common_range<BidiRange>);
    friend ranges::range_access;
    using base_iterator_t = ranges::iterator_t<BidiRange>;

    struct adaptor : ranges::adaptor_base
    {
        template<class base_mixin>
        struct mixin : base_mixin
        {
            mixin() = default;
            using base_mixin::base_mixin;

            int mixin_int = 120;

            int base_plus_adaptor() const
            {
                int y = this->get().t;
                return *this->base() + y;
            }
        };

        int t = 20;

        // Cross-wire begin and end.
        base_iterator_t begin(my_reverse_view const &rng) const
        {
            return ranges::end(rng.base());
        }
        base_iterator_t end(my_reverse_view const &rng) const
        {
            return ranges::begin(rng.base());
        }
        void next(base_iterator_t &it) const
        {
            --it;
        }
        void prev(base_iterator_t &it) const
        {
            ++it;
        }
        ranges::range_reference_t<BidiRange> read(base_iterator_t it) const
        {
            return *ranges::prev(it);
        }
        CPP_member
        auto advance(base_iterator_t &it,
            ranges::range_difference_t<BidiRange> n) const ->
            CPP_ret(void)(
                requires ranges::random_access_range<BidiRange>)
        {
            it -= n;
        }
        CPP_member
        auto distance_to(base_iterator_t const &here,
            base_iterator_t const &there) const ->
            CPP_ret(ranges::range_difference_t<BidiRange>)(
                requires ranges::sized_sentinel_for<base_iterator_t, base_iterator_t>)
        {
            return here - there;
        }
    };
    adaptor begin_adaptor() const
    {
        return {};
    }
    adaptor end_adaptor() const
    {
        return {};
    }
public:
    using my_reverse_view::view_adaptor::view_adaptor;
};

struct my_delimited_range
  : ranges::view_adaptor<
        my_delimited_range,
        ranges::delimit_view<ranges::istream_view<int>, int>>
{
    using view_adaptor::view_adaptor;

    struct adaptor : ranges::adaptor_base
    {
        template<class base_mixin>
        struct mixin : base_mixin
        {
            mixin() = default;
            using base_mixin::base_mixin;

            int mixin_int = 120;

            int adaptor_access_test() const
            {
                int y = this->get().t;
                return y;
            }
        };

        int t = 20;
    };
    adaptor begin_adaptor() const
    {
        return {};
    }
    adaptor end_adaptor() const
    {
        return {};
    }
};

int main()
{
    using namespace ranges;
    std::vector<int> v{1, 2, 3, 4};
    my_reverse_view<std::vector<int>&> retro{v};
    CPP_assert(common_range<decltype(retro)>);
    CPP_assert(view_<decltype(retro)>);
    CPP_assert(random_access_iterator<decltype(retro.begin())>);
    ::check_equal(retro, {4, 3, 2, 1});

    // test cursor mixin
    CHECK( retro.begin().mixin_int == 120 );
    CHECK( *((retro.begin()+1).base()) == 4 );
    CHECK( (retro.begin()+1).base_plus_adaptor() == 24 );

    std::list<int> l{1, 2, 3, 4};
    my_reverse_view<std::list<int>& > retro2{l};
    CPP_assert(common_range<decltype(retro2)>);
    CPP_assert(view_<decltype(retro2)>);
    CPP_assert(bidirectional_iterator<decltype(retro2.begin())>);
    CPP_assert(!random_access_iterator<decltype(retro2.begin())>);
    ::check_equal(retro2, {4, 3, 2, 1});

    std::stringstream sinx("1 2 3 4 5 6 7 8 9 1 2 3 4 5 6 7 8 9 1 2 3 4 42 6 7 8 9 ");
    my_delimited_range r{views::delimit(istream<int>(sinx), 42)};
    CPP_assert(view_<decltype(r)>);
    CPP_assert(!common_range<decltype(r)>);
    CPP_assert(input_iterator<decltype(r.begin())>);
    CPP_assert(!forward_iterator<decltype(r.begin())>);
    ::check_equal(r, {1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4});

    // test sentinel mixin
    CHECK(r.end().mixin_int == 120);
    CHECK(r.end().adaptor_access_test() == 20);

    return ::test_result();
}
