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

#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/utility/copy.hpp>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

struct MyRange
  : ranges::view_facade<MyRange>
{
private:
    friend ranges::range_access;
    std::vector<int> ints_;
    struct cursor
    {
    private:
        std::vector<int>::const_iterator iter;
    public:
        using contiguous = std::true_type;
        cursor() = default;
        cursor(std::vector<int>::const_iterator it)
          : iter(it)
        {}
        int const & read() const
        {
            return *iter;
        }
        bool equal(cursor const &that) const
        {
            return iter == that.iter;
        }
        void next()
        {
            ++iter;
        }
        void prev()
        {
            --iter;
        }
        std::ptrdiff_t distance_to(cursor const &that) const
        {
            return that.iter - iter;
        }
        void advance(std::ptrdiff_t n)
        {
            iter += n;
        }
    };
    cursor begin_cursor() const
    {
        return {ints_.begin()};
    }
    cursor end_cursor() const
    {
        return {ints_.end()};
    }
public:
    MyRange()
      : ints_{1, 2, 3, 4, 5, 6, 7}
    {}
};

int main()
{
    using namespace ranges;
    {
        auto r = MyRange{};
        CPP_assert(view_<decltype(r)>);
        CPP_assert(common_range<decltype(r)>);
        CPP_assert(sized_range<decltype(r)>);
        CPP_assert(contiguous_range<decltype(r)>);
        ::check_equal(r, {1, 2, 3, 4, 5, 6, 7});

        CHECK(7u == r.size());
        CHECK(1 == r.front());
        CHECK(7 == r.back());
        CHECK(&*r.begin() == r.data());
        CHECK(r[1] == 2);
        CHECK(r[5] == 6);
    }

    {
        const auto r = MyRange{};
        CPP_assert(common_range<decltype(r)>);
        CPP_assert(sized_range<decltype(r)>);
        CPP_assert(random_access_range<decltype(r)>);
        CPP_assert(contiguous_range<decltype(r)>);
        ::check_equal(r, {1, 2, 3, 4, 5, 6, 7});

        CHECK(7u == r.size());
        CHECK(1 == r.front());
        CHECK(7 == r.back());
        CHECK(&*r.begin() == r.data());
        CHECK(r[1] == 2);
        CHECK(r[5] == 6);
    }

    return test_result();
}
