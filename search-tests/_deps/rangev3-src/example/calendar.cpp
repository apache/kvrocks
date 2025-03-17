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

#include <range/v3/detail/config.hpp>

#if RANGES_CXX_RETURN_TYPE_DEDUCTION >= RANGES_CXX_RETURN_TYPE_DEDUCTION_14 && \
    RANGES_CXX_GENERIC_LAMBDAS >= RANGES_CXX_GENERIC_LAMBDAS_14

///[calendar]

// Usage:
//     calendar 2015
//
// Output:
/*
        January              February                March
              1  2  3   1  2  3  4  5  6  7   1  2  3  4  5  6  7
  4  5  6  7  8  9 10   8  9 10 11 12 13 14   8  9 10 11 12 13 14
 11 12 13 14 15 16 17  15 16 17 18 19 20 21  15 16 17 18 19 20 21
 18 19 20 21 22 23 24  22 23 24 25 26 27 28  22 23 24 25 26 27 28
 25 26 27 28 29 30 31                        29 30 31

         April                  May                  June
           1  2  3  4                  1  2      1  2  3  4  5  6
  5  6  7  8  9 10 11   3  4  5  6  7  8  9   7  8  9 10 11 12 13
 12 13 14 15 16 17 18  10 11 12 13 14 15 16  14 15 16 17 18 19 20
 19 20 21 22 23 24 25  17 18 19 20 21 22 23  21 22 23 24 25 26 27
 26 27 28 29 30        24 25 26 27 28 29 30  28 29 30
                       31
         July                 August               September
           1  2  3  4                     1         1  2  3  4  5
  5  6  7  8  9 10 11   2  3  4  5  6  7  8   6  7  8  9 10 11 12
 12 13 14 15 16 17 18   9 10 11 12 13 14 15  13 14 15 16 17 18 19
 19 20 21 22 23 24 25  16 17 18 19 20 21 22  20 21 22 23 24 25 26
 26 27 28 29 30 31     23 24 25 26 27 28 29  27 28 29 30
                       30 31
        October              November              December
              1  2  3   1  2  3  4  5  6  7         1  2  3  4  5
  4  5  6  7  8  9 10   8  9 10 11 12 13 14   6  7  8  9 10 11 12
 11 12 13 14 15 16 17  15 16 17 18 19 20 21  13 14 15 16 17 18 19
 18 19 20 21 22 23 24  22 23 24 25 26 27 28  20 21 22 23 24 25 26
 25 26 27 28 29 30 31  29 30                 27 28 29 30 31
// */

// Credits:
//   Thanks to H. S. Teoh for the article that served as the
//     inspiration for this example:
//     <http://wiki.dlang.org/Component_programming_with_ranges>
//   Thanks to github's Arzar for bringing date::week_number
//     to my attention.

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <algorithm>
#include <cstddef>
#include <functional>
#include <iostream>
#include <range/v3/action/join.hpp>
#include <range/v3/algorithm/copy.hpp>
#include <range/v3/algorithm/for_each.hpp>
#include <range/v3/algorithm/mismatch.hpp>
#include <range/v3/core.hpp>
#include <range/v3/iterator/stream_iterators.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/chunk.hpp>
#include <range/v3/view/chunk_by.hpp>
#include <range/v3/view/concat.hpp>
#include <range/v3/view/iota.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/repeat_n.hpp>
#include <range/v3/view/single.hpp>
#include <range/v3/view/take.hpp>
#include <range/v3/view/transform.hpp>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace po = boost::program_options;
namespace greg = boost::gregorian;
using date = greg::date;
using day = greg::date_duration;
using namespace ranges;

namespace boost
{
    namespace gregorian
    {
        date &operator++(date &d)
        {
            return d = d + day(1);
        }
        date operator++(date &d, int)
        {
            return ++d - day(1);
        }
    }
}
namespace ranges
{
    template<>
    struct incrementable_traits<date>
    {
        using difference_type = date::duration_type::duration_rep::int_type;
    };
}
CPP_assert(incrementable<date>);

auto
dates(unsigned short start, unsigned short stop)
{
    return views::iota(date{start, greg::Jan, 1}, date{stop, greg::Jan, 1});
}

auto
dates_from(unsigned short year)
{
    return views::iota(date{year, greg::Jan, 1});
}

auto
by_month()
{
    return views::chunk_by(
        [](date a, date b) { return a.month() == b.month(); });
}

auto
by_week()
{
    return views::chunk_by([](date a, date b) {
        // ++a because week_number is Mon-Sun and we want Sun-Sat
        return (++a).week_number() == (++b).week_number();
    });
}

std::string
format_day(date d)
{
    return boost::str(boost::format("%|3|") % d.day());
}

// In:  range<range<date>>: month grouped by weeks.
// Out: range<std::string>: month with formatted weeks.
auto
format_weeks()
{
    return views::transform([](/*range<date>*/ auto week) {
        return boost::str(boost::format("%1%%2%%|22t|") %
                          std::string(front(week).day_of_week() * 3u, ' ') %
                          (week | views::transform(format_day) | actions::join));
    });
}

// Return a formatted string with the title of the month
// corresponding to a date.
std::string
month_title(date d)
{
    return boost::str(boost::format("%|=22|") % d.month().as_long_string());
}

// In:  range<range<date>>: year of months of days
// Out: range<range<std::string>>: year of months of formatted wks
auto
layout_months()
{
    return views::transform([](/*range<date>*/ auto month) {
        auto week_count =
            static_cast<std::ptrdiff_t>(distance(month | by_week()));
        return views::concat(
            views::single(month_title(front(month))),
            month | by_week() | format_weeks(),
            views::repeat_n(std::string(22, ' '), 6 - week_count));
    });
}

// Flattens a range of ranges by iterating the inner
// ranges in round-robin fashion.
template<class Rngs>
class interleave_view : public view_facade<interleave_view<Rngs>>
{
    friend range_access;
    std::vector<range_value_t<Rngs>> rngs_;
    struct cursor;
    cursor begin_cursor()
    {
        return {0, &rngs_, views::transform(rngs_, ranges::begin) | to<std::vector>};
    }

public:
    interleave_view() = default;
    explicit interleave_view(Rngs rngs)
      : rngs_(std::move(rngs) | to<std::vector>)
    {}
};

template<class Rngs>
struct interleave_view<Rngs>::cursor
{
    std::size_t n_;
    std::vector<range_value_t<Rngs>> *rngs_;
    std::vector<iterator_t<range_value_t<Rngs>>> its_;
    decltype(auto) read() const
    {
        return *its_[n_];
    }
    void next()
    {
        if(0 == ((++n_) %= its_.size()))
            for_each(its_, [](auto &it) { ++it; });
    }
    bool equal(default_sentinel_t) const
    {
        if(n_ != 0)
            return false;
        auto ends = *rngs_ | views::transform(ranges::end);
        return its_.end() != std::mismatch(
            its_.begin(), its_.end(), ends.begin(), std::not_equal_to<>{}).first;
    }
    CPP_member
    auto equal(cursor const& that) const -> CPP_ret(bool)(
        requires forward_range<range_value_t<Rngs>>)
    {
        return n_ == that.n_ && its_ == that.its_;
    }
};

// In:  range<range<T>>
// Out: range<T>, flattened by walking the ranges
//                round-robin fashion.
auto
interleave()
{
    return make_view_closure([](auto &&rngs) {
        using Rngs = decltype(rngs);
        return interleave_view<views::all_t<Rngs>>(
            views::all(std::forward<Rngs>(rngs)));
    });
}

// In:  range<range<T>>
// Out: range<range<T>>, transposing the rows and columns.
auto
transpose()
{
    return make_view_closure([](auto &&rngs) {
        using Rngs = decltype(rngs);
        CPP_assert(forward_range<Rngs>);
        return std::forward<Rngs>(rngs)
            | interleave()
            | views::chunk(static_cast<std::size_t>(distance(rngs)));
    });
}

// In:  range<range<range<string>>>
// Out: range<range<range<string>>>, transposing months.
auto
transpose_months()
{
    return views::transform(
        [](/*range<range<string>>*/ auto rng) { return rng | transpose(); });
}

// In:  range<range<string>>
// Out: range<string>, joining the strings of the inner ranges
auto
join_months()
{
    return views::transform(
        [](/*range<string>*/ auto rng) { return actions::join(rng); });
}

// In:  range<date>
// Out: range<string>, lines of formatted output
auto
format_calendar(std::size_t months_per_line)
{
    return
        // Group the dates by month:
        by_month()
        // Format the month into a range of strings:
      | layout_months()
        // Group the months that belong side-by-side:
      | views::chunk(months_per_line)
        // Transpose the rows and columns of the size-by-side months:
      | transpose_months()
        // Ungroup the side-by-side months:
      | views::join
        // Join the strings of the transposed months:
      | join_months();
}

int
main(int argc, char *argv[]) try
{
    // Declare the supported options.
    po::options_description desc("Allowed options");
    desc.add_options()("help", "produce help message")(
        "start", po::value<unsigned short>(), "Year to start")(
        "stop", po::value<std::string>(), "Year to stop")(
        "per-line",
        po::value<std::size_t>()->default_value(3u),
        "Nbr of months per line");

    po::positional_options_description p;
    p.add("start", 1).add("stop", 1);

    po::variables_map vm;
    po::store(
        po::command_line_parser(argc, argv).options(desc).positional(p).run(),
        vm);
    po::notify(vm);

    if(vm.count("help") || 1 != vm.count("start"))
    {
        std::cerr << desc << '\n';
        return 1;
    }

    auto const start = vm["start"].as<unsigned short>();
    auto const stop = 0 == vm.count("stop")
                          ? (unsigned short)(start + 1)
                          : vm["stop"].as<std::string>() == "never"
                                ? (unsigned short)-1
                                : boost::lexical_cast<unsigned short>(
                                      vm["stop"].as<std::string>());
    auto const months_per_line = vm["per-line"].as<std::size_t>();

    if(stop != (unsigned short)-1 && stop <= start)
    {
        std::cerr << "ERROR: The stop year must be larger than the start"
                  << '\n';
        return 1;
    }

    if((unsigned short)-1 != stop)
    {
        copy(dates(start, stop) | format_calendar(months_per_line),
             ostream_iterator<>(std::cout, "\n"));
    }
    else
    {
        copy(dates_from(start) | format_calendar(months_per_line),
             ostream_iterator<>(std::cout, "\n"));
    }
}
catch(std::exception &e)
{
    std::cerr << "ERROR: Unhandled exception\n";
    std::cerr << "  what(): " << e.what();
    return 1;
}
///[calendar]

#else
#pragma message( \
    "calendar requires C++14 return type deduction and generic lambdas")
int
main()
{}
#endif
