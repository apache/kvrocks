// Range v3 library
//
//  Copyright Eric Niebler 2013-present
//  Copyright Gonzalo Brito Gadeschi 2015
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

#include <iostream>
#include <iomanip>
#include <vector>
#include <random>
#include <functional>
#include <climits>
#include <chrono>
#include <algorithm>
#include <range/v3/all.hpp>

RANGES_DIAGNOSTIC_IGNORE_GLOBAL_CONSTRUCTORS
RANGES_DIAGNOSTIC_IGNORE_SIGN_CONVERSION

namespace
{
  /// Creates an geometric infinite sequence starting at 1 where the
  /// successor is multiplied by \p V
  auto geometric_sequence(std::size_t V) {
    std::size_t N = 1;
    return ranges::views::generate([N, V]() mutable {
      auto old = N;
      N *= V;
      return old;
    });
  }

  /// Creates an geometric infinite sequence starting at 1 where the
  /// successor is multiplied by \p V
  auto geometric_sequence_n(std::size_t V, std::size_t limit) {
    return geometric_sequence(V) |
      ranges::views::take_while([limit](std::size_t n) { return n <= limit; });
  }

  /// Random uniform integer sequence
  struct random_uniform_integer_sequence {
    std::default_random_engine gen;
    std::uniform_int_distribution<> dist;
    auto operator()(std::size_t) {
      return ranges::views::generate([&]{ return dist(gen); });
    }
    static std::string name() { return "random_uniform_integer_sequence"; }
  };

  struct ascending_integer_sequence {
    auto operator()(std::size_t) { return ranges::views::iota(1); }
    static std::string name() { return "ascending_integer_sequence"; }
  };

  struct descending_integer_sequence {
    auto operator()(std::size_t) {
      return ranges::views::iota(0ll, std::numeric_limits<long long>::max()) |
            ranges::views::reverse;
    }
    static std::string name() { return "descending_integer_sequence"; }
  };

  auto even = [](auto i) { return i % 2 == 0; };
  auto odd = [](auto i) { return !even(i); };

  struct even_odd_integer_sequence {
    static std::string name() { return "even_odd_integer_sequence"; }
    auto operator()(std::size_t n) {
      return ranges::views::concat(ranges::views::ints(std::size_t{0}, n) | ranges::views::filter(even),
                                  ranges::views::ints(std::size_t{0}, n) | ranges::views::filter(odd));
    }
  };

  struct organ_pipe_integer_sequence {
    static std::string name() { return "organ_pipe_integer_sequence"; }
    auto operator()(std::size_t n) {
      return ranges::views::concat(ranges::views::ints(std::size_t{0}, n/2),
                                  ranges::views::ints(std::size_t{0}, n/2 + 1)
                                  | ranges::views::reverse);
    }
  };

  template<typename Seq>
  void print(Seq seq, std::size_t n) {
    std::cout << "sequence: " << seq.name() << '\n';
    RANGES_FOR(auto i, seq(n) | ranges::views::take(n)) {
      std::cout << i << '\n';
    }
  }

  /// Returns the duration of a computation
  using clock_t = std::chrono::high_resolution_clock;
  using duration_t = clock_t::duration;

  template<typename Computation>
  auto duration(Computation &&c) {
    auto time = []{ return clock_t::now(); };
    const auto start = time();
    c();
    return time() - start;
  }

  template<typename Duration>
  auto to_millis(Duration d) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
  }

  template<typename Durations> auto compute_mean(Durations &&durations) {
    using D = ranges::range_value_t<Durations>;
    D total = ranges::accumulate(durations, D{}, ranges::plus{}, ranges::convert_to<D>{});
    return total / ranges::size(durations);
  }

  template<typename Durations> auto compute_stddev(Durations &&durations) {
    using D = ranges::range_value_t<Durations>;
    using Rep = typename D::rep;
    const auto mean = compute_mean(durations);
    const auto stddev = ranges::accumulate(
      durations | ranges::views::transform([=](auto i) {
        auto const delta = (i - mean).count();
        return delta * delta;
      }), Rep{}, ranges::plus{}, ranges::convert_to<Rep>{});
    return D{static_cast<typename D::rep>(std::sqrt(stddev / ranges::size(durations)))};
  }

  struct benchmark {
    struct result_t {
      duration_t mean_t;
      duration_t max_t;
      duration_t min_t;
      std::size_t size;
      duration_t deviation;
    };
    std::vector<result_t> results;

    template<typename Computation, typename Sizes>
    benchmark(Computation &&c, Sizes &&sizes, double target_deviation = 0.25,
              std::size_t max_iters = 100, std::size_t min_iters = 5) {

      RANGES_FOR(auto size, sizes) {
        std::vector<duration_t> durations;
        duration_t deviation;
        duration_t mean_duration;
        std::size_t iter;

        for (iter = 0; iter < max_iters; ++iter) {
          c.init(size);
          durations.emplace_back(duration(c));
          mean_duration = compute_mean(durations);
          if (++iter == max_iters) {
            break;
          }
          if (iter >= min_iters) {
            deviation = compute_stddev(durations);
            if (deviation < target_deviation * mean_duration)
              break;
          }
        }
        auto minmax = ranges::minmax(durations);
        results.emplace_back(
            result_t{mean_duration, minmax.max, minmax.min, size, deviation});
        std::cerr << "size: " << size << " iter: " << iter
                  << " dev: " << to_millis(deviation)
                  << " mean: " << to_millis(mean_duration)
                  << " max: " << to_millis(minmax.max)
                  << " min: " << to_millis(minmax.min) << '\n';
      }
    }
  };

  template<typename Seq, typename Comp>
  struct computation_on_sequence {
    Seq seq;
    Comp comp;
    std::vector<ranges::range_value_t<decltype(seq(std::size_t{}))>> data;
    computation_on_sequence(Seq s, Comp c, std::size_t max_size)
        : seq(std::move(s)), comp(std::move(c)) {
      data.reserve(max_size);
    }
    void init(std::size_t size) {
      data.resize(size);
      ranges::copy(seq(size) | ranges::views::take(size), ranges::begin(data));
    }
    void operator()() { comp(data); }
  };

  template<typename Seq, typename Comp>
  auto make_computation_on_sequence(Seq s, Comp c, std::size_t max_size) {
    return computation_on_sequence<Seq, Comp>(std::move(s), std::move(c),
                                              max_size);
  }

  template<typename Seq> void benchmark_sort(Seq &&seq, std::size_t max_size) {
    auto ranges_sort_comp =
        make_computation_on_sequence(seq, ranges::sort, max_size);

    auto std_sort_comp = make_computation_on_sequence(
        seq, [](auto &&v) { std::sort(std::begin(v), std::end(v)); }, max_size);

    auto ranges_sort_benchmark =
        benchmark(ranges_sort_comp, geometric_sequence_n(2, max_size));

    auto std_sort_benchmark =
        benchmark(std_sort_comp, geometric_sequence_n(2, max_size));
    using std::setw;
    std::cout << '#'
              << "pattern: " << seq.name() << '\n';
    std::cout << '#' << setw(19) << 'N' << setw(20) << "ranges::sort" << setw(20)
              << "std::sort"
              << '\n';
    RANGES_FOR(auto p, ranges::views::zip(ranges_sort_benchmark.results,
                                         std_sort_benchmark.results)) {
      auto rs = p.first;
      auto ss = p.second;

      std::cout << setw(20) << rs.size << setw(20) << to_millis(rs.mean_t)
                << setw(20) << to_millis(ss.mean_t) << '\n';
    }
  }
} // unnamed namespace

int main()
{
  constexpr std::size_t max_size = 2000000;

  print(random_uniform_integer_sequence(), 20);
  print(ascending_integer_sequence(), 20);
  print(descending_integer_sequence(), 20);
  print(even_odd_integer_sequence(), 20);
  print(organ_pipe_integer_sequence(), 20);

  benchmark_sort(random_uniform_integer_sequence(), max_size);
  benchmark_sort(ascending_integer_sequence(), max_size);
  benchmark_sort(descending_integer_sequence(), max_size);
  benchmark_sort(organ_pipe_integer_sequence(), max_size);
}

#else

#pragma message("sort_patterns requires C++14 return type deduction and generic lambdas")

int main() {}

#endif
