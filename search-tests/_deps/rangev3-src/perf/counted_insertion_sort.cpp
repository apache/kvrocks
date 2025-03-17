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

#include <chrono>
#include <iostream>
#include <random>
#include <range/v3/all.hpp>

RANGES_DIAGNOSTIC_IGNORE_SIGN_CONVERSION

class timer
{
public:
    using clock_t = std::chrono::high_resolution_clock;
    using duration_t = clock_t::time_point::duration;

    timer()
    {
        reset();
    }
    void reset()
    {
        start_ = clock_t::now();
    }
    duration_t elapsed() const
    {
        return clock_t::now() - start_;
    }
    friend std::ostream &operator<<(std::ostream &sout, timer const &t)
    {
        return sout << t.elapsed().count() << "ms";
    }
private:
    clock_t::time_point start_;
};

template<typename D>
std::chrono::milliseconds::rep to_millis(D d) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
}

template<typename It>
struct forward_iterator
{
    It it_;
public:
    typedef          std::forward_iterator_tag                 iterator_category;
    typedef typename std::iterator_traits<It>::value_type      value_type;
    typedef typename std::iterator_traits<It>::difference_type difference_type;
    typedef It                                                 pointer;
    typedef typename std::iterator_traits<It>::reference       reference;

    forward_iterator() = default;
    explicit forward_iterator(It it) : it_(std::move(it)) {}

    reference operator*() const {return *it_;}
    pointer operator->() const {return it_;}

    forward_iterator& operator++() {++it_; return *this;}
    forward_iterator operator++(int)
        {forward_iterator tmp(*this); ++(*this); return tmp;}

    friend bool operator==(const forward_iterator& x, const forward_iterator& y)
        {return x.it_ == y.it_;}
    friend bool operator!=(const forward_iterator& x, const forward_iterator& y)
        {return !(x == y);}
};

template<typename I, typename V2>
I upper_bound_n(I first, typename std::iterator_traits<I>::difference_type d, V2 const &val)
{
    while(0 != d)
    {
        auto half = d / 2;
        auto middle = std::next(first, half);
        if(val < *middle)
            d = half;
        else
        {
            first = ++middle;
            d -= half + 1;
        }
    }
    return first;
}

template<typename I>
void insertion_sort_n(I first, typename std::iterator_traits<I>::difference_type n)
{
    auto m = 0;
    for(auto it = first; m != n; ++it, ++m)
    {
        auto insertion = upper_bound_n(first, m, *it);
        ranges::rotate(insertion, it, std::next(it));
    }
}

template<typename I, typename S>
void insertion_sort(I first, S last)
{
    for(auto it = first; it != last; ++it)
    {
        auto insertion = ranges::upper_bound(first, it, *it);
        ranges::rotate(insertion, it, std::next(it));
    }
}

template<typename Rng>
void insertion_sort(Rng && rng)
{
    ::insertion_sort(std::begin(rng), std::end(rng));
}

std::unique_ptr<int> data(int i)
{
    std::unique_ptr<int> a(new int[i]);
    auto rng = ranges::views::counted(a.get(), i);
    ranges::iota(rng, 0);
    return a;
}

template<typename Gen>
void shuffle(int *a, int i, Gen && rand)
{
    auto rng = ranges::views::counted(a, i);
    rng |= ranges::actions::shuffle(std::forward<Gen>(rand));
}

constexpr int cloops = 3;

template<typename I>
void benchmark_n(int i)
{
    std::mt19937 gen;
    auto a = data(i);
    timer::duration_t ms = {};
    for(int j = 0; j < cloops; ++j)
    {
        ::shuffle(a.get(), i, gen);
        timer t;
        insertion_sort_n(I{a.get()}, i);
        ms += t.elapsed();
    }
    std::cout << to_millis(ms/cloops) << std::endl;
}

template<typename I>
void benchmark_counted(int i)
{
    std::mt19937 gen;
    auto a = data(i);
    timer::duration_t ms = {};
    for(int j = 0; j < cloops; ++j)
    {
        ::shuffle(a.get(), i, gen);
        timer t;
        insertion_sort(ranges::views::counted(I{a.get()}, i));
        ms += t.elapsed();
    }
    std::cout << to_millis(ms/cloops) << std::endl;
}

int main(int argc, char *argv[])
{
    if(argc < 2)
        return -1;

    int i = std::atoi(argv[1]);
    std::cout << "insertion_sort_n (random-access) : ";
    benchmark_n<int*>(i);
    std::cout << "insertion_sort   (random-access) : ";
    benchmark_counted<int*>(i);
    std::cout << "\n";
    std::cout << "insertion_sort_n (forward)       : ";
    benchmark_n<forward_iterator<int*>>(i);
    std::cout << "insertion_sort   (forward)       : ";
    benchmark_counted<forward_iterator<int*>>(i);
}
