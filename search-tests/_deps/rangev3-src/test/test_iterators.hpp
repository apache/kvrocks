//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#ifndef RANGES_TEST_ITERATORS_HPP
#define RANGES_TEST_ITERATORS_HPP

#include <iterator>
#include <range/v3/range/dangling.hpp>

template<class It, bool Sized = false>
class Sentinel;

template<class It>
class OutputIterator;

template<class It, bool Sized = false>
class InputIterator;

template<class It, bool Sized = false>
class ForwardIterator;

template<class It, bool Sized = false>
class BidirectionalIterator;

template<class It>
class RandomAccessIterator;


template<class Iter, bool Sized>
constexpr /*c++14*/ Iter base(Sentinel<Iter, Sized> i) { return i.base(); }

template<class Iter>
constexpr /*c++14*/ Iter base(OutputIterator<Iter> i) { return i.base(); }

template<class Iter, bool Sized>
constexpr /*c++14*/ Iter base(InputIterator<Iter, Sized> i) { return i.base(); }

template<class Iter, bool Sized>
constexpr /*c++14*/ Iter base(ForwardIterator<Iter, Sized> i) { return i.base(); }

template<class Iter, bool Sized>
constexpr /*c++14*/ Iter base(BidirectionalIterator<Iter, Sized> i) { return i.base(); }

template<class Iter>
constexpr /*c++14*/ Iter base(RandomAccessIterator<Iter> i) { return i.base(); }

template<class Iter>    // everything else
constexpr /*c++14*/ Iter base(Iter i) { return i; }


template<class It, bool Sized>
class Sentinel
{
    It it_;
public:
    constexpr /*c++14*/ Sentinel() : it_() {}
    constexpr /*c++14*/ explicit Sentinel(It it) : it_(it) {}
    constexpr /*c++14*/ It base() const { return it_; }
    constexpr /*c++14*/ friend bool operator==(const Sentinel& x, const Sentinel& y)
    {
        RANGES_ENSURE(x.it_ == y.it_);
        return true;
    }
    constexpr /*c++14*/ friend bool operator!=(const Sentinel& x, const Sentinel& y)
    {
        RANGES_ENSURE(x.it_ == y.it_);
        return false;
    }
    template<typename I>
    constexpr /*c++14*/ friend bool operator==(const I& x, const Sentinel& y)
    {
        using ::base;
        return base(x) == y.it_;
    }
    template<typename I>
    constexpr /*c++14*/ friend bool operator!=(const I& x, const Sentinel& y)
    {
        return !(x == y);
    }
    template<typename I>
    constexpr /*c++14*/ friend bool operator==(const Sentinel& x, const I& y)
    {
        using ::base;
        return x.it_ == base(y);
    }
    template<typename I>
    constexpr /*c++14*/ friend bool operator!=(const Sentinel& x, const I& y)
    {
        return !(x == y);
    }
};

// For making sized iterator ranges:
template<template<typename> class I, typename It>
constexpr /*c++14*/
auto CPP_auto_fun(operator-)(Sentinel<It, true> last, I<It> first)
(
    return base(last) - base(first)
)
template<template<typename> class I, typename It>
constexpr /*c++14*/
auto CPP_auto_fun(operator-)(I<It> first, Sentinel<It, true> last)
(
    return base(first) - base(last)
)

template<class It>
class OutputIterator
{
    It it_;

    template<class U> friend class OutputIterator;
public:
    typedef          std::output_iterator_tag                  iterator_category;
    typedef void                                               value_type;
    typedef typename std::iterator_traits<It>::difference_type difference_type;
    typedef It                                                 pointer;
    typedef typename std::iterator_traits<It>::reference       reference;

    constexpr /*c++14*/ It base() const {return it_;}

    constexpr /*c++14*/ OutputIterator () {}
    constexpr /*c++14*/ explicit OutputIterator(It it) : it_(it) {}
    template<class U, class = typename std::enable_if<std::is_convertible<U, It>{}>::type>
    constexpr /*c++14*/
    OutputIterator(const OutputIterator<U>& u) :it_(u.it_) {}

    constexpr /*c++14*/ reference operator*() const {return *it_;}

    constexpr /*c++14*/ OutputIterator& operator++() {++it_; return *this;}
    constexpr /*c++14*/ OutputIterator operator++(int)
    {OutputIterator tmp(*this); ++(*this); return tmp;}
};

template<class It, bool Sized>
class InputIterator
{
    It it_;

    template<class, bool> friend class InputIterator;
public:
    typedef          std::input_iterator_tag                   iterator_category;
    typedef typename std::iterator_traits<It>::value_type      value_type;
    typedef typename std::iterator_traits<It>::difference_type difference_type;
    typedef It                                                 pointer;
    typedef typename std::iterator_traits<It>::reference       reference;

    constexpr /*c++14*/ It base() const {return it_;}

    constexpr /*c++14*/ InputIterator() : it_() {}
    constexpr /*c++14*/ explicit InputIterator(It it) : it_(it) {}
    template<class U, bool USized>
    constexpr /*c++14*/ CPP_ctor(InputIterator)(const InputIterator<U, USized>& u)(
        requires (std::is_convertible<U, It>::value)) :it_(u.it_) {}

    constexpr /*c++14*/ reference operator*() const {return *it_;}
    constexpr /*c++14*/ pointer operator->() const {return it_;}

    constexpr /*c++14*/ InputIterator& operator++() {++it_; return *this;}
    constexpr /*c++14*/ InputIterator operator++(int)
        {InputIterator tmp(*this); ++(*this); return tmp;}

    constexpr /*c++14*/
    friend bool operator==(const InputIterator& x, const InputIterator& y)
        {return x.it_ == y.it_;}
    constexpr /*c++14*/
    friend bool operator!=(const InputIterator& x, const InputIterator& y)
        {return !(x == y);}

    template<bool B = Sized, meta::if_c<B, int> = 42>
    constexpr /*c++14*/
    friend difference_type operator-(const InputIterator& x, const InputIterator& y)
        {return x.it_ - y.it_;}
};

template<class T, bool TSized, class U, bool USized>
constexpr /*c++14*/
bool
operator==(const InputIterator<T, TSized>& x, const InputIterator<U, USized>& y)
{
    return x.base() == y.base();
}

template<class T, bool TSized, class U, bool USized>
constexpr /*c++14*/
bool
operator!=(const InputIterator<T, TSized>& x, const InputIterator<U, USized>& y)
{
    return !(x == y);
}

template<class It, bool Sized>
class ForwardIterator
{
    It it_;

    template<class, bool> friend class ForwardIterator;
public:
    typedef          std::forward_iterator_tag                 iterator_category;
    typedef typename std::iterator_traits<It>::value_type      value_type;
    typedef typename std::iterator_traits<It>::difference_type difference_type;
    typedef It                                                 pointer;
    typedef typename std::iterator_traits<It>::reference       reference;

    constexpr /*c++14*/ It base() const {return it_;}

    constexpr /*c++14*/ ForwardIterator() : it_() {}
    constexpr /*c++14*/ explicit ForwardIterator(It it) : it_(it) {}
    template<class U, bool USized>
    constexpr /*c++14*/ CPP_ctor(ForwardIterator)(const ForwardIterator<U, USized>& u)(
        requires (std::is_convertible<U, It>::value)) :it_(u.it_) {}

    constexpr /*c++14*/ reference operator*() const {return *it_;}
    constexpr /*c++14*/ pointer operator->() const {return it_;}

    constexpr /*c++14*/ ForwardIterator& operator++() {++it_; return *this;}
    constexpr /*c++14*/ ForwardIterator operator++(int)
    {ForwardIterator tmp(*this); ++(*this); return tmp;}

    constexpr /*c++14*/
    friend bool operator==(const ForwardIterator& x, const ForwardIterator& y)
    {return x.it_ == y.it_;}
    constexpr /*c++14*/
    friend bool operator!=(const ForwardIterator& x, const ForwardIterator& y)
    {return !(x == y);}
};

template<class T, bool TSized, class U, bool USized>
constexpr /*c++14*/
bool
operator==(const ForwardIterator<T, TSized>& x, const ForwardIterator<U, USized>& y)
{
    return x.base() == y.base();
}

template<class T, bool TSized, class U, bool USized>
constexpr /*c++14*/
bool
operator!=(const ForwardIterator<T, TSized>& x, const ForwardIterator<U, USized>& y)
{
    return !(x == y);
}

template<class It, bool Sized>
class BidirectionalIterator
{
    It it_;

    template<class, bool> friend class BidirectionalIterator;
public:
    typedef          std::bidirectional_iterator_tag           iterator_category;
    typedef typename std::iterator_traits<It>::value_type      value_type;
    typedef typename std::iterator_traits<It>::difference_type difference_type;
    typedef It                                                 pointer;
    typedef typename std::iterator_traits<It>::reference       reference;

    constexpr /*c++14*/ It base() const {return it_;}

    constexpr /*c++14*/ BidirectionalIterator() : it_() {}
    constexpr /*c++14*/ explicit BidirectionalIterator(It it) : it_(it) {}
    template<class U, bool USized>
    constexpr /*c++14*/ CPP_ctor(BidirectionalIterator)(const BidirectionalIterator<U, USized>& u)(
        requires (std::is_convertible<U, It>::value)) :it_(u.it_) {}

    constexpr /*c++14*/ reference operator*() const {return *it_;}
    constexpr /*c++14*/ pointer operator->() const {return it_;}

    constexpr /*c++14*/ BidirectionalIterator& operator++() {++it_; return *this;}
    constexpr /*c++14*/ BidirectionalIterator operator++(int)
    {BidirectionalIterator tmp(*this); ++(*this); return tmp;}

    constexpr /*c++14*/ BidirectionalIterator& operator--() {--it_; return *this;}
    constexpr /*c++14*/ BidirectionalIterator operator--(int)
    {BidirectionalIterator tmp(*this); --(*this); return tmp;}
};

template<class T, bool TSized, class U, bool USized>
constexpr /*c++14*/
bool
operator==(const BidirectionalIterator<T, TSized>& x, const BidirectionalIterator<U, USized>& y)
{
    return x.base() == y.base();
}

template<class T, bool TSized, class U, bool USized>
constexpr /*c++14*/
bool
operator!=(const BidirectionalIterator<T, TSized>& x, const BidirectionalIterator<U, USized>& y)
{
    return !(x == y);
}

template<class It>
class RandomAccessIterator
{
    It it_;

    template<class U> friend class RandomAccessIterator;
public:
    typedef          std::random_access_iterator_tag           iterator_category;
    typedef typename std::iterator_traits<It>::value_type      value_type;
    typedef typename std::iterator_traits<It>::difference_type difference_type;
    typedef It                                                 pointer;
    typedef typename std::iterator_traits<It>::reference       reference;

    constexpr /*c++14*/ It base() const {return it_;}

    constexpr /*c++14*/ RandomAccessIterator() : it_() {}
    constexpr /*c++14*/ explicit RandomAccessIterator(It it) : it_(it) {}
    template<class U>
    constexpr /*c++14*/ CPP_ctor(RandomAccessIterator)(const RandomAccessIterator<U>& u)(
        requires (std::is_convertible<U, It>::value)) :it_(u.it_) {}

    constexpr /*c++14*/ reference operator*() const {return *it_;}
    constexpr /*c++14*/ pointer operator->() const {return it_;}

    constexpr /*c++14*/ RandomAccessIterator& operator++() {++it_; return *this;}
    constexpr /*c++14*/ RandomAccessIterator operator++(int)
    {RandomAccessIterator tmp(*this); ++(*this); return tmp;}

    constexpr /*c++14*/ RandomAccessIterator& operator--() {--it_; return *this;}
    constexpr /*c++14*/ RandomAccessIterator operator--(int)
    {RandomAccessIterator tmp(*this); --(*this); return tmp;}

    constexpr /*c++14*/
    RandomAccessIterator& operator+=(difference_type n) {it_ += n; return *this;}
    constexpr /*c++14*/
    RandomAccessIterator operator+(difference_type n) const
    {RandomAccessIterator tmp(*this); tmp += n; return tmp;}
    constexpr /*c++14*/
    friend RandomAccessIterator operator+(difference_type n, RandomAccessIterator x)
    {x += n; return x;}
    constexpr /*c++14*/
    RandomAccessIterator& operator-=(difference_type n) {return *this += -n;}
    constexpr /*c++14*/
    RandomAccessIterator operator-(difference_type n) const
    {RandomAccessIterator tmp(*this); tmp -= n; return tmp;}

    constexpr /*c++14*/
    reference operator[](difference_type n) const {return it_[n];}
};

template<class T, class U>
constexpr /*c++14*/
bool
operator==(const RandomAccessIterator<T>& x, const RandomAccessIterator<U>& y)
{
    return x.base() == y.base();
}

template<class T, class U>
constexpr /*c++14*/
bool
operator!=(const RandomAccessIterator<T>& x, const RandomAccessIterator<U>& y)
{
    return !(x == y);
}

template<class T, class U>
constexpr /*c++14*/
bool
operator<(const RandomAccessIterator<T>& x, const RandomAccessIterator<U>& y)
{
    return x.base() < y.base();
}

template<class T, class U>
constexpr /*c++14*/
bool
operator<=(const RandomAccessIterator<T>& x, const RandomAccessIterator<U>& y)
{
    return !(y < x);
}

template<class T, class U>
constexpr /*c++14*/
bool
operator>(const RandomAccessIterator<T>& x, const RandomAccessIterator<U>& y)
{
    return y < x;
}

template<class T, class U>
constexpr /*c++14*/
bool
operator>=(const RandomAccessIterator<T>& x, const RandomAccessIterator<U>& y)
{
    return !(x < y);
}

template<class T, class U>
constexpr /*c++14*/
auto CPP_auto_fun(operator-)(const RandomAccessIterator<T>& x, const RandomAccessIterator<U>& y)
(
    return x.base() - y.base()
)

template<typename It, bool Sized = false>
struct sentinel_type
{
    using type = It;
};

template<typename T, bool Sized>
struct sentinel_type<T*, Sized>
{
    using type = Sentinel<T*, Sized>;
};

template<template<typename> class I, typename It, bool Sized>
struct sentinel_type<I<It>, Sized>
{
    using type = Sentinel<It, Sized>;
};

template<class I, class S>
struct TestRange
{
    I first;
    S second;
    constexpr I begin() const { return first; }
    constexpr S end() const { return second; }
};

template<class I, class S>
TestRange<I, S> MakeTestRange(I i, S s)
{
    return {i, s};
}

template<typename T>
constexpr bool is_dangling(T)
{
    return false;
}
constexpr bool is_dangling(::ranges::dangling)
{
    return true;
}

#endif  // RANGES_TEST_ITERATORS_HPP
