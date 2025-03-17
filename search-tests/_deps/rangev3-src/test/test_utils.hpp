// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)

#ifndef RANGES_TEST_UTILS_HPP
#define RANGES_TEST_UTILS_HPP

#include <algorithm>
#include <cstring>
#include <functional>
#include <initializer_list>
#include <ostream>

#include <meta/meta.hpp>

#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/operations.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/concepts.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/view/subrange.hpp>

#include "./debug_view.hpp"
#include "./simple_test.hpp"
#include "./test_iterators.hpp"

#if defined(__clang__) || defined(__GNUC__)
#if defined(__has_builtin)
#if __has_builtin(__builtin_FILE) && \
    __has_builtin(__builtin_LINE) && \
    __has_builtin(__builtin_FUNCTION)
#define RANGES_CXX_HAS_SLOC_BUILTINS
#endif
#endif
#else
#define RANGES_CXX_HAS_SLOC_BUILTINS
#endif

#if defined(RANGES_CXX_HAS_SLOC_BUILTINS) && defined(__has_include)
#if __has_include(<source_location>)
#include <source_location>
#ifdef __cpp_lib_source_location
#define RANGES_HAS_SLOC 1
using source_location = std::source_location;
#endif
#elif __has_include(<experimental/source_location>)
#include <experimental/source_location>
#if __cpp_lib_experimental_source_location
#define RANGES_HAS_SLOC 1
using source_location = std::experimental::source_location;
#endif
#endif
#endif

#ifndef RANGES_HAS_SLOC
struct source_location
{
    static source_location current()
    {
        return {};
    }
};
#define CHECK_SLOC(sloc, ...) \
    do                        \
    {                         \
        (void)sloc;           \
        CHECK(__VA_ARGS__);   \
    } while(false)
#else
#define CHECK_SLOC(sloc, ...) CHECK_LINE(sloc.file_name(), (int)sloc.line(), __VA_ARGS__)
#endif

RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_DEPRECATED_THIS_CAPTURE

template<typename T, typename U>
CPP_concept both_ranges = ranges::input_range<T> && ranges::input_range<U>;

struct check_equal_fn
{
    CPP_template(typename T, typename U)(
        requires(!both_ranges<T, U>))     //
    constexpr void operator()(
        T && actual, U && expected,
        source_location sloc = source_location::current()) const
    {
        CHECK_SLOC(sloc, (T &&) actual == (U &&) expected);
    }

    CPP_template(typename Rng1, typename Rng2)(
        requires both_ranges<Rng1, Rng2>)
    constexpr void operator()(
        Rng1 && actual, Rng2 && expected,
        source_location sloc = source_location::current()) const
    {
        auto begin0 = ranges::begin(actual);
        auto end0 = ranges::end(actual);
        auto begin1 = ranges::begin(expected);
        auto end1 = ranges::end(expected);
        for(; begin0 != end0 && begin1 != end1; ++begin0, ++begin1)
            (*this)(*begin0, *begin1, sloc);
        CHECK_SLOC(sloc, begin0 == end0);
        CHECK_SLOC(sloc, begin1 == end1);
    }

    CPP_template(typename Rng, typename Val)(
        requires ranges::input_range<Rng>)
    constexpr void operator()(
        Rng && actual, std::initializer_list<Val> && expected,
        source_location sloc = source_location::current()) const
    {
        (*this)(actual, expected, sloc);
    }
};

inline namespace function_objects
{
    RANGES_INLINE_VARIABLE(check_equal_fn, check_equal)
}

template<typename Expected, typename Actual>
constexpr void has_type(Actual &&)
{
    static_assert(std::is_same<Expected, Actual>::value, "Not the same");
}

template<ranges::cardinality Expected, 
         typename Rng, 
         ranges::cardinality Actual = ranges::range_cardinality<Rng>::value>
constexpr void has_cardinality(Rng &&)
{
    static_assert(Actual == Expected, "Unexpected cardinality");
}

template<typename T>
constexpr T & as_lvalue(T && t)
{
    return t;
}

// A simple, light-weight, non-owning reference to a type-erased function.
template<typename Sig>
struct function_ref;

template<typename Ret, typename... Args>
struct function_ref<Ret(Args...)>
{
private:
    void const * data_{nullptr};
    Ret (*pfun_)(void const *, Args...){nullptr};
    template<typename Fun>
    static Ret apply_(void const * data, Args... args)
    {
        return (*static_cast<Fun const *>(data))(args...);
    }

public:
    function_ref() = default;
    template<typename T>
    function_ref(T const & t)
      : data_(&t)
      , pfun_(&apply_<T>)
    {}
    Ret operator()(Args... args) const
    {
        return (*pfun_)(data_, args...);
    }
};

template<typename T>
struct checker
{
private:
    std::function<void(function_ref<void(T)>)> algo_;

public:
    explicit checker(std::function<void(function_ref<void(T)>)> algo)
      : algo_(std::move(algo))
    {}
    void check(function_ref<void(T)> const & check) const
    {
        algo_(check);
    }
};

template<bool B, typename T>
meta::if_c<B, T, T const &> rvalue_if(T const & t)
{
    return t;
}

template<typename Algo, bool RvalueOK = false>
struct test_range_algo_1
{
private:
    Algo algo_;

    template<typename I, typename... Rest>
    static auto _impl(Algo algo, I first, I last, Rest &&... rest)
        -> ::checker<decltype(algo(first, last, rest...))>
    {
        using S = meta::_t<sentinel_type<I>>;
        using R = decltype(algo(first, last, rest...));
        auto check_algo = [algo, first, last, rest...](
                              function_ref<void(R)> const & check) {
            check(algo(first, last, rest...));
            check(algo(first, S{base(last)}, rest...));
            check(
                algo(::rvalue_if<RvalueOK>(ranges::make_subrange(first, last)), rest...));
            check(algo(::rvalue_if<RvalueOK>(ranges::make_subrange(first, S{base(last)})),
                       rest...));
        };
        return ::checker<R>{check_algo};
    }

public:
    explicit test_range_algo_1(Algo algo)
      : algo_(algo)
    {}
    template<typename I>
    auto operator()(I first, I last) const -> ::checker<decltype(algo_(first, last))>
    {
        return test_range_algo_1::_impl(algo_, first, last);
    }
    template<typename I, typename T>
    auto operator()(I first, I last, T t) const -> ::checker<decltype(algo_(first, last, t))>
    {
        return test_range_algo_1::_impl(algo_, first, last, t);
    }
    template<typename I, typename T, typename U>
    auto operator()(I first, I last, T t, U u) const
        -> ::checker<decltype(algo_(first, last, t, u))>
    {
        return test_range_algo_1::_impl(algo_, first, last, t, u);
    }
    template<typename I, typename T, typename U, typename V>
    auto operator()(I first, I last, T t, U u, V v) const
        -> ::checker<decltype(algo_(first, last, t, u, v))>
    {
        return test_range_algo_1::_impl(algo_, first, last, t, u, v);
    }
};

template<bool RvalueOK = false, typename Algo>
test_range_algo_1<Algo, RvalueOK> make_testable_1(Algo algo)
{
    return test_range_algo_1<Algo, RvalueOK>{algo};
}

template<typename Algo, bool RvalueOK1 = false, bool RvalueOK2 = false>
struct test_range_algo_2
{
private:
    Algo algo_;

public:
    explicit test_range_algo_2(Algo algo)
      : algo_(algo)
    {}
    template<typename I1, typename I2, typename... Rest>
    auto operator()(I1 begin1, I1 end1, I2 begin2, I2 end2, Rest &&... rest) const
        -> checker<decltype(algo_(begin1, end1, begin2, end2, rest...))>
    {
        using S1 = meta::_t<sentinel_type<I1>>;
        using S2 = meta::_t<sentinel_type<I2>>;
        using R = decltype(algo_(begin1, end1, begin2, end2, rest...));
        return checker<R>{[algo = algo_, begin1, end1, begin2, end2, rest...](
                              function_ref<void(R)> const & check) {
            check(algo(begin1, end1, begin2, end2, rest...));
            check(algo(begin1, S1{base(end1)}, begin2, S2{base(end2)}, rest...));
            check(algo(::rvalue_if<RvalueOK1>(ranges::make_subrange(begin1, end1)),
                       ::rvalue_if<RvalueOK2>(ranges::make_subrange(begin2, end2)),
                       rest...));
            check(algo(
                ::rvalue_if<RvalueOK1>(ranges::make_subrange(begin1, S1{base(end1)})),
                ::rvalue_if<RvalueOK2>(ranges::make_subrange(begin2, S2{base(end2)})),
                rest...));
        }};
    }
};

template<bool RvalueOK1 = false, bool RvalueOK2 = false, typename Algo>
test_range_algo_2<Algo, RvalueOK1, RvalueOK2> make_testable_2(Algo algo)
{
    return test_range_algo_2<Algo, RvalueOK1, RvalueOK2>{algo};
}

// a simple type to test move semantics
struct MoveOnlyString
{
    char const * sz_;

    MoveOnlyString(char const * sz = "")
      : sz_(sz)
    {}
    MoveOnlyString(MoveOnlyString && that)
      : sz_(that.sz_)
    {
        that.sz_ = "";
    }
    MoveOnlyString(MoveOnlyString const &) = delete;
    MoveOnlyString & operator=(MoveOnlyString && that)
    {
        sz_ = that.sz_;
        that.sz_ = "";
        return *this;
    }
    MoveOnlyString & operator=(MoveOnlyString const &) = delete;
    bool operator==(MoveOnlyString const & that) const
    {
        return 0 == std::strcmp(sz_, that.sz_);
    }
    bool operator<(const MoveOnlyString & that) const
    {
        return std::strcmp(sz_, that.sz_) < 0;
    }
    bool operator!=(MoveOnlyString const & that) const
    {
        return !(*this == that);
    }
    friend std::ostream & operator<<(std::ostream & sout, MoveOnlyString const & str)
    {
        return sout << '"' << str.sz_ << '"';
    }
};

RANGES_DIAGNOSTIC_POP

#endif
