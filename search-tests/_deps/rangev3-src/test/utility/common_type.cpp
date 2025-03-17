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

#include <utility>
#include <type_traits>
#include <range/v3/utility/common_type.hpp>
#include <range/v3/utility/common_tuple.hpp>

struct B {};
struct D : B {};

struct noncopyable
{
    noncopyable() = default;
    noncopyable(noncopyable const &) = delete;
    noncopyable(noncopyable &&) = default;
    noncopyable &operator=(noncopyable const &) = delete;
    noncopyable &operator=(noncopyable &&) = default;
};

struct noncopyable2 : noncopyable
{};

struct X {};
struct Y {};
struct Z {};
namespace concepts
{
    template<>
    struct common_type<X, Y>
    {
        using type = Z;
    };
    template<>
    struct common_type<Y, X>
    {
        using type = Z;
    };
}

template<typename T>
struct ConvTo
{
    operator T();
};

// Whoops, fails:
static_assert(std::is_same<
    ranges::common_type_t<ConvTo<int>, int>,
    int
>::value, "");

int main()
{
    using namespace ranges;
    using namespace detail;
    static_assert(std::is_same<common_reference_t<B &, D &>, B &>::value, "");
    static_assert(std::is_same<common_reference_t<B &, D const &>, B const &>::value, "");
    static_assert(std::is_same<common_reference_t<B &, D const &, D &>, B const &>::value, "");
    static_assert(std::is_same<common_reference_t<B const &, D &>, B const &>::value, "");
    static_assert(std::is_same<common_reference_t<B &, D &, B &, D &>, B &>::value, "");

    static_assert(std::is_same<common_reference_t<B &&, D &&>, B &&>::value, "");
    static_assert(std::is_same<common_reference_t<B const &&, D &&>, B const &&>::value, "");
    static_assert(std::is_same<common_reference_t<B &&, D const &&>, B const &&>::value, "");

    static_assert(std::is_same<common_reference_t<B &, D &&>, B const &>::value, "");
    static_assert(std::is_same<common_reference_t<B &, D const &&>, B const &>::value, "");
    static_assert(std::is_same<common_reference_t<B const &, D &&>, B const &>::value, "");

    static_assert(std::is_same<common_reference_t<B &&, D &>, B const &>::value, "");
    static_assert(std::is_same<common_reference_t<B &&, D const &>, B const &>::value, "");
    static_assert(std::is_same<common_reference_t<B const &&, D &>, B const &>::value, "");

    static_assert(std::is_same<common_reference_t<int, short>, int>::value, "");
    static_assert(std::is_same<common_reference_t<int, short &>, int>::value, "");
    static_assert(std::is_same<common_reference_t<int &, short &>, int>::value, "");
    static_assert(std::is_same<common_reference_t<int &, short>, int>::value, "");

    // tricky volatile reference case
    static_assert(std::is_same<common_reference_t<int &&, int volatile &>, int>::value, "");
    static_assert(std::is_same<common_reference_t<int volatile &, int &&>, int>::value, "");
    static_assert(std::is_same<common_reference_t<int const volatile &&, int volatile &&>, int const volatile &&>::value, "");
    static_assert(std::is_same<common_reference_t<int &&, int const &, int volatile &>, int const volatile &>(), "");

    // Array types?? Yup!
    static_assert(std::is_same<common_reference_t<int (&)[10], int (&&)[10]>, int const(&)[10]>::value, "");
    static_assert(std::is_same<common_reference_t<int const (&)[10], int volatile (&)[10]>, int const volatile(&)[10]>::value, "");
    static_assert(std::is_same<common_reference_t<int (&)[10], int (&)[11]>, int *>::value, "");

    // Some tests for common_pair with common_reference
    static_assert(std::is_same<
        common_reference_t<std::pair<int &, int &>, common_pair<int,int> const &>,
        common_pair<int const &, int const &>
    >::value, "");

    // BUGBUG TODO Is a workaround possible?
#if !defined(__GNUC__) || __GNUC__ != 4 || __GNUC_MINOR__ > 8
    static_assert(std::is_same<
        common_reference_t<common_pair<int const &, int const &>, std::pair<int, int>>,
        common_pair<int, int>
    >::value, "");

    static_assert(std::is_same<
        ::concepts::detail::_builtin_common_t<common_pair<int, int> const &, std::pair<int, int> &>,
        std::pair<int, int> const &
    >::value, "");
#endif

    static_assert(std::is_same<
        common_reference_t<common_pair<int, int> const &, std::pair<int, int> &>,
        std::pair<int, int> const &
    >::value, "");

    // Some tests with noncopyable types
    static_assert(std::is_same<
        ::concepts::detail::_builtin_common_t<noncopyable const &, noncopyable>,
        noncopyable
    >::value, "");

    static_assert(std::is_same<
        ::concepts::detail::_builtin_common_t<noncopyable2 const &, noncopyable>,
        noncopyable
    >::value, "");

    static_assert(std::is_same<
        ::concepts::detail::_builtin_common_t<noncopyable const &, noncopyable2>,
        noncopyable
    >::value, "");

    static_assert(std::is_same<
        common_reference_t<X &, Y const &>,
        Z
    >::value, "");

    {
        // Regression test for #367
        using CP = common_pair<int, int>;
        CPP_assert(same_as<common_type_t<CP, CP>, CP>);
    }
}
