///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2015 Microsoft Corporation. All rights reserved.
//
// This code is licensed under the MIT License (MIT).
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
///////////////////////////////////////////////////////////////////////////////

#include <range/v3/view/span.hpp>

#include <array>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <regex>
#include <string>
#include <vector>
#include "../simple_test.hpp"

using ranges::span;
using ranges::dynamic_extent;
using ranges::make_span;
using ranges::as_bytes;
using ranges::as_writeable_bytes;
using ranges::detail::narrow_cast;

RANGES_DIAGNOSTIC_IGNORE_MISSING_BRACES

namespace {
    struct BaseClass {};
    struct DerivedClass : BaseClass {};
}

void test_case_default_constructor()
{
    {
        span<int> s;
        CHECK((s.size() == 0 && s.data() == nullptr));

        span<const int> cs;
        CHECK((cs.size() == 0 && cs.data() == nullptr));
    }

    {
        span<int, 0> s;
        CHECK((s.size() == 0 && s.data() == nullptr));

        span<const int, 0> cs;
        CHECK((cs.size() == 0 && cs.data() == nullptr));
    }

    {
        span<int, 1> s;
        CHECK((s.size() == 1 && s.data() == nullptr));
    }

    {
        span<int> s{};
        CHECK((s.size() == 0 && s.data() == nullptr));

        span<const int> cs{};
        CHECK((cs.size() == 0 && cs.data() == nullptr));
    }
}

void test_case_size_optimization()
{
    {
        span<int> s;
        CHECK(sizeof(s) == sizeof(int*) + sizeof(std::ptrdiff_t));
    }

    {
        span<int, 0> s;
        CHECK(sizeof(s) == sizeof(int*));
    }
}

void test_case_from_nullptr_constructor()
{
    // This implementation doesn't support the silly nullptr_t constructor.
    CPP_assert(!std::is_constructible<span<int>, std::nullptr_t>::value);
    CPP_assert(!std::is_constructible<span<const int>, std::nullptr_t>::value);

    CPP_assert(!std::is_constructible<span<int, 0>, std::nullptr_t>::value);
    CPP_assert(!std::is_constructible<span<const int, 0>, std::nullptr_t>::value);

    CPP_assert(!std::is_constructible<span<int, 1>, std::nullptr_t>::value);
    CPP_assert(!std::is_constructible<span<const int, 1>, std::nullptr_t>::value);
}

void test_case_from_nullptr_size_constructor()
{
    {
        span<int> s{nullptr, static_cast<span<int>::index_type>(0)};
        CHECK((s.size() == 0 && s.data() == nullptr));

        span<const int> cs{nullptr, static_cast<span<int>::index_type>(0)};
        CHECK((cs.size() == 0 && cs.data() == nullptr));
    }

    {
        span<int, 0> s{nullptr, static_cast<span<int>::index_type>(0)};
        CHECK((s.size() == 0 && s.data() == nullptr));

        span<const int, 0> cs{nullptr, static_cast<span<int>::index_type>(0)};
        CHECK((cs.size() == 0 && cs.data() == nullptr));
    }

    {
        span<int*> s{nullptr, static_cast<span<int>::index_type>(0)};
        CHECK((s.size() == 0 && s.data() == nullptr));

        span<const int*> cs{nullptr, static_cast<span<int>::index_type>(0)};
        CHECK((cs.size() == 0 && cs.data() == nullptr));
    }
}

void test_case_from_pointer_size_constructor()
{
    int arr[4] = {1, 2, 3, 4};

    {
        span<int> s{&arr[0], 2};
        CHECK((s.size() == 2 && s.data() == &arr[0]));
        CHECK((s[0] == 1 && s[1] == 2));
    }

    {
        span<int, 2> s{&arr[0], 2};
        CHECK((s.size() == 2 && s.data() == &arr[0]));
        CHECK((s[0] == 1 && s[1] == 2));
    }

    {
        int* p = nullptr;
        span<int> s{p, static_cast<span<int>::index_type>(0)};
        CHECK((s.size() == 0 && s.data() == nullptr));
    }

    {
        auto s = make_span(&arr[0], 2);
        CHECK((s.size() == 2 && s.data() == &arr[0]));
        CHECK((s[0] == 1 && s[1] == 2));
    }

    {
        int* p = nullptr;
        auto s = make_span(p, static_cast<span<int>::index_type>(0));
        CHECK((s.size() == 0 && s.data() == nullptr));
    }

    {
        int i = 42;
        span<int> s{&i, 0};
        CHECK((s.size() == 0 && s.data() == &i));

        span<const int> cs{&i, 0};
        CHECK((s.size() == 0 && s.data() == &i));
    }
}

void test_case_from_pointer_pointer_constructor()
{
    int arr[4] = {1, 2, 3, 4};

    {
        span<int> s{&arr[0], &arr[2]};
        CHECK((s.size() == 2 && s.data() == &arr[0]));
        CHECK((s[0] == 1 && s[1] == 2));
    }

    {
        span<int, 2> s{&arr[0], &arr[2]};
        CHECK((s.size() == 2 && s.data() == &arr[0]));
        CHECK((s[0] == 1 && s[1] == 2));
    }

    {
        span<int> s{&arr[0], &arr[0]};
        CHECK((s.size() == 0 && s.data() == &arr[0]));
    }

    {
        span<int, 0> s{&arr[0], &arr[0]};
        CHECK((s.size() == 0 && s.data() == &arr[0]));
    }

    {
        int* p = nullptr;
        span<int> s{p, p};
        CHECK((s.size() == 0 && s.data() == nullptr));
    }

    {
        int* p = nullptr;
        span<int, 0> s{p, p};
        CHECK((s.size() == 0 && s.data() == nullptr));
    }

    {
        auto s = make_span(&arr[0], &arr[2]);
        CHECK((s.size() == 2 && s.data() == &arr[0]));
        CHECK((s[0] == 1 && s[1] == 2));
    }

    {
        auto s = make_span(&arr[0], &arr[0]);
        CHECK((s.size() == 0 && s.data() == &arr[0]));
    }

    {
        int* p = nullptr;
        auto s = make_span(p, p);
        CHECK((s.size() == 0 && s.data() == nullptr));
    }
}

void test_case_from_array_constructor()
{
    int arr[5] = {1, 2, 3, 4, 5};

    {
        span<int> s{arr};
        CHECK((s.size() == 5 && s.data() == &arr[0]));
    }

    {
        span<int, 5> s{arr};
        CHECK((s.size() == 5 && s.data() == &arr[0]));
    }

    int arr2d[2][3] = {1, 2, 3, 4, 5, 6};

    CPP_assert(!std::is_constructible<span<int, 6>, int(&)[5]>::value);
    CPP_assert(!std::is_constructible<span<int, 0>, int(&)[5]>::value);
    CPP_assert(!std::is_constructible<span<int>, decltype((arr2d))>::value);
    CPP_assert(!std::is_constructible<span<int, 0>, decltype((arr2d))>::value);
    CPP_assert(!std::is_constructible<span<int, 6>, decltype((arr2d))>::value);

    {
        span<int[3]> s{&(arr2d[0]), 1};
        CHECK((s.size() == 1 && s.data() == &arr2d[0]));
    }

    int arr3d[2][3][2] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

    CPP_assert(!std::is_constructible<span<int>, decltype((arr3d))>::value);
    CPP_assert(!std::is_constructible<span<int, 0>, decltype((arr3d))>::value);
    CPP_assert(!std::is_constructible<span<int, 11>, decltype((arr3d))>::value);
    CPP_assert(!std::is_constructible<span<int, 12>, decltype((arr3d))>::value);

    {
        span<int[3][2]> s{&arr3d[0], 1};
        CHECK((s.size() == 1 && s.data() == &arr3d[0]));
    }

    {
        auto s = make_span(arr);
        CHECK((s.size() == 5 && s.data() == &arr[0]));
    }

    {
        auto s = make_span(&(arr2d[0]), 1);
        CHECK((s.size() == 1 && s.data() == &arr2d[0]));
    }

    {
        auto s = make_span(&arr3d[0], 1);
        CHECK((s.size() == 1 && s.data() == &arr3d[0]));
    }
}

void test_case_from_std_array_constructor()
{
    std::array<int, 4> arr = {1, 2, 3, 4};

    {
        span<int> s{arr};
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && s.data() == arr.data()));

        span<const int> cs{arr};
        CHECK((cs.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && cs.data() == arr.data()));
    }

    {
        span<int, 4> s{arr};
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && s.data() == arr.data()));

        span<const int, 4> cs{arr};
        CHECK((cs.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && cs.data() == arr.data()));
    }

    CPP_assert(!std::is_constructible<span<int, 2>, decltype((arr))>::value);
    CPP_assert(!std::is_constructible<span<const int, 2>, decltype((arr))>::value);
    CPP_assert(!std::is_constructible<span<int, 0>, decltype((arr))>::value);
    CPP_assert(!std::is_constructible<span<const int, 0>, decltype((arr))>::value);
    CPP_assert(!std::is_constructible<span<int, 5>, decltype((arr))>::value);

    {
        auto get_an_array = []() -> std::array<int, 4> { return {1, 2, 3, 4}; };
        auto take_a_span = [](span<int>) {};
        take_a_span(get_an_array());
    }

    {
        auto get_an_array = []() -> std::array<int, 4> { return {1, 2, 3, 4}; };
        auto take_a_span = [](span<const int>) {};
        take_a_span(get_an_array());
    }

    {
        auto s = make_span(arr);
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && s.data() == arr.data()));
    }
}

void test_case_from_const_std_array_constructor()
{
    const std::array<int, 4> arr = {1, 2, 3, 4};

    {
        span<const int> s{arr};
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && s.data() == arr.data()));
    }

    {
        span<const int, 4> s{arr};
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && s.data() == arr.data()));
    }

    CPP_assert(!std::is_constructible<span<const int, 2>, decltype((arr))>::value);
    CPP_assert(!std::is_constructible<span<const int, 0>, decltype((arr))>::value);
    CPP_assert(!std::is_constructible<span<const int, 5>, decltype((arr))>::value);

    {
        auto get_an_array = []() -> const std::array<int, 4> { return {1, 2, 3, 4}; };
        auto take_a_span = [](span<const int>) {};
        take_a_span(get_an_array());
    }

    {
        auto s = make_span(arr);
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && s.data() == arr.data()));
    }
}

void test_case_from_std_array_const_constructor()
{
    std::array<const int, 4> arr = {1, 2, 3, 4};

    {
        span<const int> s{arr};
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && s.data() == arr.data()));
    }

    {
        span<const int, 4> s{arr};
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && s.data() == arr.data()));
    }

    CPP_assert(!std::is_constructible<span<const int, 2>, decltype((arr))>::value);
    CPP_assert(!std::is_constructible<span<const int, 0>, decltype((arr))>::value);
    CPP_assert(!std::is_constructible<span<const int, 5>, decltype((arr))>::value);
    CPP_assert(!std::is_constructible<span<int, 4>, decltype((arr))>::value);

    {
        auto s = make_span(arr);
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(arr.size()) && s.data() == arr.data()));
    }
}

void test_case_from_container_constructor()
{
    std::vector<int> v = {1, 2, 3};
    const std::vector<int> cv = v;

    {
        span<int> s{v};
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(v.size()) && s.data() == v.data()));

        span<const int> cs{v};
        CHECK((cs.size() == narrow_cast<std::ptrdiff_t>(v.size()) && cs.data() == v.data()));
    }

    std::string str = "hello";
    const std::string cstr = "hello";

    {
        span<char> s{str};
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(str.size()) && s.data() == str.data()));
    }

    {
        auto get_temp_string = []() -> std::string { return {}; };
        auto use_span = [](span<char>) {};
        use_span(get_temp_string());
    }

    {
        span<const char> cs{str};
        CHECK((cs.size() == narrow_cast<std::ptrdiff_t>(str.size()) && cs.data() == str.data()));
    }

    {
        auto get_temp_string = []() -> std::string { return {}; };
        auto use_span = [](span<const char>) {};
        use_span(get_temp_string());
    }

    {
        CPP_assert(!std::is_constructible<span<char>, decltype((cstr))>::value);
        span<const char> cs{cstr};
        CHECK((cs.size() == narrow_cast<std::ptrdiff_t>(cstr.size()) &&
              cs.data() == cstr.data()));
    }

    {
        auto get_temp_vector = []() -> std::vector<int> { return {}; };
        auto use_span = [](span<int>) {};
        use_span(get_temp_vector());
    }

    {
        auto get_temp_vector = []() -> std::vector<int> { return {}; };
        auto use_span = [](span<const int>) {};
        use_span(get_temp_vector());
    }

    CPP_assert(!ranges::detail::is_convertible<const std::vector<int>, span<const char>>::value);

    {
        auto get_temp_string = []() -> const std::string { return {}; };
        auto use_span = [](span<const char> s) { static_cast<void>(s); };
        use_span(get_temp_string());
    }

    CPP_assert(!std::is_constructible<span<int>, std::map<int, int>&>::value);

    {
        auto s = make_span(v);
        CHECK((s.size() == narrow_cast<std::ptrdiff_t>(v.size()) && s.data() == v.data()));

        auto cs = make_span(cv);
        CHECK((cs.size() == narrow_cast<std::ptrdiff_t>(cv.size()) && cs.data() == cv.data()));
    }
}

void test_case_from_convertible_span_constructor()
{
    {
        span<DerivedClass> avd;
        span<const DerivedClass> avcd = avd;
        static_cast<void>(avcd);
    }

    CPP_assert(!std::is_constructible<span<BaseClass>, span<DerivedClass>>::value);
    CPP_assert(!std::is_constructible<span<DerivedClass>, span<BaseClass>>::value);
    CPP_assert(!std::is_constructible<span<unsigned int>, span<int>>::value);
    CPP_assert(!std::is_constructible<span<const unsigned int>, span<int>>::value);
    CPP_assert(!std::is_constructible<span<short>, span<int>>::value);
}

void test_case_copy_move_and_assignment()
{
    span<int> s1;
    CHECK(s1.empty());

    int arr[] = {3, 4, 5};

    span<const int> s2 = arr;
    CHECK((s2.size() == 3 && s2.data() == &arr[0]));

    s2 = s1;
    CHECK(s2.empty());

    auto get_temp_span = [&]() -> span<int> { return {&arr[1], 2}; };
    auto use_span = [&](span<const int> s) { CHECK((s.size() == 2 && s.data() == &arr[1])); };
    use_span(get_temp_span());

    s1 = get_temp_span();
    CHECK((s1.size() == 2 && s1.data() == &arr[1]));
}

void test_case_class_template_argument_deduction()
{
#if RANGES_CXX_DEDUCTION_GUIDES >= RANGES_CXX_DEDUCTION_GUIDES_17
#if defined(__clang__) && __clang_major__ < 6
// Workaround https://bugs.llvm.org/show_bug.cgi?id=33314
RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_UNDEFINED_FUNC_TEMPLATE
#endif
    {
        int arr[] = {1, 2, 3, 4, 5};
        {
            span s{arr};
            CPP_assert(std::is_same<span<int, 5>, decltype(s)>::value);
        }
        {
            span s{ranges::data(arr), ranges::size(arr)};
            CPP_assert(std::is_same<span<int>, decltype(s)>::value);
        }
        {
            span s{ranges::begin(arr), ranges::end(arr)};
            CPP_assert(std::is_same<span<int>, decltype(s)>::value);
        }
    }
    {
        std::array<int, 5> arr = {1, 2, 3, 4, 5};
        {
            span s{arr};
            CPP_assert(std::is_same<span<int, 5>, decltype(s)>::value);
        }
    }
    {
        std::vector<int> vec = {1, 2, 3, 4, 5};
        {
            span s{vec};
            CPP_assert(std::is_same<span<int>, decltype(s)>::value);
        }
    }
#if defined(__clang__) && __clang_major__ < 6
RANGES_DIAGNOSTIC_POP
#endif // clang bug workaround
#endif // use deduction guides
}

void test_case_first()
{
    int arr[5] = {1, 2, 3, 4, 5};

    {
        span<int, 5> av = arr;
        CHECK(av.first<2>().size() == 2);
        CHECK(av.first(2).size() == 2);
    }

    {
        span<int, 5> av = arr;
        CHECK(av.first<0>().size() == 0);
        CHECK(av.first(0).size() == 0);
    }

    {
        span<int, 5> av = arr;
        CHECK(av.first<5>().size() == 5);
        CHECK(av.first(5).size() == 5);
    }

    {
        span<int> av;
        CHECK(av.first<0>().size() == 0);
        CHECK(av.first(0).size() == 0);
    }
}

void test_case_last()
{
    int arr[5] = {1, 2, 3, 4, 5};

    {
        span<int, 5> av = arr;
        CHECK(av.last<2>().size() == 2);
        CHECK(av.last(2).size() == 2);
    }

    {
        span<int, 5> av = arr;
        CHECK(av.last<0>().size() == 0);
        CHECK(av.last(0).size() == 0);
    }

    {
        span<int, 5> av = arr;
        CHECK(av.last<5>().size() == 5);
        CHECK(av.last(5).size() == 5);
    }

    {
        span<int> av;
        CHECK(av.last<0>().size() == 0);
        CHECK(av.last(0).size() == 0);
    }
}

void test_case_subspan()
{
    int arr[5] = {1, 2, 3, 4, 5};

    {
        span<int, 5> av = arr;
        CHECK((av.subspan<2, 2>().size() == 2));
        CHECK(av.subspan(2, 2).size() == 2);
        CHECK(av.subspan(2, 3).size() == 3);
    }

    {
        span<int, 5> av = arr;
        CHECK((av.subspan<0, 0>().size() == 0));
        CHECK(av.subspan(0, 0).size() == 0);
    }

    {
        span<int, 5> av = arr;
        CHECK((av.subspan<0, 5>().size() == 5));
        CHECK(av.subspan(0, 5).size() == 5);
    }

    {
        span<int, 5> av = arr;
        CHECK((av.subspan<4, 0>().size() == 0));
        CHECK(av.subspan(4, 0).size() == 0);
        CHECK(av.subspan(5, 0).size() == 0);
    }

    {
        span<int> av;
        CHECK((av.subspan<0, 0>().size() == 0));
        CHECK(av.subspan(0, 0).size() == 0);
    }

    {
        span<int> av;
        CHECK(av.subspan(0).size() == 0);
    }

    {
        span<int> av = arr;
        CHECK(av.subspan(0).size() == 5);
        CHECK(av.subspan(1).size() == 4);
        CHECK(av.subspan(4).size() == 1);
        CHECK(av.subspan(5).size() == 0);
        const auto av2 = av.subspan(1);
        for (int i = 0; i < 4; ++i) CHECK(av2[i] == i + 2);
    }

    {
        span<int, 5> av = arr;
        CHECK(av.subspan(0).size() == 5);
        CHECK(av.subspan(1).size() == 4);
        CHECK(av.subspan(4).size() == 1);
        CHECK(av.subspan(5).size() == 0);
        const auto av2 = av.subspan(1);
        for (int i = 0; i < 4; ++i) CHECK(av2[i] == i + 2);
    }

    {
        span<int, 5> av = arr;
        CHECK(decltype(av.subspan<0, dynamic_extent>())::extent == 5);
        CHECK((av.subspan<0, dynamic_extent>().size() == 5));
        CHECK(decltype(av.subspan<1, dynamic_extent>())::extent == 4);
        CHECK((av.subspan<1, dynamic_extent>().size() == 4));
        CHECK(decltype(av.subspan<2, dynamic_extent>())::extent == 3);
        CHECK((av.subspan<2, dynamic_extent>().size() == 3));
        CHECK(decltype(av.subspan<3, dynamic_extent>())::extent == 2);
        CHECK((av.subspan<3, dynamic_extent>().size() == 2));
        CHECK(decltype(av.subspan<4, dynamic_extent>())::extent == 1);
        CHECK((av.subspan<4, dynamic_extent>().size() == 1));
        CHECK(decltype(av.subspan<5, dynamic_extent>())::extent == 0);
        CHECK((av.subspan<5, dynamic_extent>().size() == 0));
    }

    {
        span<int> av = arr;
        CHECK(decltype(av.subspan<0, dynamic_extent>())::extent == dynamic_extent);
        CHECK((av.subspan<0, dynamic_extent>().size() == 5));
        CHECK(decltype(av.subspan<1, dynamic_extent>())::extent == dynamic_extent);
        CHECK((av.subspan<1, dynamic_extent>().size() == 4));
        CHECK(decltype(av.subspan<2, dynamic_extent>())::extent == dynamic_extent);
        CHECK((av.subspan<2, dynamic_extent>().size() == 3));
        CHECK(decltype(av.subspan<3, dynamic_extent>())::extent == dynamic_extent);
        CHECK((av.subspan<3, dynamic_extent>().size() == 2));
        CHECK(decltype(av.subspan<4, dynamic_extent>())::extent == dynamic_extent);
        CHECK((av.subspan<4, dynamic_extent>().size() == 1));
        CHECK(decltype(av.subspan<5, dynamic_extent>())::extent == dynamic_extent);
        CHECK((av.subspan<5, dynamic_extent>().size() == 0));
    }
}

void test_case_iterator_value_init()
{
    span<int>::iterator it1{};
    span<int>::iterator it2{};
    CHECK(it1 == it2);
}
void test_case_iterator_comparisons()
{
    int a[] = {1, 2, 3, 4};
    {
        span<int> s = a;
        auto it = s.begin();
        CPP_assert(ranges::same_as<decltype(it), span<int>::iterator>);
        auto it2 = it + 1;

        CHECK(it == it);

        CHECK(it != it2);
        CHECK(it2 != it);
        CHECK(it != s.end());
        CHECK(it2 != s.end());
        CHECK(s.end() != it);

        CHECK(it < it2);
        CHECK(it <= it2);
        CHECK(it2 <= s.end());
        CHECK(it < s.end());

        CHECK(it2 > it);
        CHECK(it2 >= it);
        CHECK(s.end() > it2);
        CHECK(s.end() >= it2);
    }
}

void test_case_begin_end()
{
    {
        int a[] = {1, 2, 3, 4};
        span<int> s = a;

        span<int>::iterator it = s.begin();
        span<int>::iterator it2 = ranges::begin(s);
        CHECK(it == it2);

        it = s.end();
        it2 = ranges::end(s);
        CHECK(it == it2);
    }

    {
        int a[] = {1, 2, 3, 4};
        span<int> s = a;

        auto it = s.begin();
        auto first = it;
        CHECK(it == first);
        CHECK(*it == 1);

        auto beyond = s.end();
        CHECK(it != beyond);

        CHECK((beyond - first) == 4);
        CHECK((first - first) == 0);
        CHECK((beyond - beyond) == 0);

        ++it;
        CHECK((it - first) == 1);
        CHECK(*it == 2);
        *it = 22;
        CHECK(*it == 22);
        CHECK((beyond - it) == 3);

        it = first;
        CHECK(it == first);
        while (it != s.end()) {
            *it = 5;
            ++it;
        }

        CHECK(it == beyond);
        CHECK((it - beyond) == 0);

        for (const auto& n : s) {
            CHECK(n == 5);
        }
    }
}

void test_case_rbegin_rend()
{
    {
        int a[] = {1, 2, 3, 4};
        span<int> s = a;

        auto it = s.rbegin();
        auto first = it;
        CHECK(it == first);
        CHECK(*it == 4);

        auto beyond = s.rend();
        CHECK(it != beyond);

        CHECK((beyond - first) == 4);
        CHECK((first - first) == 0);
        CHECK((beyond - beyond) == 0);

        ++it;
        CHECK((it - first) == 1);
        CHECK(*it == 3);
        *it = 22;
        CHECK(*it == 22);
        CHECK((beyond - it) == 3);

        it = first;
        CHECK(it == first);
        while (it != s.rend()) {
            *it = 5;
            ++it;
        }

        CHECK(it == beyond);
        CHECK((it - beyond) == 0);

        for (const auto& n : s) {
            CHECK(n == 5);
        }
    }
}

void test_case_comparison_operators()
{
    {
        span<int> s1;
        span<int> s2;
        CHECK(s1 == s2);
        CHECK(!(s1 != s2));
        CHECK(!(s1 < s2));
        CHECK(s1 <= s2);
        CHECK(!(s1 > s2));
        CHECK(s1 >= s2);
        CHECK(s2 == s1);
        CHECK(!(s2 != s1));
        CHECK(!(s2 < s1));
        CHECK(s2 <= s1);
        CHECK(!(s2 > s1));
        CHECK(s2 >= s1);
    }

    {
        int arr[] = {2, 1};
        span<int> s1 = arr;
        span<int> s2 = arr;

        CHECK(s1 == s2);
        CHECK(!(s1 != s2));
        CHECK(!(s1 < s2));
        CHECK(s1 <= s2);
        CHECK(!(s1 > s2));
        CHECK(s1 >= s2);
        CHECK(s2 == s1);
        CHECK(!(s2 != s1));
        CHECK(!(s2 < s1));
        CHECK(s2 <= s1);
        CHECK(!(s2 > s1));
        CHECK(s2 >= s1);
    }

    {
        int arr[] = {2, 1}; // bigger

        span<int> s1;
        span<int> s2 = arr;

        CHECK(s1 != s2);
        CHECK(s2 != s1);
        CHECK(!(s1 == s2));
        CHECK(!(s2 == s1));
        CHECK(s1 < s2);
        CHECK(!(s2 < s1));
        CHECK(s1 <= s2);
        CHECK(!(s2 <= s1));
        CHECK(s2 > s1);
        CHECK(!(s1 > s2));
        CHECK(s2 >= s1);
        CHECK(!(s1 >= s2));
    }

    {
        int arr1[] = {1, 2};
        int arr2[] = {1, 2};
        span<int> s1 = arr1;
        span<int> s2 = arr2;

        CHECK(s1 == s2);
        CHECK(!(s1 != s2));
        CHECK(!(s1 < s2));
        CHECK(s1 <= s2);
        CHECK(!(s1 > s2));
        CHECK(s1 >= s2);
        CHECK(s2 == s1);
        CHECK(!(s2 != s1));
        CHECK(!(s2 < s1));
        CHECK(s2 <= s1);
        CHECK(!(s2 > s1));
        CHECK(s2 >= s1);
    }

    {
        int arr[] = {1, 2, 3};

        span<int> s1 = {&arr[0], 2}; // shorter
        span<int> s2 = arr;          // longer

        CHECK(s1 != s2);
        CHECK(s2 != s1);
        CHECK(!(s1 == s2));
        CHECK(!(s2 == s1));
        CHECK(s1 < s2);
        CHECK(!(s2 < s1));
        CHECK(s1 <= s2);
        CHECK(!(s2 <= s1));
        CHECK(s2 > s1);
        CHECK(!(s1 > s2));
        CHECK(s2 >= s1);
        CHECK(!(s1 >= s2));
    }

    {
        int arr1[] = {1, 2}; // smaller
        int arr2[] = {2, 1}; // bigger

        span<int> s1 = arr1;
        span<int> s2 = arr2;

        CHECK(s1 != s2);
        CHECK(s2 != s1);
        CHECK(!(s1 == s2));
        CHECK(!(s2 == s1));
        CHECK(s1 < s2);
        CHECK(!(s2 < s1));
        CHECK(s1 <= s2);
        CHECK(!(s2 <= s1));
        CHECK(s2 > s1);
        CHECK(!(s1 > s2));
        CHECK(s2 >= s1);
        CHECK(!(s1 >= s2));
    }
}

void test_case_as_bytes()
{
    int a[] = {1, 2, 3, 4};

    {
        const span<const int> s = a;
        CHECK(s.size() == 4);
        const auto bs = as_bytes(s);
        CHECK(static_cast<const void*>(bs.data()) == static_cast<const void*>(s.data()));
        CHECK(bs.size() == s.size_bytes());
    }

    {
        span<int> s;
        const auto bs = as_bytes(s);
        CHECK(bs.size() == s.size());
        CHECK(bs.size() == 0);
        CHECK(bs.size_bytes() == 0);
        CHECK(static_cast<const void*>(bs.data()) == static_cast<const void*>(s.data()));
        CHECK(bs.data() == nullptr);
    }

    {
        span<int> s = a;
        const auto bs = as_bytes(s);
        CHECK(static_cast<const void*>(bs.data()) == static_cast<const void*>(s.data()));
        CHECK(bs.size() == s.size_bytes());
    }
}

void test_case_as_writeable_bytes()
{
    int a[] = {1, 2, 3, 4};

    {
        span<int> s;
        const auto bs = as_writeable_bytes(s);
        CHECK(bs.size() == s.size());
        CHECK(bs.size() == 0);
        CHECK(bs.size_bytes() == 0);
        CHECK(static_cast<void*>(bs.data()) == static_cast<void*>(s.data()));
        CHECK(bs.data() == nullptr);
    }

    {
        span<int> s = a;
        const auto bs = as_writeable_bytes(s);
        CHECK(static_cast<void*>(bs.data()) == static_cast<void*>(s.data()));
        CHECK(bs.size() == s.size_bytes());
    }
}

void test_case_fixed_size_conversions()
{
    int arr[] = {1, 2, 3, 4};

    // converting to an span from an equal size array is ok
    span<int, 4> s4 = arr;
    CHECK(s4.size() == 4);

    // converting to dynamic_range is always ok
    {
        span<int> s = s4;
        CHECK(s.size() == s4.size());
        static_cast<void>(s);
    }

    // initialization or assignment to static span that REDUCES size is NOT ok
    CPP_assert(!ranges::detail::is_convertible<decltype((arr)), span<int, 2>>::value);
    CPP_assert(!ranges::detail::is_convertible<span<int, 4>, span<int, 2>>::value);


    // you can convert statically
    {
        const span<int, 2> s2 = {arr, 2};
        static_cast<void>(s2);
    }
    {
        const span<int, 1> s1 = s4.first<1>();
        static_cast<void>(s1);
    }

    // ...or dynamically
    {
        // NB: implicit conversion to span<int,1> from span<int>
        span<int, 1> s1 = s4.first(1);
        static_cast<void>(s1);
    }

    // initialization or assignment to static span that requires size INCREASE is not ok.
    int arr2[2] = {1, 2};
    (void)arr2;

    CPP_assert(!std::is_constructible<span<int, 4>, decltype((arr2))>::value);
    CPP_assert(!std::is_constructible<span<int, 4>, span<int, 2>>::value);
}

void test_case_interop_with_std_regex()
{
    char lat[] = {'1', '2', '3', '4', '5', '6', 'E', 'F', 'G'};
    span<char> s = lat;
    const auto f_it = s.begin() + 7;

    std::match_results<span<char>::iterator> match;

    std::regex_match(s.begin(), s.end(), match, std::regex(".*"));
    CHECK(match.ready());
    CHECK(!match.empty());
    CHECK(match[0].matched);
    CHECK(match[0].first == s.begin());
    CHECK(match[0].second == s.end());

    std::regex_search(s.begin(), s.end(), match, std::regex("F"));
    CHECK(match.ready());
    CHECK(!match.empty());
    CHECK(match[0].matched);
    CHECK(match[0].first == f_it);
    CHECK(match[0].second == (f_it + 1));
}

void test_case_default_constructible()
{
    CHECK((std::is_default_constructible<span<int>>::value));
    CHECK((std::is_default_constructible<span<int, 0>>::value));
    CHECK((std::is_default_constructible<span<int, 42>>::value));
}

int main() {
    test_case_default_constructor();
    test_case_size_optimization();
    test_case_from_nullptr_constructor();
    test_case_from_nullptr_size_constructor();
    test_case_from_pointer_size_constructor();
    test_case_from_pointer_pointer_constructor();
    test_case_from_array_constructor();
    test_case_from_std_array_constructor();
    test_case_from_const_std_array_constructor();
    test_case_from_std_array_const_constructor();
    test_case_from_container_constructor();
    test_case_from_convertible_span_constructor();
    test_case_copy_move_and_assignment();
    test_case_class_template_argument_deduction();
    test_case_first();
    test_case_last();
    test_case_subspan();
    test_case_iterator_value_init();
    test_case_iterator_comparisons();
    test_case_begin_end();
    test_case_rbegin_rend();
    test_case_comparison_operators();
    test_case_as_bytes();
    test_case_as_writeable_bytes();
    test_case_fixed_size_conversions();
    test_case_interop_with_std_regex();
    test_case_default_constructible();

    CPP_assert(ranges::view_<span<int>>);
    CPP_assert(ranges::contiguous_range<span<int>>);
    CPP_assert(ranges::view_<span<int, 42>>);
    CPP_assert(ranges::contiguous_range<span<int, 42>>);
}
