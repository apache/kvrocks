/// \file
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

#ifndef RANGES_V3_RANGE_CONCEPTS_HPP
#define RANGES_V3_RANGE_CONCEPTS_HPP

#include <range/v3/detail/config.hpp>

#include <initializer_list>
#include <type_traits>
#include <utility>

#ifdef __has_include
#if __has_include(<span>) && !defined(RANGES_WORKAROUND_MSVC_UNUSABLE_SPAN)
#include <span>
#endif
#if __has_include(<string_view>)
#include <string_view>
#endif
#endif

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/functional/comparisons.hpp>
#include <range/v3/iterator/concepts.hpp>
#include <range/v3/iterator/traits.hpp>
#include <range/v3/range/access.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/range/traits.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-range-concepts
    /// @{

    //
    // Range concepts below
    //

    // clang-format off
    /// \concept _range_
    /// \brief The \c _range_ concept
    template<typename T>
    CPP_requires(_range_,
        requires(T & t) //
        (
            ranges::begin(t), // not necessarily equality-preserving
            ranges::end(t)
        ));
    /// \concept range
    /// \brief The \c range concept
    template<typename T>
    CPP_concept range =
        CPP_requires_ref(ranges::_range_, T);

    /// \concept borrowed_range
    /// \brief The \c borrowed_range concept
    template<typename T>
    CPP_concept borrowed_range =
        range<T> && detail::_borrowed_range<T>;

    template <typename R>
    RANGES_DEPRECATED("Please use ranges::borrowed_range instead.")
    RANGES_INLINE_VAR constexpr bool safe_range = borrowed_range<R>;

    /// \concept output_range_
    /// \brief The \c output_range_ concept
    template(typename T, typename V)(
    concept (output_range_)(T, V),
        output_iterator<iterator_t<T>, V>
    );
    /// \concept output_range
    /// \brief The \c output_range concept
    template<typename T, typename V>
    CPP_concept output_range =
        range<T> && CPP_concept_ref(ranges::output_range_, T, V);

    /// \concept input_range_
    /// \brief The \c input_range_ concept
    template(typename T)(
    concept (input_range_)(T),
        input_iterator<iterator_t<T>>
    );
    /// \concept input_range
    /// \brief The \c input_range concept
    template<typename T>
    CPP_concept input_range =
        range<T> && CPP_concept_ref(ranges::input_range_, T);

    /// \concept forward_range_
    /// \brief The \c forward_range_ concept
    template(typename T)(
    concept (forward_range_)(T),
        forward_iterator<iterator_t<T>>
    );
    /// \concept forward_range
    /// \brief The \c forward_range concept
    template<typename T>
    CPP_concept forward_range =
        input_range<T> && CPP_concept_ref(ranges::forward_range_, T);

    /// \concept bidirectional_range_
    /// \brief The \c bidirectional_range_ concept
    template(typename T)(
    concept (bidirectional_range_)(T),
        bidirectional_iterator<iterator_t<T>>
    );
    /// \concept bidirectional_range
    /// \brief The \c bidirectional_range concept
    template<typename T>
    CPP_concept bidirectional_range =
        forward_range<T> && CPP_concept_ref(ranges::bidirectional_range_, T);

    /// \concept random_access_range_
    /// \brief The \c random_access_range_ concept
    template(typename T)(
    concept (random_access_range_)(T),
        random_access_iterator<iterator_t<T>>
    );

    /// \concept random_access_range
    /// \brief The \c random_access_range concept
    template<typename T>
    CPP_concept random_access_range =
        bidirectional_range<T> && CPP_concept_ref(ranges::random_access_range_, T);
    // clang-format on

    /// \cond
    namespace detail
    {
        template<typename Rng>
        using data_t = decltype(ranges::data(std::declval<Rng &>()));

        template<typename Rng>
        using element_t = meta::_t<std::remove_pointer<data_t<Rng>>>;
    } // namespace detail
      /// \endcond

    // clang-format off
    /// \concept contiguous_range_
    /// \brief The \c contiguous_range_ concept
    template(typename T)(
    concept (contiguous_range_)(T),
        contiguous_iterator<iterator_t<T>> AND
        same_as<detail::data_t<T>, std::add_pointer_t<iter_reference_t<iterator_t<T>>>>
    );

    /// \concept contiguous_range
    /// \brief The \c contiguous_range concept
    template<typename T>
    CPP_concept contiguous_range =
        random_access_range<T> && CPP_concept_ref(ranges::contiguous_range_, T);

    /// \concept common_range_
    /// \brief The \c common_range_ concept
    template(typename T)(
    concept (common_range_)(T),
        same_as<iterator_t<T>, sentinel_t<T>>
    );

    /// \concept common_range
    /// \brief The \c common_range concept
    template<typename T>
    CPP_concept common_range =
        range<T> && CPP_concept_ref(ranges::common_range_, T);

    /// \cond
    /// \concept bounded_range
    /// \brief The \c bounded_range concept
    template<typename T>
    CPP_concept bounded_range =
        common_range<T>;
    /// \endcond

    /// \concept sized_range_
    /// \brief The \c sized_range_ concept
    template<typename T>
    CPP_requires(sized_range_,
        requires(T & t) //
        (
            ranges::size(t)
        ));
    /// \concept sized_range_
    /// \brief The \c sized_range_ concept
    template(typename T)(
    concept (sized_range_)(T),
        detail::integer_like_<range_size_t<T>>);

    /// \concept sized_range
    /// \brief The \c sized_range concept
    template<typename T>
    CPP_concept sized_range =
        range<T> &&
        !disable_sized_range<uncvref_t<T>> &&
        CPP_requires_ref(ranges::sized_range_, T) &&
        CPP_concept_ref(ranges::sized_range_, T);
    // clang-format on

    /// \cond
    namespace ext
    {
        template<typename T>
        struct enable_view
          : std::is_base_of<view_base, T>
        {};
    } // namespace detail
    /// \endcond

    // Specialize this if the default is wrong.
    template<typename T>
    RANGES_INLINE_VAR constexpr bool enable_view =
        ext::enable_view<T>::value;

#if defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201603L
    template<typename Char, typename Traits>
    RANGES_INLINE_VAR constexpr bool enable_view<std::basic_string_view<Char, Traits>> =
        true;
#endif

// libstdc++'s <span> header only defines std::span when concepts
// are also enabled. https://gcc.gnu.org/bugzilla/show_bug.cgi?id=97869
#if defined(__cpp_lib_span) && __cpp_lib_span >= 202002L && \
    (!defined(__GLIBCXX__) || defined(__cpp_lib_concepts))
    template<typename T, std::size_t N>
    RANGES_INLINE_VAR constexpr bool enable_view<std::span<T, N>> = N + 1 < 2;
#endif

    //
    // View concepts below
    //

    // clang-format off
    /// \concept view_
    /// \brief The \c view_ concept
    template<typename T>
    CPP_concept view_ =
        range<T> &&
        semiregular<T> &&
        enable_view<T>;

    /// \concept viewable_range
    /// \brief The \c viewable_range concept
    template<typename T>
    CPP_concept viewable_range =
        range<T> &&
        (borrowed_range<T> || view_<uncvref_t<T>>);
    // clang-format on

    //////////////////////////////////////////////////////////////////////////////////////
    // range_tag
    struct range_tag
    {};

    struct input_range_tag : range_tag
    {};
    struct forward_range_tag : input_range_tag
    {};
    struct bidirectional_range_tag : forward_range_tag
    {};
    struct random_access_range_tag : bidirectional_range_tag
    {};
    struct contiguous_range_tag : random_access_range_tag
    {};

    template<typename Rng>
    using range_tag_of =                          //
        std::enable_if_t<                         //
            range<Rng>,                           //
            meta::conditional_t<                    //
                contiguous_range<Rng>,            //
                contiguous_range_tag,             //
                meta::conditional_t<                //
                    random_access_range<Rng>,     //
                    random_access_range_tag,      //
                    meta::conditional_t<            //
                        bidirectional_range<Rng>, //
                        bidirectional_range_tag,  //
                        meta::conditional_t<        //
                            forward_range<Rng>,   //
                            forward_range_tag,    //
                            meta::conditional_t<    //
                                input_range<Rng>, //
                                input_range_tag,  //
                                range_tag>>>>>>;

    //////////////////////////////////////////////////////////////////////////////////////
    // common_range_tag_of
    struct common_range_tag : range_tag
    {};

    template<typename Rng>
    using common_range_tag_of = //
        std::enable_if_t<       //
            range<Rng>,         //
            meta::conditional_t<common_range<Rng>, common_range_tag, range_tag>>;

    //////////////////////////////////////////////////////////////////////////////////////
    // sized_range_concept
    struct sized_range_tag : range_tag
    {};

    template<typename Rng>
    using sized_range_tag_of = //
        std::enable_if_t<      //
            range<Rng>,        //
            meta::conditional_t<sized_range<Rng>, sized_range_tag, range_tag>>;

    /// \cond
    namespace view_detail_
    {
        // clang-format off
        /// \concept view
        /// \brief The \c view concept
        template<typename T>
        CPP_concept view =
            ranges::view_<T>;
        // clang-format on
    } // namespace view_detail_
    /// \endcond

    namespace cpp20
    {
        using ranges::borrowed_range;
        using ranges::bidirectional_range;
        using ranges::common_range;
        using ranges::contiguous_range;
        using ranges::enable_view;
        using ranges::forward_range;
        using ranges::input_range;
        using ranges::output_range;
        using ranges::random_access_range;
        using ranges::range;
        using ranges::sized_range;
        using ranges::viewable_range;
        using ranges::view_detail_::view;
        using ranges::view_base;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
