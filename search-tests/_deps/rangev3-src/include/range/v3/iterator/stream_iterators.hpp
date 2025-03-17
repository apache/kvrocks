/// \file
// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//  Copyright Google LLC 2020-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_ITERATOR_STREAM_ITERATORS_HPP
#define RANGES_V3_ITERATOR_STREAM_ITERATORS_HPP

#include <cstddef>
#include <iosfwd>
#include <iterator>
#include <string>
#include <type_traits>
#include <utility>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/concepts.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-iterator
    /// @{
    template<typename T = void, typename Char = char,
             typename Traits = std::char_traits<Char>>
    struct ostream_iterator
    {
    private:
        template<class U>
        using value_t = meta::if_<std::is_void<T>, U, T>;

    public:
        using difference_type = std::ptrdiff_t;
        using char_type = Char;
        using traits_type = Traits;
        using ostream_type = std::basic_ostream<Char, Traits>;

        constexpr ostream_iterator() = default;
        ostream_iterator(ostream_type & s, Char const * d = nullptr) noexcept
          : sout_(&s)
          , delim_(d)
        {}
        template(typename U)(
            requires convertible_to<U, value_t<U> const &>)
        ostream_iterator & operator=(U && value)
        {
            RANGES_EXPECT(sout_);
            *sout_ << static_cast<value_t<U> const &>(static_cast<U &&>(value));
            if(delim_)
                *sout_ << delim_;
            return *this;
        }
        ostream_iterator & operator*()
        {
            return *this;
        }
        ostream_iterator & operator++()
        {
            return *this;
        }
        ostream_iterator & operator++(int)
        {
            return *this;
        }

    private:
        ostream_type * sout_;
        Char const * delim_;
    };

    template<typename Delim, typename Char = char,
             typename Traits = std::char_traits<Char>>
    struct ostream_joiner
    {
        CPP_assert(semiregular<Delim>);
        using difference_type = std::ptrdiff_t;
        using char_type = Char;
        using traits_type = Traits;
        using ostream_type = std::basic_ostream<Char, Traits>;

        constexpr ostream_joiner() = default;
        ostream_joiner(ostream_type & s, Delim const & d)
          : delim_(d)
          , sout_(std::addressof(s))
          , first_(true)
        {}
        ostream_joiner(ostream_type & s, Delim && d)
          : delim_(std::move(d))
          , sout_(std::addressof(s))
          , first_(true)
        {}
        template<typename T>
        ostream_joiner & operator=(T const & value)
        {
            RANGES_EXPECT(sout_);
            if(!first_)
                *sout_ << delim_;
            first_ = false;
            *sout_ << value;
            return *this;
        }
        ostream_joiner & operator*() noexcept
        {
            return *this;
        }
        ostream_joiner & operator++() noexcept
        {
            return *this;
        }
        ostream_joiner & operator++(int) noexcept
        {
            return *this;
        }

    private:
        Delim delim_;
        ostream_type * sout_;
        bool first_;
    };

    struct make_ostream_joiner_fn
    {
        template(typename Delim, typename Char, typename Traits)(
            requires semiregular<detail::decay_t<Delim>>)
        ostream_joiner<detail::decay_t<Delim>, Char, Traits> //
        operator()(std::basic_ostream<Char, Traits> & s, Delim && d) const
        {
            return {s, std::forward<Delim>(d)};
        }
    };

    /// \sa `make_ostream_joiner_fn`
    RANGES_INLINE_VARIABLE(make_ostream_joiner_fn, make_ostream_joiner)

    template<typename Char, typename Traits = std::char_traits<Char>>
    struct ostreambuf_iterator
    {
    public:
        typedef ptrdiff_t difference_type;
        typedef Char char_type;
        typedef Traits traits_type;
        typedef std::basic_streambuf<Char, Traits> streambuf_type;
        typedef std::basic_ostream<Char, Traits> ostream_type;

        constexpr ostreambuf_iterator() = default;
        ostreambuf_iterator(ostream_type & s) noexcept
          : ostreambuf_iterator(s.rdbuf())
        {}
        ostreambuf_iterator(streambuf_type * s) noexcept
          : sbuf_(s)
        {
            RANGES_ASSERT(s != nullptr);
        }
        ostreambuf_iterator & operator=(Char c)
        {
            RANGES_ASSERT(sbuf_ != nullptr);
            if(!failed_)
                failed_ = (sbuf_->sputc(c) == Traits::eof());
            return *this;
        }
        ostreambuf_iterator & operator*()
        {
            return *this;
        }
        ostreambuf_iterator & operator++()
        {
            return *this;
        }
        ostreambuf_iterator & operator++(int)
        {
            return *this;
        }
        bool failed() const noexcept
        {
            return failed_;
        }

    private:
        streambuf_type * sbuf_ = nullptr;
        bool failed_ = false;
    };

    namespace cpp20
    {
        template<typename T, typename Char = char,
                 typename Traits = std::char_traits<Char>>
        using ostream_iterator = ranges::ostream_iterator<T, Char, Traits>;

        using ranges::ostreambuf_iterator;
    } // namespace cpp20

    /// \brief Writes to an ostream object using the unformatted
    /// `std::basic_ostream::write` operation. This means that `32` will be encoded as
    /// `100000` as opposed to the string "32".
    ///
    template<typename CharT = char, typename Traits = std::char_traits<CharT>>
    class unformatted_ostream_iterator final
    {
    public:
        using iterator_category = std::output_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using char_type = CharT;
        using traits_type = Traits;
        using ostream_type = std::basic_ostream<CharT, Traits>;

        unformatted_ostream_iterator() = default;

        explicit unformatted_ostream_iterator(ostream_type & out) noexcept
          : out_(&out)
        {}

        template<typename T>
        // requires stream_insertible<T, ostream_type>
        unformatted_ostream_iterator & operator=(T const & t)
        {
            RANGES_EXPECT(out_);
            out_->write(reinterpret_cast<char const *>(std::addressof(t)), sizeof(T));
            return *this;
        }

        unformatted_ostream_iterator & operator*() noexcept
        {
            return *this;
        }
        unformatted_ostream_iterator & operator++() noexcept
        {
            return *this;
        }
        unformatted_ostream_iterator & operator++(int) noexcept
        {
            return *this;
        }

    private:
        ostream_type * out_ = nullptr;
    };
    /// @}
} // namespace ranges

/// \cond
RANGES_DIAGNOSTIC_PUSH
RANGES_DIAGNOSTIC_IGNORE_MISMATCHED_TAGS

namespace std
{
    template<typename T, typename Char, typename Traits>
    struct iterator_traits<::ranges::ostream_iterator<T, Char, Traits>>
      : ::ranges::detail::std_output_iterator_traits<>
    {};

    template<typename Char, typename Traits>
    struct iterator_traits<::ranges::ostreambuf_iterator<Char, Traits>>
      : ::ranges::detail::std_output_iterator_traits<>
    {};
} // namespace std

RANGES_DIAGNOSTIC_POP
/// \endcond

#include <range/v3/detail/epilogue.hpp>

#endif // RANGES_V3_ITERATOR_STREAM_ITERATORS_HPP
