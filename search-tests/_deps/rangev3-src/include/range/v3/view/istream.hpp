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

#ifndef RANGES_V3_VIEW_ISTREAM_HPP
#define RANGES_V3_VIEW_ISTREAM_HPP

#include <istream>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/static_const.hpp>
#include <range/v3/view/facade.hpp>

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-views
    /// @{
    template<typename Val>
    struct istream_view : view_facade<istream_view<Val>, unknown>
    {
    private:
        friend range_access;
        std::istream * sin_;
        semiregular_box_t<Val> obj_;
        struct cursor
        {
        private:
            friend range_access;
            using single_pass = std::true_type;
            istream_view * rng_ = nullptr;

        public:
            cursor() = default;
            explicit cursor(istream_view * rng)
              : rng_(rng)
            {}
            void next()
            {
                rng_->next();
            }
            Val & read() const noexcept
            {
                return rng_->cached();
            }
            bool equal(default_sentinel_t) const
            {
                return !rng_->sin_;
            }
            bool equal(cursor that) const
            {
                return !rng_->sin_ == !that.rng_->sin_;
            }
        };
        void next()
        {
            if(!(*sin_ >> cached()))
                sin_ = nullptr;
        }
        cursor begin_cursor()
        {
            return cursor{this};
        }

    public:
        istream_view() = default;
        explicit istream_view(std::istream & sin)
          : sin_(&sin)
          , obj_{}
        {
            next(); // prime the pump
        }
        Val & cached() noexcept
        {
            return obj_;
        }
    };

    /// \cond
    template<typename Val>
    using istream_range RANGES_DEPRECATED(
        "istream_range<T> has been renamed to istream_view<T>") = istream_view<Val>;
    /// \endcond

    /// \cond
    namespace _istream_
    {
        /// \endcond
        template(typename Val)(
            requires copy_constructible<Val> AND default_constructible<Val>)
        inline istream_view<Val> istream(std::istream & sin)
        {
            return istream_view<Val>{sin};
        }
        /// \cond
    } // namespace _istream_
    using namespace _istream_;
    /// \endcond

    namespace cpp20
    {
        template<typename Val>
        using basic_istream_view = ::ranges::istream_view<Val>;
    } // namespace cpp20
    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif
