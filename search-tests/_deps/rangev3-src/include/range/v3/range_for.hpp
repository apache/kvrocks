/// \file
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
//

#ifndef RANGES_V3_RANGE_FOR_HPP
#define RANGES_V3_RANGE_FOR_HPP

#include <range/v3/range_fwd.hpp>

#include <range/v3/range/access.hpp>

#if RANGES_CXX_RANGE_BASED_FOR < RANGES_CXX_RANGE_BASED_FOR_17
/// A range-based for macro, basically a hack until the built-in range-for can handle
/// Ranges that have a different type for begin and end. \ingroup range-core
#define RANGES_FOR(VAR_DECL, ...)                                              \
    if(bool CPP_PP_CAT(_range_v3_done, __LINE__) = false) {}                   \
    else                                                                       \
        for(auto && CPP_PP_CAT(_range_v3_rng, __LINE__) = (__VA_ARGS__);       \
            !CPP_PP_CAT(_range_v3_done, __LINE__);)                            \
            for(auto CPP_PP_CAT(_range_v3_begin, __LINE__) =                   \
                    ranges::begin(CPP_PP_CAT(_range_v3_rng, __LINE__));        \
                !CPP_PP_CAT(_range_v3_done, __LINE__);                         \
                CPP_PP_CAT(_range_v3_done, __LINE__) = true)                   \
                for(auto CPP_PP_CAT(_range_v3_end, __LINE__) =                 \
                        ranges::end(CPP_PP_CAT(_range_v3_rng, __LINE__));      \
                    !CPP_PP_CAT(_range_v3_done, __LINE__) &&                   \
                    CPP_PP_CAT(_range_v3_begin, __LINE__) !=                   \
                        CPP_PP_CAT(_range_v3_end, __LINE__);                   \
                    ++CPP_PP_CAT(_range_v3_begin, __LINE__))                   \
                    if(!(CPP_PP_CAT(_range_v3_done, __LINE__) = true)) {}      \
                    else                                                       \
                        for(VAR_DECL = *CPP_PP_CAT(_range_v3_begin, __LINE__); \
                            CPP_PP_CAT(_range_v3_done, __LINE__);              \
                            CPP_PP_CAT(_range_v3_done, __LINE__) = false)      \
    /**/

#else
#define RANGES_FOR(VAR_DECL, ...) for(VAR_DECL : (__VA_ARGS__))
#endif

#endif
