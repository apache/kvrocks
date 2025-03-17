// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_BSON_BSON_OPTIONS_HPP
#define JSONCONS_EXT_BSON_BSON_OPTIONS_HPP

#include <cwchar>

namespace jsoncons { namespace bson {

class bson_options;

class bson_options_common
{
    friend class bson_options;

    int max_nesting_depth_;
protected:
    virtual ~bson_options_common() = default;

    bson_options_common()
        : max_nesting_depth_(1024)
    {
    }

    bson_options_common(const bson_options_common&) = default;
    bson_options_common& operator=(const bson_options_common&) = default;
    bson_options_common(bson_options_common&&) = default;
    bson_options_common& operator=(bson_options_common&&) = default;
public:
    int max_nesting_depth() const 
    {
        return max_nesting_depth_;
    }
};

class bson_decode_options : public virtual bson_options_common
{
    friend class bson_options;
public:
    bson_decode_options()
    {
    }
};

class bson_encode_options : public virtual bson_options_common
{
    friend class bson_options;
public:
    bson_encode_options()
    {
    }
};

class bson_options final : public bson_decode_options, public bson_encode_options
{
public:
    using bson_options_common::max_nesting_depth;

    bson_options& max_nesting_depth(int value)
    {
        this->max_nesting_depth_ = value;
        return *this;
    }
};

} // namespace bson
} // namespace jsoncons

#endif // JSONCONS_EXT_BSON_BSON_OPTIONS_HPP
