// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_UBJSON_UBJSON_OPTIONS_HPP
#define JSONCONS_EXT_UBJSON_UBJSON_OPTIONS_HPP

#include <cwchar>

namespace jsoncons { namespace ubjson {

class ubjson_options;

class ubjson_options_common
{
    friend class ubjson_options;

    int max_nesting_depth_;
protected:
    virtual ~ubjson_options_common() = default;

    ubjson_options_common()
        : max_nesting_depth_(1024)
    {
    }

    ubjson_options_common(const ubjson_options_common&) = default;
    ubjson_options_common& operator=(const ubjson_options_common&) = default;
    ubjson_options_common(ubjson_options_common&&) = default;
    ubjson_options_common& operator=(ubjson_options_common&&) = default;
public:
    int max_nesting_depth() const 
    {
        return max_nesting_depth_;
    }
};

class ubjson_decode_options : public virtual ubjson_options_common
{
    friend class ubjson_options;
    std::size_t max_items_{1 << 24};
public:
    ubjson_decode_options()
    {
    }
    
    ~ubjson_decode_options() = default;

    std::size_t max_items() const
    {
        return max_items_;
    }
};

class ubjson_encode_options : public virtual ubjson_options_common
{
    friend class ubjson_options;
public:
    ubjson_encode_options()
    {
    }
};

class ubjson_options final : public ubjson_decode_options, public ubjson_encode_options
{
public:
    using ubjson_options_common::max_nesting_depth;

    ubjson_options& max_nesting_depth(int value)
    {
        this->max_nesting_depth_ = value;
        return *this;
    }

    ubjson_options& max_items(std::size_t value)
    {
        this->max_items_ = value;
        return *this;
    }
};

} // namespace ubjson
} // namespace jsoncons

#endif // JSONCONS_EXT_UBJSON_UBJSON_OPTIONS_HPP
