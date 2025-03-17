// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CBOR_CBOR_OPTIONS_HPP
#define JSONCONS_EXT_CBOR_CBOR_OPTIONS_HPP

#include <cwchar>

#include <jsoncons_ext/cbor/cbor_detail.hpp>

namespace jsoncons { namespace cbor {

class cbor_options;

class cbor_options_common
{
    friend class cbor_options;

    int max_nesting_depth_;
protected:
    virtual ~cbor_options_common() = default;

    cbor_options_common()
        : max_nesting_depth_(1024)
    {
    }

    cbor_options_common(const cbor_options_common&) = default;
    cbor_options_common& operator=(const cbor_options_common&) = default;
    cbor_options_common(cbor_options_common&&) = default;
    cbor_options_common& operator=(cbor_options_common&&) = default;
public:
    int max_nesting_depth() const 
    {
        return max_nesting_depth_;
    }
};

class cbor_decode_options : public virtual cbor_options_common
{
    friend class cbor_options;
public:
    cbor_decode_options()
    {
    }
};

class cbor_encode_options : public virtual cbor_options_common
{
    friend class cbor_options;

    bool use_stringref_;
    bool use_typed_arrays_;
public:
    cbor_encode_options()
        : use_stringref_(false),
          use_typed_arrays_(false)
    {
    }

    bool pack_strings() const 
    {
        return use_stringref_;
    }

    bool use_typed_arrays() const 
    {
        return use_typed_arrays_;
    }
};

class cbor_options final : public cbor_decode_options, public cbor_encode_options
{
public:
    using cbor_options_common::max_nesting_depth;
    using cbor_encode_options::pack_strings;
    using cbor_encode_options::use_typed_arrays;

    cbor_options& max_nesting_depth(int value)
    {
        this->max_nesting_depth_ = value;
        return *this;
    }

    cbor_options& pack_strings(bool value)
    {
        this->use_stringref_ = value;
        return *this;
    }

    cbor_options& use_typed_arrays(bool value)
    {
        this->use_typed_arrays_ = value;
        return *this;
    }
};

} // namespace cbor
} // namespace jsoncons

#endif
