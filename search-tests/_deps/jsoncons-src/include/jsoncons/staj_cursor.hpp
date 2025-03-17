// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_STAJ_CURSOR_HPP
#define JSONCONS_STAJ_CURSOR_HPP

#include <array> // std::array
#include <cstddef>
#include <cstdint>
#include <functional> // std::function
#include <ios>
#include <memory> // std::allocator
#include <system_error>

#include <jsoncons/detail/write_number.hpp>
#include <jsoncons/json_parser.hpp>
#include <jsoncons/json_type_traits.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/sink.hpp>
#include <jsoncons/staj_event.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/typed_array_view.hpp>
#include <jsoncons/utility/bigint.hpp>
#include <jsoncons/value_converter.hpp>

namespace jsoncons {

// basic_staj_visitor

enum class staj_cursor_state
{
    typed_array = 1,
    multi_dim,
    shape
};

template <typename CharT>
class basic_staj_visitor : public basic_json_visitor<CharT>
{
    using super_type = basic_json_visitor<CharT>;
public:
    using char_type = CharT;
    using typename super_type::string_view_type;
private:
    std::function<bool(const basic_staj_event<CharT>&, const ser_context&)> pred_;
    basic_staj_event<CharT> event_;

    staj_cursor_state state_;
    typed_array_view data_;
    jsoncons::span<const size_t> shape_;
    std::size_t index_{0};
public:
    basic_staj_visitor()
        : pred_(accept), event_(staj_event_type::null_value),
          state_(), data_(), shape_()
    {
    }

    basic_staj_visitor(std::function<bool(const basic_staj_event<CharT>&, const ser_context&)> pred)
        : pred_(pred), event_(staj_event_type::null_value),
          state_(), data_(), shape_()
    {
    }
    
    ~basic_staj_visitor() = default;

    void reset()
    {
        event_ = staj_event_type::null_value;
        state_ = {};
        data_ = {};
        shape_ = {};
        index_ = 0;
    }

    const basic_staj_event<CharT>& event() const
    {
        return event_;
    }

    bool in_available() const
    {
        return state_ != staj_cursor_state();
    }

    void send_available(std::error_code& ec)
    {
        switch (state_)
        {
            case staj_cursor_state::typed_array:
                advance_typed_array(ec);
                break;
            case staj_cursor_state::multi_dim:
            case staj_cursor_state::shape:
                advance_multi_dim(ec);
                break;
            default:
                break;
        }
    }

    bool is_typed_array() const
    {
        return data_.type() != typed_array_type();
    }

    staj_cursor_state state() const
    {
        return state_;
    }

    void advance_typed_array(std::error_code& ec)
    {
        if (is_typed_array())
        {
            if (index_ < data_.size())
            {
                switch (data_.type())
                {
                    case typed_array_type::uint8_value:
                    {
                        this->uint64_value(data_.data(uint8_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    case typed_array_type::uint16_value:
                    {
                        this->uint64_value(data_.data(uint16_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    case typed_array_type::uint32_value:
                    {
                        this->uint64_value(data_.data(uint32_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    case typed_array_type::uint64_value:
                    {
                        this->uint64_value(data_.data(uint64_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    case typed_array_type::int8_value:
                    {
                        this->int64_value(data_.data(int8_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    case typed_array_type::int16_value:
                    {
                        this->int64_value(data_.data(int16_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    case typed_array_type::int32_value:
                    {
                        this->int64_value(data_.data(int32_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    case typed_array_type::int64_value:
                    {
                        this->int64_value(data_.data(int64_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    case typed_array_type::half_value:
                    {
                        this->half_value(data_.data(half_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    case typed_array_type::float_value:
                    {
                        this->double_value(data_.data(float_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    case typed_array_type::double_value:
                    {
                        this->double_value(data_.data(double_array_arg)[index_], semantic_tag::none, ser_context(), ec);
                        break;
                    }
                    default:
                        break;
                }
                ++index_;
            }
            else
            {
                this->end_array();
                state_ = staj_cursor_state();
                data_ = typed_array_view();
                index_ = 0;
            }
        }
    }

    void advance_multi_dim(std::error_code& ec)
    {
        if (shape_.size() != 0)
        {
            if (state_ == staj_cursor_state::multi_dim)
            {
                this->begin_array(shape_.size(), semantic_tag::none, ser_context(), ec);
                state_ = staj_cursor_state::shape;
            }
            else if (index_ < shape_.size())
            {
                this->uint64_value(shape_[index_], semantic_tag::none, ser_context(), ec);
                ++index_;
            }
            else
            {
                state_ = staj_cursor_state();
                this->end_array(ser_context(), ec);
                shape_ = jsoncons::span<const size_t>();
                index_ = 0;
            }
        }
    }

    bool dump(basic_json_visitor<CharT>& visitor, const ser_context& context, std::error_code& ec)
    {
        bool more = true;
        if (is_typed_array())
        {
            if (index_ != 0)
            {
                more = event().send_json_event(visitor, context, ec);
                while (more && is_typed_array())
                {
                    if (index_ < data_.size())
                    {
                        switch (data_.type())
                        {
                            case typed_array_type::uint8_value:
                            {
                                more = visitor.uint64_value(data_.data(uint8_array_arg)[index_]);
                                break;
                            }
                            case typed_array_type::uint16_value:
                            {
                                more = visitor.uint64_value(data_.data(uint16_array_arg)[index_]);
                                break;
                            }
                            case typed_array_type::uint32_value:
                            {
                                more = visitor.uint64_value(data_.data(uint32_array_arg)[index_]);
                                break;
                            }
                            case typed_array_type::uint64_value:
                            {
                                more = visitor.uint64_value(data_.data(uint64_array_arg)[index_]);
                                break;
                            }
                            case typed_array_type::int8_value:
                            {
                                more = visitor.int64_value(data_.data(int8_array_arg)[index_]);
                                break;
                            }
                            case typed_array_type::int16_value:
                            {
                                more = visitor.int64_value(data_.data(int16_array_arg)[index_]);
                                break;
                            }
                            case typed_array_type::int32_value:
                            {
                                more = visitor.int64_value(data_.data(int32_array_arg)[index_]);
                                break;
                            }
                            case typed_array_type::int64_value:
                            {
                                more = visitor.int64_value(data_.data(int64_array_arg)[index_]);
                                break;
                            }
                            case typed_array_type::float_value:
                            {
                                more = visitor.double_value(data_.data(float_array_arg)[index_]);
                                break;
                            }
                            case typed_array_type::double_value:
                            {
                                more = visitor.double_value(data_.data(double_array_arg)[index_]);
                                break;
                            }
                            default:
                                break;
                        }
                        ++index_;
                    }
                    else
                    {
                        more = visitor.end_array();
                        state_ = staj_cursor_state();
                        data_ = typed_array_view();
                        index_ = 0;
                    }
                }
            }
            else
            {
                switch (data_.type())
                {
                    case typed_array_type::uint8_value:
                    {
                        more = visitor.typed_array(data_.data(uint8_array_arg));
                        break;
                    }
                    case typed_array_type::uint16_value:
                    {
                        more = visitor.typed_array(data_.data(uint16_array_arg));
                        break;
                    }
                    case typed_array_type::uint32_value:
                    {
                        more = visitor.typed_array(data_.data(uint32_array_arg));
                        break;
                    }
                    case typed_array_type::uint64_value:
                    {
                        more = visitor.typed_array(data_.data(uint64_array_arg));
                        break;
                    }
                    case typed_array_type::int8_value:
                    {
                        more = visitor.typed_array(data_.data(int8_array_arg));
                        break;
                    }
                    case typed_array_type::int16_value:
                    {
                        more = visitor.typed_array(data_.data(int16_array_arg));
                        break;
                    }
                    case typed_array_type::int32_value:
                    {
                        more = visitor.typed_array(data_.data(int32_array_arg));
                        break;
                    }
                    case typed_array_type::int64_value:
                    {
                        more = visitor.typed_array(data_.data(int64_array_arg));
                        break;
                    }
                    case typed_array_type::float_value:
                    {
                        more = visitor.typed_array(data_.data(float_array_arg));
                        break;
                    }
                    case typed_array_type::double_value:
                    {
                        more = visitor.typed_array(data_.data(double_array_arg));
                        break;
                    }
                    default:
                        break;
                }

                state_ = staj_cursor_state();
                data_ = typed_array_view();
            }
        }
        else
        {
            more = event().send_json_event(visitor, context, ec);
        }
        return more;
    }

private:
    static constexpr bool accept(const basic_staj_event<CharT>&, const ser_context&) 
    {
        return true;
    }

    bool visit_begin_object(semantic_tag tag, const ser_context& context, std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::begin_object, tag);
        return !pred_(event_, context);
    }

    bool visit_begin_object(std::size_t length, semantic_tag tag, const ser_context& context, std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::begin_object, length, tag);
        return !pred_(event_, context);
    }

    bool visit_end_object(const ser_context& context, std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::end_object);
        return !pred_(event_, context);
    }

    bool visit_begin_array(semantic_tag tag, const ser_context& context, std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::begin_array, tag);
        return !pred_(event_, context);
    }

    bool visit_begin_array(std::size_t length, semantic_tag tag, const ser_context& context, std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::begin_array, length, tag);
        return !pred_(event_, context);
    }

    bool visit_end_array(const ser_context& context, std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::end_array);
        return !pred_(event_, context);
    }

    bool visit_key(const string_view_type& name, const ser_context& context, std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(name, staj_event_type::key);
        return !pred_(event_, context);
    }

    bool visit_null(semantic_tag tag, const ser_context& context, std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::null_value, tag);
        return !pred_(event_, context);
    }

    bool visit_bool(bool value, semantic_tag tag, const ser_context& context, std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(value, tag);
        return !pred_(event_, context);
    }

    bool visit_string(const string_view_type& s, semantic_tag tag, const ser_context& context, std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(s, staj_event_type::string_value, tag);
        return !pred_(event_, context);
    }

    bool visit_byte_string(const byte_string_view& s, 
                           semantic_tag tag,
                           const ser_context& context,
                           std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(s, staj_event_type::byte_string_value, tag);
        return !pred_(event_, context);
    }

    bool visit_byte_string(const byte_string_view& s, 
                           uint64_t ext_tag,
                           const ser_context& context,
                           std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(s, staj_event_type::byte_string_value, ext_tag);
        return !pred_(event_, context);
    }

    bool visit_uint64(uint64_t value, 
                         semantic_tag tag, 
                         const ser_context& context,
                         std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(value, tag);
        return !pred_(event_, context);
    }

    bool visit_int64(int64_t value, 
                  semantic_tag tag,
                  const ser_context& context,
                  std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(value, tag);
        return !pred_(event_, context);
    }

    bool visit_half(uint16_t value, 
                 semantic_tag tag,
                 const ser_context& context,
                 std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(half_arg, value, tag);
        return !pred_(event_, context);
    }

    bool visit_double(double value, 
                   semantic_tag tag, 
                   const ser_context& context,
                   std::error_code&) override
    {
        event_ = basic_staj_event<CharT>(value, tag);
        return !pred_(event_, context);
    }

    bool visit_typed_array(const jsoncons::span<const uint8_t>& v, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(v.data(), v.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }

    bool visit_typed_array(const jsoncons::span<const uint16_t>& data, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(data.data(), data.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }

    bool visit_typed_array(const jsoncons::span<const uint32_t>& data, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(data.data(), data.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }

    bool visit_typed_array(const jsoncons::span<const uint64_t>& data, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(data.data(), data.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }

    bool visit_typed_array(const jsoncons::span<const int8_t>& data, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(data.data(), data.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }

    bool visit_typed_array(const jsoncons::span<const int16_t>& data, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(data.data(), data.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }

    bool visit_typed_array(const jsoncons::span<const int32_t>& data, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(data.data(), data.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }

    bool visit_typed_array(const jsoncons::span<const int64_t>& data, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(data.data(), data.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }

    bool visit_typed_array(half_arg_t, const jsoncons::span<const uint16_t>& data, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(data.data(), data.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }

    bool visit_typed_array(const jsoncons::span<const float>& data, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(data.data(), data.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }

    bool visit_typed_array(const jsoncons::span<const double>& data, 
                        semantic_tag tag,
                        const ser_context& context,
                        std::error_code& ec) override
    {
        state_ = staj_cursor_state::typed_array;
        data_ = typed_array_view(data.data(), data.size());
        index_ = 0;
        return this->begin_array(tag, context, ec);
    }
/*
    bool visit_typed_array(const jsoncons::span<const float128_type>&, 
                        semantic_tag,
                        const ser_context&,
                        std::error_code&) override
    {
        return true;
    }
*/
    bool visit_begin_multi_dim(const jsoncons::span<const size_t>& shape,
                            semantic_tag tag,
                            const ser_context& context, 
                            std::error_code& ec) override
    {
        state_ = staj_cursor_state::multi_dim;
        shape_ = shape;
        return this->begin_array(2, tag, context, ec);
    }

    bool visit_end_multi_dim(const ser_context& context,
                          std::error_code& ec) override
    {
        return this->end_array(context, ec);
    }

    void visit_flush() override
    {
    }
};


// basic_staj_cursor

template <typename CharT>
class basic_staj_cursor
{
public:
    virtual ~basic_staj_cursor() = default;

    virtual void array_expected(std::error_code& ec)
    {
        if (!(current().event_type() == staj_event_type::begin_array || current().event_type() == staj_event_type::byte_string_value))
        {
            ec = conv_errc::not_vector;
        }
    }

    virtual bool done() const = 0;

    virtual const basic_staj_event<CharT>& current() const = 0;

    virtual void read_to(basic_json_visitor<CharT>& visitor) = 0;

    virtual void read_to(basic_json_visitor<CharT>& visitor,
                         std::error_code& ec) = 0;

    virtual void next() = 0;

    virtual void next(std::error_code& ec) = 0;

    virtual const ser_context& context() const = 0;
};

template <typename CharT>
class basic_staj_filter_view : basic_staj_cursor<CharT>
{
    basic_staj_cursor<CharT>* cursor_;
    std::function<bool(const basic_staj_event<CharT>&, const ser_context&)> pred_;
public:
    basic_staj_filter_view(basic_staj_cursor<CharT>& cursor, 
                     std::function<bool(const basic_staj_event<CharT>&, const ser_context&)> pred)
        : cursor_(std::addressof(cursor)), pred_(pred)
    {
        while (!done() && !pred_(current(),context()))
        {
            cursor_->next();
        }
    }

    bool done() const override
    {
        return cursor_->done();
    }

    const basic_staj_event<CharT>& current() const override
    {
        return cursor_->current();
    }

    void read_to(basic_json_visitor<CharT>& visitor) override
    {
        cursor_->read_to(visitor);
    }

    void read_to(basic_json_visitor<CharT>& visitor,
                 std::error_code& ec) override
    {
        cursor_->read_to(visitor, ec);
    }

    void next() override
    {
        cursor_->next();
        while (!done() && !pred_(current(),context()))
        {
            cursor_->next();
        }
    }

    void next(std::error_code& ec) override
    {
        cursor_->next(ec);
        while (!done() && !pred_(current(),context()) && !ec)
        {
            cursor_->next(ec);
        }
    }

    const ser_context& context() const override
    {
        return cursor_->context();
    }

    friend
    basic_staj_filter_view<CharT> operator|(basic_staj_filter_view& cursor, 
                                      std::function<bool(const basic_staj_event<CharT>&, const ser_context&)> pred)
    {
        return basic_staj_filter_view<CharT>(cursor, pred);
    }
};

using staj_event = basic_staj_event<char>;
using wstaj_event = basic_staj_event<wchar_t>;

using staj_cursor = basic_staj_cursor<char>;
using wstaj_cursor = basic_staj_cursor<wchar_t>;

using staj_filter_view = basic_staj_filter_view<char>;
using wstaj_filter_view = basic_staj_filter_view<wchar_t>;

} // namespace jsoncons

#endif // JSONCONS_STAJ_CURSOR_HPP

