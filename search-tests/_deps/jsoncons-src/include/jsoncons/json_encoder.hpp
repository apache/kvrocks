// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_ENCODER_HPP
#define JSONCONS_JSON_ENCODER_HPP

#include <array> // std::array
#include <cstddef>
#include <cstdint>
#include <cmath> // std::isfinite, std::isnan
#include <limits> // std::numeric_limits
#include <memory>
#include <string>
#include <utility> // std::move
#include <vector>

#include <jsoncons/utility/byte_string.hpp>
#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/detail/write_number.hpp>
#include <jsoncons/json_error.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_options.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/sink.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/utility/bigint.hpp>
#include <jsoncons/utility/unicode_traits.hpp>

namespace jsoncons { 
namespace detail {

    inline
    bool is_control_character(uint32_t c)
    {
        return c <= 0x1F || c == 0x7f;
    }

    inline
    bool is_non_ascii_codepoint(uint32_t cp)
    {
        return cp >= 0x80;
    }

    template <typename CharT,typename Sink>
    std::size_t escape_string(const CharT* s, std::size_t length,
                         bool escape_all_non_ascii, bool escape_solidus,
                         Sink& sink)
    {
        std::size_t count = 0;
        const CharT* begin = s;
        const CharT* end = s + length;
        for (const CharT* it = begin; it != end; ++it)
        {
            CharT c = *it;
            switch (c)
            {
                case '\\':
                    sink.push_back('\\');
                    sink.push_back('\\');
                    count += 2;
                    break;
                case '"':
                    sink.push_back('\\');
                    sink.push_back('\"');
                    count += 2;
                    break;
                case '\b':
                    sink.push_back('\\');
                    sink.push_back('b');
                    count += 2;
                    break;
                case '\f':
                    sink.push_back('\\');
                    sink.push_back('f');
                    count += 2;
                    break;
                case '\n':
                    sink.push_back('\\');
                    sink.push_back('n');
                    count += 2;
                    break;
                case '\r':
                    sink.push_back('\\');
                    sink.push_back('r');
                    count += 2;
                    break;
                case '\t':
                    sink.push_back('\\');
                    sink.push_back('t');
                    count += 2;
                    break;
                default:
                    if (escape_solidus && c == '/')
                    {
                        sink.push_back('\\');
                        sink.push_back('/');
                        count += 2;
                    }
                    else if (is_control_character(c) || escape_all_non_ascii)
                    {
                        // convert to codepoint
                        uint32_t cp;
                        auto r = unicode_traits::to_codepoint(it, end, cp, unicode_traits::conv_flags::strict);
                        if (r.ec != unicode_traits::conv_errc())
                        {
                            JSONCONS_THROW(ser_error(json_errc::illegal_codepoint));
                        }
                        it = r.ptr - 1;
                        if (is_non_ascii_codepoint(cp) || is_control_character(c))
                        {
                            if (cp > 0xFFFF)
                            {
                                cp -= 0x10000;
                                uint32_t first = (cp >> 10) + 0xD800;
                                uint32_t second = ((cp & 0x03FF) + 0xDC00);

                                sink.push_back('\\');
                                sink.push_back('u');
                                sink.push_back(jsoncons::detail::to_hex_character(first >> 12 & 0x000F));
                                sink.push_back(jsoncons::detail::to_hex_character(first >> 8 & 0x000F));
                                sink.push_back(jsoncons::detail::to_hex_character(first >> 4 & 0x000F));
                                sink.push_back(jsoncons::detail::to_hex_character(first & 0x000F));
                                sink.push_back('\\');
                                sink.push_back('u');
                                sink.push_back(jsoncons::detail::to_hex_character(second >> 12 & 0x000F));
                                sink.push_back(jsoncons::detail::to_hex_character(second >> 8 & 0x000F));
                                sink.push_back(jsoncons::detail::to_hex_character(second >> 4 & 0x000F));
                                sink.push_back(jsoncons::detail::to_hex_character(second & 0x000F));
                                count += 12;
                            }
                            else
                            {
                                sink.push_back('\\');
                                sink.push_back('u');
                                sink.push_back(jsoncons::detail::to_hex_character(cp >> 12 & 0x000F));
                                sink.push_back(jsoncons::detail::to_hex_character(cp >> 8 & 0x000F));
                                sink.push_back(jsoncons::detail::to_hex_character(cp >> 4 & 0x000F));
                                sink.push_back(jsoncons::detail::to_hex_character(cp & 0x000F));
                                count += 6;
                            }
                        }
                        else
                        {
                            sink.push_back(c);
                            ++count;
                        }
                    }
                    else
                    {
                        sink.push_back(c);
                        ++count;
                    }
                    break;
            }
        }
        return count;
    }

    inline
    byte_string_chars_format resolve_byte_string_chars_format(byte_string_chars_format format1,
                                                              byte_string_chars_format format2,
                                                              byte_string_chars_format default_format = byte_string_chars_format::base64url)
    {
        byte_string_chars_format sink;
        switch (format1)
        {
            case byte_string_chars_format::base16:
            case byte_string_chars_format::base64:
            case byte_string_chars_format::base64url:
                sink = format1;
                break;
            default:
                switch (format2)
                {
                    case byte_string_chars_format::base64url:
                    case byte_string_chars_format::base64:
                    case byte_string_chars_format::base16:
                        sink = format2;
                        break;
                    default: // base64url
                    {
                        sink = default_format;
                        break;
                    }
                }
                break;
        }
        return sink;
    }

} // namespace detail

    template <typename CharT,typename Sink=jsoncons::stream_sink<CharT>,typename Allocator=std::allocator<char>>
    class basic_json_encoder final : public basic_json_visitor<CharT>
    {
        static const jsoncons::basic_string_view<CharT> null_constant()
        {
            static const jsoncons::basic_string_view<CharT> k = JSONCONS_STRING_VIEW_CONSTANT(CharT, "null");
            return k;
        }
        static const jsoncons::basic_string_view<CharT> true_constant()
        {
            static const jsoncons::basic_string_view<CharT> k = JSONCONS_STRING_VIEW_CONSTANT(CharT, "true");
            return k;
        }
        static const jsoncons::basic_string_view<CharT> false_constant()
        {
            static const jsoncons::basic_string_view<CharT> k = JSONCONS_STRING_VIEW_CONSTANT(CharT, "false");
            return k;
        }
    public:
        using allocator_type = Allocator;
        using char_type = CharT;
        using typename basic_json_visitor<CharT>::string_view_type;
        using sink_type = Sink;
        using string_type = typename basic_json_encode_options<CharT>::string_type;

    private:
        enum class container_type {object, array};

        class encoding_context
        {
            container_type type_;
            std::size_t count_{0};
            line_split_kind line_splits_;
            bool indent_before_;
            bool new_line_after_;
            std::size_t begin_pos_{0};
            std::size_t data_pos_{0};
        public:
            encoding_context(container_type type, line_split_kind split_lines, bool indent_once,
                             std::size_t begin_pos, std::size_t data_pos) noexcept
               : type_(type), count_(0), line_splits_(split_lines), indent_before_(indent_once), new_line_after_(false),
                 begin_pos_(begin_pos), data_pos_(data_pos)
            {
            }

            encoding_context(const encoding_context&) = default;
            
            ~encoding_context() = default;
            
            encoding_context& operator=(const encoding_context&) = default;

            void set_position(std::size_t pos)
            {
                data_pos_ = pos;
            }

            std::size_t begin_pos() const
            {
                return begin_pos_;
            }

            std::size_t data_pos() const
            {
                return data_pos_;
            }

            std::size_t count() const
            {
                return count_;
            }

            void increment_count()
            {
                ++count_;
            }

            bool new_line_after() const
            {
                return new_line_after_;
            }

            void new_line_after(bool value) 
            {
                new_line_after_ = value;
            }

            bool is_object() const
            {
                return type_ == container_type::object;
            }

            bool is_array() const
            {
                return type_ == container_type::array;
            }

            bool is_same_line() const
            {
                return line_splits_ == line_split_kind::same_line;
            }

            bool is_new_line() const
            {
                return line_splits_ == line_split_kind::new_line;
            }

            bool is_multi_line() const
            {
                return line_splits_ == line_split_kind::multi_line;
            }

            bool is_indent_once() const
            {
                return count_ == 0 ? indent_before_ : false;
            }

        };
        using encoding_context_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<encoding_context>;

        Sink sink_;
        basic_json_encode_options<CharT> options_;
        jsoncons::detail::write_double fp_;

        std::vector<encoding_context,encoding_context_allocator_type> stack_;
        int indent_amount_;
        std::size_t column_;
        std::basic_string<CharT> colon_str_;
        std::basic_string<CharT> comma_str_;
        std::basic_string<CharT> open_object_brace_str_;
        std::basic_string<CharT> close_object_brace_str_;
        std::basic_string<CharT> open_array_bracket_str_;
        std::basic_string<CharT> close_array_bracket_str_;
        int nesting_depth_;
    public:

        // Noncopyable and nonmoveable
        basic_json_encoder(const basic_json_encoder&) = delete;
        basic_json_encoder(basic_json_encoder&&) = delete;

        basic_json_encoder(Sink&& sink, 
                           const Allocator& alloc = Allocator())
            : basic_json_encoder(std::forward<Sink>(sink), basic_json_encode_options<CharT>(), alloc)
        {
        }

        basic_json_encoder(Sink&& sink, 
                           const basic_json_encode_options<CharT>& options, 
                           const Allocator& alloc = Allocator())
           : sink_(std::forward<Sink>(sink)), 
             options_(options),
             fp_(options.float_format(), options.precision()),
             stack_(alloc),
             indent_amount_(0), 
             column_(0),
             nesting_depth_(0)
        {
            switch (options.spaces_around_colon())
            {
                case spaces_option::space_after:
                    colon_str_ = std::basic_string<CharT>({':',' '});
                    break;
                case spaces_option::space_before:
                    colon_str_ = std::basic_string<CharT>({' ',':'});
                    break;
                case spaces_option::space_before_and_after:
                    colon_str_ = std::basic_string<CharT>({' ',':',' '});
                    break;
                default:
                    colon_str_.push_back(':');
                    break;
            }
            switch (options.spaces_around_comma())
            {
                case spaces_option::space_after:
                    comma_str_ = std::basic_string<CharT>({',',' '});
                    break;
                case spaces_option::space_before:
                    comma_str_ = std::basic_string<CharT>({' ',','});
                    break;
                case spaces_option::space_before_and_after:
                    comma_str_ = std::basic_string<CharT>({' ',',',' '});
                    break;
                default:
                    comma_str_.push_back(',');
                    break;
            }
            if (options.pad_inside_object_braces())
            {
                open_object_brace_str_ = std::basic_string<CharT>({'{', ' '});
                close_object_brace_str_ = std::basic_string<CharT>({' ', '}'});
            }
            else
            {
                open_object_brace_str_.push_back('{');
                close_object_brace_str_.push_back('}');
            }
            if (options.pad_inside_array_brackets())
            {
                open_array_bracket_str_ = std::basic_string<CharT>({'[', ' '});
                close_array_bracket_str_ = std::basic_string<CharT>({' ', ']'});
            }
            else
            {
                open_array_bracket_str_.push_back('[');
                close_array_bracket_str_.push_back(']');
            }
        }

        ~basic_json_encoder() noexcept
        {
            JSONCONS_TRY
            {
                sink_.flush();
            }
            JSONCONS_CATCH(...)
            {
            }
        }

        basic_json_encoder& operator=(const basic_json_encoder&) = delete;
        basic_json_encoder& operator=(basic_json_encoder&&) = delete;

        void reset()
        {
            stack_.clear();
            indent_amount_ = 0;
            column_ = 0;
            nesting_depth_ = 0;
        }

        void reset(Sink&& sink)
        {
            sink_ = std::move(sink);
            reset();
        }

    private:
        // Implementing methods
        void visit_flush() final
        {
            sink_.flush();
        }

        bool visit_begin_object(semantic_tag, const ser_context&, std::error_code& ec) final
        {
            if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
            {
                ec = json_errc::max_nesting_depth_exceeded;
                return false;
            } 
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.append(comma_str_.data(),comma_str_.length());
                column_ += comma_str_.length();
            }

            if (!stack_.empty()) // object or array
            {
                if (stack_.back().is_object())
                {
                    switch (options_.object_object_line_splits())
                    {
                        case line_split_kind::same_line:
                        case line_split_kind::new_line:
                            if (column_ >= options_.line_length_limit())
                            {
                                break_line();
                            }
                            break;
                        default: // multi_line
                            break;
                    }
                    stack_.emplace_back(container_type::object,options_.object_object_line_splits(), false,
                                        column_, column_+open_object_brace_str_.length());
                }
                else // array
                {
                    switch (options_.array_object_line_splits())
                    {
                        case line_split_kind::same_line:
                            if (column_ >= options_.line_length_limit())
                            {
                                //stack_.back().new_line_after(true);
                                new_line();
                            }
                            else
                            {
                                stack_.back().new_line_after(true);
                                new_line();
                            }
                            break;
                        case line_split_kind::new_line:
                            stack_.back().new_line_after(true);
                            new_line();
                            break;
                        default: // multi_line
                            stack_.back().new_line_after(true);
                            new_line();
                            break;
                    }
                    stack_.emplace_back(container_type::object,options_.array_object_line_splits(), false,
                                        column_, column_+open_object_brace_str_.length());
                }
            }
            else 
            {
                stack_.emplace_back(container_type::object, options_.line_splits(), false,
                                    column_, column_+open_object_brace_str_.length());
            }
            indent();
            
            sink_.append(open_object_brace_str_.data(), open_object_brace_str_.length());
            column_ += open_object_brace_str_.length();
            return true;
        }

        bool visit_end_object(const ser_context&, std::error_code&) final
        {
            JSONCONS_ASSERT(!stack_.empty());
            --nesting_depth_;

            unindent();
            if (stack_.back().new_line_after())
            {
                new_line();
            }
            stack_.pop_back();
            sink_.append(close_object_brace_str_.data(), close_object_brace_str_.length());
            column_ += close_object_brace_str_.length();

            end_value();
            return true;
        }

        bool visit_begin_array(semantic_tag, const ser_context&, std::error_code& ec) final
        {
            if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
            {
                ec = json_errc::max_nesting_depth_exceeded;
                return false;
            } 
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.append(comma_str_.data(),comma_str_.length());
                column_ += comma_str_.length();
            }
            if (!stack_.empty())
            {
                if (stack_.back().is_object())
                {
                    switch (options_.object_array_line_splits())
                    {
                        case line_split_kind::same_line:
                            stack_.emplace_back(container_type::array,options_.object_array_line_splits(),false,
                                                column_, column_ + open_array_bracket_str_.length());
                            break;
                        case line_split_kind::new_line:
                        {
                            stack_.emplace_back(container_type::array,options_.object_array_line_splits(),true,
                                                column_, column_+open_array_bracket_str_.length());
                            break;
                        }
                        default: // multi_line
                            stack_.emplace_back(container_type::array,options_.object_array_line_splits(),true,
                                                column_, column_+open_array_bracket_str_.length());
                            break;
                    }
                }
                else // array
                {
                    switch (options_.array_array_line_splits())
                    {
                        case line_split_kind::same_line:
                            if (stack_.back().is_multi_line())
                            {
                                stack_.back().new_line_after(true);
                                new_line();
                            }
                            stack_.emplace_back(container_type::array,options_.array_array_line_splits(), false,
                                                column_, column_+open_array_bracket_str_.length());
                            break;
                        case line_split_kind::new_line:
                            stack_.back().new_line_after(true);
                            new_line();
                            stack_.emplace_back(container_type::array,options_.array_array_line_splits(), false,
                                                column_, column_+open_array_bracket_str_.length());
                            break;
                        default: // multi_line
                            stack_.back().new_line_after(true);
                            new_line();
                            stack_.emplace_back(container_type::array,options_.array_array_line_splits(), false,
                                                column_, column_+open_array_bracket_str_.length());
                            //new_line();
                            break;
                    }
                }
            }
            else 
            {
                stack_.emplace_back(container_type::array, options_.line_splits(), false,
                                    column_, column_+open_array_bracket_str_.length());
            }
            indent();
            sink_.append(open_array_bracket_str_.data(), open_array_bracket_str_.length());
            column_ += open_array_bracket_str_.length();
            return true;
        }

        bool visit_end_array(const ser_context&, std::error_code&) final
        {
            JSONCONS_ASSERT(!stack_.empty());
            --nesting_depth_;

            unindent();
            if (stack_.back().new_line_after())
            {
                new_line();
            }
            stack_.pop_back();
            sink_.append(close_array_bracket_str_.data(), close_array_bracket_str_.length());
            column_ += close_array_bracket_str_.length();
            end_value();
            return true;
        }

        bool visit_key(const string_view_type& name, const ser_context&, std::error_code&) final
        {
            JSONCONS_ASSERT(!stack_.empty());
            if (stack_.back().count() > 0)
            {
                sink_.append(comma_str_.data(),comma_str_.length());
                column_ += comma_str_.length();
            }

            if (stack_.back().is_multi_line())
            {
                stack_.back().new_line_after(true);
                new_line();
            }
            else if (stack_.back().count() > 0 && column_ >= options_.line_length_limit())
            {
                //stack_.back().new_line_after(true);
                new_line(stack_.back().data_pos());
            }

            if (stack_.back().count() == 0)
            {
                stack_.back().set_position(column_);
            }
            sink_.push_back('\"');
            std::size_t length = jsoncons::detail::escape_string(name.data(), name.length(),options_.escape_all_non_ascii(),options_.escape_solidus(),sink_);
            sink_.push_back('\"');
            sink_.append(colon_str_.data(),colon_str_.length());
            column_ += (length+2+colon_str_.length());
            return true;
        }

        bool visit_null(semantic_tag, const ser_context&, std::error_code&) final
        {
            if (!stack_.empty()) 
            {
                if (stack_.back().is_array())
                {
                    begin_scalar_value();
                }
                if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
                {
                    break_line();
                }
            }

            sink_.append(null_constant().data(), null_constant().size());
            column_ += null_constant().size();

            end_value();
            return true;
        }

        bool visit_string(const string_view_type& sv, semantic_tag tag, const ser_context& context, std::error_code& ec) final
        {
            if (!stack_.empty()) 
            {
                if (stack_.back().is_array())
                {
                    begin_scalar_value();
                }
                if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
                {
                    break_line();
                }
            }
            
            write_string(sv, tag, context, ec);

            end_value();
            return true;
        }

        bool write_string(const string_view_type& sv, semantic_tag tag, const ser_context&, std::error_code&) 
        {
            switch (tag)
            {
                case semantic_tag::bigint:
                    write_bigint_value(sv);
                    break;
                case semantic_tag::bigdec:
                {
                    // output lossless number
                    if (options_.bignum_format() == bignum_format_kind::raw)
                    {
                        write_bigint_value(sv);
                break;
            }
            JSONCONS_FALLTHROUGH;
        }
                default:
                {
                    sink_.push_back('\"');
                    std::size_t length = jsoncons::detail::escape_string(sv.data(), sv.length(),options_.escape_all_non_ascii(),options_.escape_solidus(),sink_);
                    sink_.push_back('\"');
                    column_ += (length+2);
                    break;
                }
            }

            return true;
        }

        bool visit_byte_string(const byte_string_view& b, 
                                  semantic_tag tag,
                                  const ser_context&,
                                  std::error_code&) final
        {
            if (!stack_.empty()) 
            {
                if (stack_.back().is_array())
                {
                    begin_scalar_value();
                }
                if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
                {
                    break_line();
                }
            }

            byte_string_chars_format encoding_hint;
            switch (tag)
            {
                case semantic_tag::base16:
                    encoding_hint = byte_string_chars_format::base16;
                    break;
                case semantic_tag::base64:
                    encoding_hint = byte_string_chars_format::base64;
                    break;
                case semantic_tag::base64url:
                    encoding_hint = byte_string_chars_format::base64url;
                    break;
                default:
                    encoding_hint = byte_string_chars_format::none;
                    break;
            }

            byte_string_chars_format format = jsoncons::detail::resolve_byte_string_chars_format(options_.byte_string_format(), 
                                                                                                 encoding_hint, 
                                                                                                 byte_string_chars_format::base64url);
            switch (format)
            {
                case byte_string_chars_format::base16:
                {
                    sink_.push_back('\"');
                    std::size_t length = encode_base16(b.begin(),b.end(),sink_);
                    sink_.push_back('\"');
                    column_ += (length + 2);
                    break;
                }
                case byte_string_chars_format::base64:
                {
                    sink_.push_back('\"');
                    std::size_t length = encode_base64(b.begin(), b.end(), sink_);
                    sink_.push_back('\"');
                    column_ += (length + 2);
                    break;
                }
                case byte_string_chars_format::base64url:
                {
                    sink_.push_back('\"');
                    std::size_t length = encode_base64url(b.begin(),b.end(),sink_);
                    sink_.push_back('\"');
                    column_ += (length + 2);
                    break;
                }
                default:
                {
                    JSONCONS_UNREACHABLE();
                }
            }

            end_value();
            return true;
        }

        bool visit_double(double value, 
                             semantic_tag,
                             const ser_context& context,
                             std::error_code& ec) final
        {
            if (!stack_.empty()) 
            {
                if (stack_.back().is_array())
                {
                    begin_scalar_value();
                }
                if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
                {
                    break_line();
                }
            }

            if (!std::isfinite(value))
            {
                if ((std::isnan)(value))
                {
                    if (options_.enable_nan_to_num())
                    {
                        sink_.append(options_.nan_to_num().data(), options_.nan_to_num().length());
                        column_ += options_.nan_to_num().length();
                    }
                    else if (options_.enable_nan_to_str())
                    {
                        write_string(options_.nan_to_str(), semantic_tag::none, context, ec);
                    }
                    else
                    {
                        sink_.append(null_constant().data(), null_constant().size());
                        column_ += null_constant().size();
                    }
                }
                else if (value == std::numeric_limits<double>::infinity())
                {
                    if (options_.enable_inf_to_num())
                    {
                        sink_.append(options_.inf_to_num().data(), options_.inf_to_num().length());
                        column_ += options_.inf_to_num().length();
                    }
                    else if (options_.enable_inf_to_str())
                    {
                        write_string(options_.inf_to_str(), semantic_tag::none, context, ec);
                    }
                    else
                    {
                        sink_.append(null_constant().data(), null_constant().size());
                        column_ += null_constant().size();
                    }
                }
                else
                {
                    if (options_.enable_neginf_to_num())
                    {
                        sink_.append(options_.neginf_to_num().data(), options_.neginf_to_num().length());
                        column_ += options_.neginf_to_num().length();
                    }
                    else if (options_.enable_neginf_to_str())
                    {
                        write_string(options_.neginf_to_str(), semantic_tag::none, context, ec);
                    }
                    else
                    {
                        sink_.append(null_constant().data(), null_constant().size());
                        column_ += null_constant().size();
                    }
                }
            }
            else
            {
                std::size_t length = fp_(value, sink_);
                column_ += length;
            }

            end_value();
            return true;
        }

        bool visit_int64(int64_t value, 
                            semantic_tag,
                            const ser_context&,
                            std::error_code&) final
        {
            if (!stack_.empty()) 
            {
                if (stack_.back().is_array())
                {
                    begin_scalar_value();
                }
                if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
                {
                    break_line();
                }
            }
            std::size_t length = jsoncons::detail::from_integer(value, sink_);
            column_ += length;
            end_value();
            return true;
        }

        bool visit_uint64(uint64_t value, 
                             semantic_tag, 
                             const ser_context&,
                             std::error_code&) final
        {
            if (!stack_.empty()) 
            {
                if (stack_.back().is_array())
                {
                    begin_scalar_value();
                }
                if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
                {
                    break_line();
                }
            }
            std::size_t length = jsoncons::detail::from_integer(value, sink_);
            column_ += length;
            end_value();
            return true;
        }

        bool visit_bool(bool value, semantic_tag, const ser_context&, std::error_code&) final
        {
            if (!stack_.empty()) 
            {
                if (stack_.back().is_array())
                {
                    begin_scalar_value();
                }
                if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
                {
                    break_line();
                }
            }

            if (value)
            {
                sink_.append(true_constant().data(), true_constant().size());
                column_ += true_constant().size();
            }
            else
            {
                sink_.append(false_constant().data(), false_constant().size());
                column_ += false_constant().size();
            }

            end_value();
            return true;
        }

        void begin_scalar_value()
        {
            if (!stack_.empty())
            {
                if (stack_.back().count() > 0)
                {
                    sink_.append(comma_str_.data(),comma_str_.length());
                    column_ += comma_str_.length();
                }
                if (stack_.back().is_multi_line() || stack_.back().is_indent_once())
                {
                    stack_.back().new_line_after(true);
                    new_line();
                }
            }
        }

        void write_bigint_value(const string_view_type& sv)
        {
            switch (options_.bignum_format())
            {
                case bignum_format_kind::raw:
                {
                    sink_.append(sv.data(),sv.size());
                    column_ += sv.size();
                    break;
                }
                case bignum_format_kind::base64:
                {
                    bigint n = bigint::from_string(sv.data(), sv.length());
                    bool is_neg = n < 0;
                    if (is_neg)
                    {
                        n = - n -1;
                    }
                    int signum;
                    std::vector<uint8_t> v;
                    n.write_bytes_be(signum, v);

                    sink_.push_back('\"');
                    if (is_neg)
                    {
                        sink_.push_back('~');
                        ++column_;
                    }
                    std::size_t length = encode_base64(v.begin(), v.end(), sink_);
                    sink_.push_back('\"');
                    column_ += (length+2);
                    break;
                }
                case bignum_format_kind::base64url:
                {
                    bigint n = bigint::from_string(sv.data(), sv.length());
                    bool is_neg = n < 0;
                    if (is_neg)
                    {
                        n = - n -1;
                    }
                    int signum;
                    std::vector<uint8_t> v;
                    n.write_bytes_be(signum, v);

                    sink_.push_back('\"');
                    if (is_neg)
                    {
                        sink_.push_back('~');
                        ++column_;
                    }
                    std::size_t length = encode_base64url(v.begin(), v.end(), sink_);
                    sink_.push_back('\"');
                    column_ += (length+2);
                    break;
                }
                default:
                {
                    sink_.push_back('\"');
                    sink_.append(sv.data(),sv.size());
                    sink_.push_back('\"');
                    column_ += (sv.size() + 2);
                    break;
                }
            }
        }

        void end_value()
        {
            if (!stack_.empty())
            {
                stack_.back().increment_count();
            }
        }

        void indent()
        {
            indent_amount_ += static_cast<int>(options_.indent_size());
        }

        void unindent()
        {
            indent_amount_ -= static_cast<int>(options_.indent_size());
        }

        void new_line()
        {
            sink_.append(options_.new_line_chars().data(),options_.new_line_chars().length());
            for (int i = 0; i < indent_amount_; ++i)
            {
                sink_.push_back(' ');
            }
            column_ = indent_amount_;
        }

        void new_line(std::size_t len)
        {
            sink_.append(options_.new_line_chars().data(),options_.new_line_chars().length());
            for (std::size_t i = 0; i < len; ++i)
            {
                sink_.push_back(' ');
            }
            column_ = len;
        }

        void break_line()
        {
            stack_.back().new_line_after(true);
            new_line();
        }
    };

    template <typename CharT,typename Sink=jsoncons::stream_sink<CharT>,typename Allocator=std::allocator<char>>
    class basic_compact_json_encoder final : public basic_json_visitor<CharT>
    {
        static const std::array<CharT, 4>& null_constant()
        {
            static constexpr std::array<CharT,4> k{{'n','u','l','l'}};
            return k;
        }
        static const std::array<CharT, 4>& true_constant()
        {
            static constexpr std::array<CharT,4> k{{'t','r','u','e'}};
            return k;
        }
        static const std::array<CharT, 5>& false_constant()
        {
            static constexpr std::array<CharT,5> k{{'f','a','l','s','e'}};
            return k;
        }
    public:
        using allocator_type = Allocator;
        using char_type = CharT;
        using typename basic_json_visitor<CharT>::string_view_type;
        using sink_type = Sink;
        using string_type = typename basic_json_encode_options<CharT>::string_type;

    private:
        enum class container_type {object, array};

        class encoding_context
        {
            container_type type_;
            std::size_t count_;
        public:
            encoding_context(container_type type) noexcept
               : type_(type), count_(0)
            {
            }

            std::size_t count() const
            {
                return count_;
            }

            void increment_count()
            {
                ++count_;
            }

            bool is_array() const
            {
                return type_ == container_type::array;
            }
        };
        using encoding_context_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<encoding_context>;

        Sink sink_;
        basic_json_encode_options<CharT> options_;
        jsoncons::detail::write_double fp_;
        std::vector<encoding_context,encoding_context_allocator_type> stack_;
        int nesting_depth_;
    public:

        // Noncopyable and nonmoveable
        basic_compact_json_encoder(const basic_compact_json_encoder&) = delete;
        basic_compact_json_encoder(basic_compact_json_encoder&&) = delete;

        basic_compact_json_encoder(Sink&& sink, 
            const Allocator& alloc = Allocator())
            : basic_compact_json_encoder(std::forward<Sink>(sink), basic_json_encode_options<CharT>(), alloc)
        {
        }

        basic_compact_json_encoder(Sink&& sink, 
            const basic_json_encode_options<CharT>& options, 
            const Allocator& alloc = Allocator())
           : sink_(std::forward<Sink>(sink)),
             options_(options),
             fp_(options.float_format(), options.precision()),
             stack_(alloc),
             nesting_depth_(0)          
        {
        }

        ~basic_compact_json_encoder() noexcept
        {
            JSONCONS_TRY
            {
                sink_.flush();
            }
            JSONCONS_CATCH(...)
            {
            }
        }

        basic_compact_json_encoder& operator=(const basic_compact_json_encoder&) = delete;
        basic_compact_json_encoder& operator=(basic_compact_json_encoder&&) = delete;

        void reset()
        {
            stack_.clear();
            nesting_depth_ = 0;
        }

        void reset(Sink&& sink)
        {
            sink_ = std::move(sink);
            reset();
        }

    private:
        // Implementing methods
        void visit_flush() final
        {
            sink_.flush();
        }

        bool visit_begin_object(semantic_tag, const ser_context&, std::error_code& ec) final
        {
            if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
            {
                ec = json_errc::max_nesting_depth_exceeded;
                return false;
            } 
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.push_back(',');
            }

            stack_.emplace_back(container_type::object);
            sink_.push_back('{');
            return true;
        }

        bool visit_end_object(const ser_context&, std::error_code&) final
        {
            JSONCONS_ASSERT(!stack_.empty());
            --nesting_depth_;

            stack_.pop_back();
            sink_.push_back('}');

            if (!stack_.empty())
            {
                stack_.back().increment_count();
            }
            return true;
        }


        bool visit_begin_array(semantic_tag, const ser_context&, std::error_code& ec) final
        {
            if (JSONCONS_UNLIKELY(++nesting_depth_ > options_.max_nesting_depth()))
            {
                ec = json_errc::max_nesting_depth_exceeded;
                return false;
            } 
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.push_back(',');
            }
            stack_.emplace_back(container_type::array);
            sink_.push_back('[');
            return true;
        }

        bool visit_end_array(const ser_context&, std::error_code&) final
        {
            JSONCONS_ASSERT(!stack_.empty());
            --nesting_depth_;

            stack_.pop_back();
            sink_.push_back(']');
            if (!stack_.empty())
            {
                stack_.back().increment_count();
            }
            return true;
        }

        bool visit_key(const string_view_type& name, const ser_context&, std::error_code&) final
        {
            if (!stack_.empty() && stack_.back().count() > 0)
            {
                sink_.push_back(',');
            }

            sink_.push_back('\"');
            jsoncons::detail::escape_string(name.data(), name.length(),options_.escape_all_non_ascii(),options_.escape_solidus(),sink_);
            sink_.push_back('\"');
            sink_.push_back(':');
            return true;
        }

        bool visit_null(semantic_tag, const ser_context&, std::error_code&) final
        {
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.push_back(',');
            }

            sink_.append(null_constant().data(), null_constant().size());

            if (!stack_.empty())
            {
                stack_.back().increment_count();
            }
            return true;
        }

        void write_bigint_value(const string_view_type& sv)
        {
            switch (options_.bignum_format())
            {
                case bignum_format_kind::raw:
                {
                    sink_.append(sv.data(),sv.size());
                    break;
                }
                case bignum_format_kind::base64:
                {
                    bigint n = bigint::from_string(sv.data(), sv.length());
                    bool is_neg = n < 0;
                    if (is_neg)
                    {
                        n = - n -1;
                    }
                    int signum;
                    std::vector<uint8_t> v;
                    n.write_bytes_be(signum, v);

                    sink_.push_back('\"');
                    if (is_neg)
                    {
                        sink_.push_back('~');
                    }
                    encode_base64(v.begin(), v.end(), sink_);
                    sink_.push_back('\"');
                    break;
                }
                case bignum_format_kind::base64url:
                {
                    bigint n = bigint::from_string(sv.data(), sv.length());
                    bool is_neg = n < 0;
                    if (is_neg)
                    {
                        n = - n -1;
                    }
                    int signum;
                    std::vector<uint8_t> v;
                    n.write_bytes_be(signum, v);

                    sink_.push_back('\"');
                    if (is_neg)
                    {
                        sink_.push_back('~');
                    }
                    encode_base64url(v.begin(), v.end(), sink_);
                    sink_.push_back('\"');
                    break;
                }
                default:
                {
                    sink_.push_back('\"');
                    sink_.append(sv.data(),sv.size());
                    sink_.push_back('\"');
                    break;
                }
            }
        }

        bool visit_string(const string_view_type& sv, semantic_tag tag, const ser_context&, std::error_code&) final
        {
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.push_back(',');
            }

            switch (tag)
            {
                case semantic_tag::bigint:
                    write_bigint_value(sv);
                    break;
                case semantic_tag::bigdec:
                {
                    // output lossless number
                    if (options_.bignum_format() == bignum_format_kind::raw)
                    {
                        write_bigint_value(sv);
                        break;
            }
            JSONCONS_FALLTHROUGH;
        }
                default:
                {
                    sink_.push_back('\"');
                    jsoncons::detail::escape_string(sv.data(), sv.length(),options_.escape_all_non_ascii(),options_.escape_solidus(),sink_);
                    sink_.push_back('\"');
                    break;
                }
            }

            if (!stack_.empty())
            {
                stack_.back().increment_count();
            }
            return true;
        }

        bool write_string(const string_view_type& sv, semantic_tag tag, const ser_context&, std::error_code&) 
        {
            switch (tag)
            {
                case semantic_tag::bigint:
                    write_bigint_value(sv);
                    break;
                case semantic_tag::bigdec:
                {
                    // output lossless number
                    if (options_.bignum_format() == bignum_format_kind::raw)
                    {
                        write_bigint_value(sv);
                        break;
            }
            JSONCONS_FALLTHROUGH;
        }
                default:
                {
                    sink_.push_back('\"');
                    jsoncons::detail::escape_string(sv.data(), sv.length(),options_.escape_all_non_ascii(),options_.escape_solidus(),sink_);
                    sink_.push_back('\"');
                    break;
                }
            }
            return true;
        }

        bool visit_byte_string(const byte_string_view& b, 
                                  semantic_tag tag,
                                  const ser_context&,
                                  std::error_code&) final
        {
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.push_back(',');
            }

            byte_string_chars_format encoding_hint;
            switch (tag)
            {
                case semantic_tag::base16:
                    encoding_hint = byte_string_chars_format::base16;
                    break;
                case semantic_tag::base64:
                    encoding_hint = byte_string_chars_format::base64;
                    break;
                case semantic_tag::base64url:
                    encoding_hint = byte_string_chars_format::base64url;
                    break;
                default:
                    encoding_hint = byte_string_chars_format::none;
                    break;
            }

            byte_string_chars_format format = jsoncons::detail::resolve_byte_string_chars_format(options_.byte_string_format(), 
                                                                                       encoding_hint, 
                                                                                       byte_string_chars_format::base64url);
            switch (format)
            {
                case byte_string_chars_format::base16:
                {
                    sink_.push_back('\"');
                    encode_base16(b.begin(),b.end(),sink_);
                    sink_.push_back('\"');
                    break;
                }
                case byte_string_chars_format::base64:
                {
                    sink_.push_back('\"');
                    encode_base64(b.begin(), b.end(), sink_);
                    sink_.push_back('\"');
                    break;
                }
                case byte_string_chars_format::base64url:
                {
                    sink_.push_back('\"');
                    encode_base64url(b.begin(),b.end(),sink_);
                    sink_.push_back('\"');
                    break;
                }
                default:
                {
                    JSONCONS_UNREACHABLE();
                }
            }

            if (!stack_.empty())
            {
                stack_.back().increment_count();
            }
            return true;
        }

        bool visit_double(double value, 
                             semantic_tag,
                             const ser_context& context,
                             std::error_code& ec) final
        {
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.push_back(',');
            }

            if (JSONCONS_UNLIKELY(!std::isfinite(value)))
            {
                if ((std::isnan)(value))
                {
                    if (options_.enable_nan_to_num())
                    {
                        sink_.append(options_.nan_to_num().data(), options_.nan_to_num().length());
                    }
                    else if (options_.enable_nan_to_str())
                    {
                        write_string(options_.nan_to_str(), semantic_tag::none, context, ec);
                    }
                    else
                    {
                        sink_.append(null_constant().data(), null_constant().size());
                    }
                }
                else if (value == std::numeric_limits<double>::infinity())
                {
                    if (options_.enable_inf_to_num())
                    {
                        sink_.append(options_.inf_to_num().data(), options_.inf_to_num().length());
                    }
                    else if (options_.enable_inf_to_str())
                    {
                        write_string(options_.inf_to_str(), semantic_tag::none, context, ec);
                    }
                    else
                    {
                        sink_.append(null_constant().data(), null_constant().size());
                    }
                }
                else 
                {
                    if (options_.enable_neginf_to_num())
                    {
                        sink_.append(options_.neginf_to_num().data(), options_.neginf_to_num().length());
                    }
                    else if (options_.enable_neginf_to_str())
                    {
                        write_string(options_.neginf_to_str(), semantic_tag::none, context, ec);
                    }
                    else
                    {
                        sink_.append(null_constant().data(), null_constant().size());
                    }
                }
            }
            else
            {
                fp_(value, sink_);
            }

            if (!stack_.empty())
            {
                stack_.back().increment_count();
            }
            return true;
        }

        bool visit_int64(int64_t value, 
                            semantic_tag,
                            const ser_context&,
                            std::error_code&) final
        {
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.push_back(',');
            }
            jsoncons::detail::from_integer(value, sink_);
            if (!stack_.empty())
            {
                stack_.back().increment_count();
            }
            return true;
        }

        bool visit_uint64(uint64_t value, 
                             semantic_tag, 
                             const ser_context&,
                             std::error_code&) final
        {
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.push_back(',');
            }
            jsoncons::detail::from_integer(value, sink_);
            if (!stack_.empty())
            {
                stack_.back().increment_count();
            }
            return true;
        }

        bool visit_bool(bool value, semantic_tag, const ser_context&, std::error_code&) final
        {
            if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
            {
                sink_.push_back(',');
            }

            if (value)
            {
                sink_.append(true_constant().data(), true_constant().size());
            }
            else
            {
                sink_.append(false_constant().data(), false_constant().size());
            }

            if (!stack_.empty())
            {
                stack_.back().increment_count();
            }
            return true;
        }
    };

    using json_stream_encoder = basic_json_encoder<char,jsoncons::stream_sink<char>>;
    using wjson_stream_encoder = basic_json_encoder<wchar_t,jsoncons::stream_sink<wchar_t>>;
    using compact_json_stream_encoder = basic_compact_json_encoder<char,jsoncons::stream_sink<char>>;
    using compact_wjson_stream_encoder = basic_compact_json_encoder<wchar_t,jsoncons::stream_sink<wchar_t>>;

    using json_string_encoder = basic_json_encoder<char,jsoncons::string_sink<std::string>>;
    using wjson_string_encoder = basic_json_encoder<wchar_t,jsoncons::string_sink<std::wstring>>;
    using compact_json_string_encoder = basic_compact_json_encoder<char,jsoncons::string_sink<std::string>>;
    using compact_wjson_string_encoder = basic_compact_json_encoder<wchar_t,jsoncons::string_sink<std::wstring>>;

} // namespace jsoncons

#endif // JSONCONS_JSON_ENCODER_HPP
