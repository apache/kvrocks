// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CSV_CSV_OPTIONS_HPP
#define JSONCONS_EXT_CSV_CSV_OPTIONS_HPP

#include <cstdint>
#include <cwchar>
#include <limits> // std::numeric_limits
#include <map>
#include <string>
#include <unordered_map> // std::unordered_map
#include <utility> // std::pair

#include <jsoncons/json_options.hpp>

namespace jsoncons { namespace csv {

enum class csv_column_type : uint8_t 
{
    string_t,integer_t,float_t,boolean_t,repeat_t
};

enum class quote_style_kind : uint8_t 
{
    minimal,all,nonnumeric,none
};

enum class csv_mapping_kind : uint8_t 
{
    n_rows = 1, 
    n_objects, 
    m_columns
};

enum class column_state {sequence,label};

struct csv_type_info
{
    csv_type_info() = default;
    csv_type_info(const csv_type_info&) = default;
    csv_type_info(csv_type_info&&) = default;

    csv_type_info(csv_column_type ctype, std::size_t lev, std::size_t repcount = 0) noexcept
    {
        col_type = ctype;
        level = lev;
        rep_count = repcount;
    }

    csv_column_type col_type;
    std::size_t level;
    std::size_t rep_count;
};

namespace detail {

template <typename CharT,typename Container>
void parse_column_names(const std::basic_string<CharT>& names, 
                        Container& cont)
{
    column_state state = column_state::sequence;
    typename Container::value_type buffer(cont.get_allocator());

    auto p = names.begin();
    while (p != names.end())
    {
        switch (state)
        {
            case column_state::sequence:
            {
                switch (*p)
                {
                    case ' ': case '\t':case '\r': case '\n':
                        ++p;
                        break;
                    default:
                        buffer.clear();
                        state = column_state::label;
                        break;
                }
                break;
            }
            case column_state::label:
            {
                switch (*p)
                {
                case ',':
                    cont.push_back(buffer);
                    buffer.clear();
                    ++p;
                    state = column_state::sequence;
                    break;
                default:
                    buffer.push_back(*p);
                    ++p;
                    break;
                }
                break;
            }
        }
    }
    if (state == column_state::label)
    {
        cont.push_back(buffer);
        buffer.clear();
    }

} // namespace detail

template <typename CharT,typename Container>
void parse_column_types(const std::basic_string<CharT>& types, 
                        Container& column_types)
{
    const std::map<jsoncons::basic_string_view<CharT>,csv_column_type> type_dictionary =
    {

        {JSONCONS_STRING_VIEW_CONSTANT(CharT,"string"),csv_column_type::string_t},
        {JSONCONS_STRING_VIEW_CONSTANT(CharT,"integer"),csv_column_type::integer_t},
        {JSONCONS_STRING_VIEW_CONSTANT(CharT,"float"),csv_column_type::float_t},
        {JSONCONS_STRING_VIEW_CONSTANT(CharT,"boolean"),csv_column_type::boolean_t}
    };

    column_state state = column_state::sequence;
    int depth = 0;
    std::basic_string<CharT> buffer;

    auto p = types.begin();
    while (p != types.end())
    {
        switch (state)
        {
            case column_state::sequence:
            {
                switch (*p)
                {
                case ' ': case '\t':case '\r': case '\n':
                    ++p;
                    break;
                case '[':
                    ++depth;
                    ++p;
                    break;
                case ']':
                    JSONCONS_ASSERT(depth > 0);
                    --depth;
                    ++p;
                    break;
                case '*':
                    {
                        JSONCONS_ASSERT(column_types.size() != 0);
                        std::size_t offset = 0;
                        std::size_t level = column_types.size() > 0 ? column_types.back().level: 0;
                        if (level > 0)
                        {
                            for (auto it = column_types.rbegin();
                                 it != column_types.rend() && level == (*it).level;
                                 ++it)
                            {
                                ++offset;
                            }
                        }
                        else
                        {
                            offset = 1;
                        }
                        column_types.emplace_back(csv_column_type::repeat_t,depth,offset);
                        ++p;
                        break;
                    }
                default:
                    buffer.clear();
                    state = column_state::label;
                    break;
                }
                break;
            }
            case column_state::label:
            {
                switch (*p)
                {
                    case '*':
                    {
                        auto it = type_dictionary.find(buffer);
                        if (it != type_dictionary.end())
                        {
                            column_types.emplace_back((*it).second,depth);
                            buffer.clear();
                        }
                        else
                        {
                            JSONCONS_ASSERT(false);
                        }
                        state = column_state::sequence;
                        break;
                    }
                    case ',':
                    {
                        auto it = type_dictionary.find(buffer);
                        if (it != type_dictionary.end())
                        {
                            column_types.emplace_back((*it).second,depth);
                            buffer.clear();
                        }
                        else
                        {
                            JSONCONS_ASSERT(false);
                        }
                        ++p;
                        state = column_state::sequence;
                        break;
                    }
                    case ']':
                    {
                        JSONCONS_ASSERT(depth > 0);
                        auto it = type_dictionary.find(buffer);
                        if (it != type_dictionary.end())
                        {
                            column_types.emplace_back((*it).second,depth);
                            buffer.clear();
                        }
                        else
                        {
                            JSONCONS_ASSERT(false);
                        }
                        --depth;
                        ++p;
                        state = column_state::sequence;
                        break;
                    }
                    default:
                    {
                        buffer.push_back(*p);
                        ++p;
                        break;
                    }
                }
                break;
            }
        }
    }
    if (state == column_state::label)
    {
        auto it = type_dictionary.find(buffer);
        if (it != type_dictionary.end())
        {
            column_types.emplace_back((*it).second,depth);
            buffer.clear();
        }
        else
        {
            JSONCONS_ASSERT(false);
        }
    }
}

} // namespace detail

template <typename CharT>
class basic_csv_options;

template <typename CharT>
class basic_csv_options_common 
{
    friend class basic_csv_options<CharT>;
public:
    using char_type = CharT;
    using string_type = std::basic_string<CharT>;
private:
    char_type field_delimiter_{','};
    char_type quote_char_{'\"'};
    char_type quote_escape_char_{'\"'};
    char_type subfield_delimiter_{char_type{}};

    bool flat_:1;
    bool enable_nan_to_num_:1;
    bool enable_inf_to_num_:1;
    bool enable_neginf_to_num_:1;
    bool enable_nan_to_str_:1;
    bool enable_inf_to_str_:1;
    bool enable_neginf_to_str_:1;
    bool enable_str_to_nan_:1;
    bool enable_str_to_inf_:1;
    bool enable_str_to_neginf_:1;

    string_type nan_to_num_;
    string_type inf_to_num_;
    string_type neginf_to_num_;
    string_type nan_to_str_;
    string_type inf_to_str_;
    string_type neginf_to_str_;
    string_type column_names_;
    std::vector<std::pair<std::string,std::string>> column_mapping_; 
    std::size_t max_nesting_depth_{1024};

protected:
    basic_csv_options_common()
      : flat_{true},                  
        enable_nan_to_num_{false},    
        enable_inf_to_num_{false},    
        enable_neginf_to_num_{false}, 
        enable_nan_to_str_{false},    
        enable_inf_to_str_{false},    
        enable_neginf_to_str_{false}, 
        enable_str_to_nan_{false},    
        enable_str_to_inf_{false},    
        enable_str_to_neginf_{false}
    {
    }

    basic_csv_options_common(const basic_csv_options_common&) = default;
    basic_csv_options_common& operator=(const basic_csv_options_common&) = default;
    //basic_csv_options_common& operator=(basic_csv_options_common&&) = default;

    virtual ~basic_csv_options_common() = default;
public:

    bool flat() const 
    {
        return flat_;
    }

    std::size_t max_nesting_depth() const 
    {
        return max_nesting_depth_;
    }

    char_type field_delimiter() const 
    {
        return field_delimiter_;
    }

    const char_type subfield_delimiter() const 
    {
        return subfield_delimiter_;
    }

    char_type quote_char() const 
    {
        return quote_char_;
    }

    char_type quote_escape_char() const 
    {
        return quote_escape_char_;
    }

    const string_type& column_names() const 
    {
        return column_names_;
    }

    const std::vector<std::pair<std::string,std::string>>& column_mapping() const 
    {
        return column_mapping_;
    }

    bool enable_nan_to_num() const
    {
        return enable_nan_to_num_;
    }

    bool enable_inf_to_num() const
    {
        return enable_inf_to_num_;
    }

    bool enable_neginf_to_num() const
    {
        return enable_neginf_to_num_ || enable_inf_to_num_;
    }

    bool enable_nan_to_str() const
    {
        return enable_nan_to_str_;
    }

    bool enable_str_to_nan() const
    {
        return enable_str_to_nan_;
    }

    bool enable_inf_to_str() const
    {
        return enable_inf_to_str_;
    }

    bool enable_str_to_inf() const
    {
        return enable_str_to_inf_;
    }

    bool enable_neginf_to_str() const
    {
        return enable_neginf_to_str_ || enable_inf_to_str_;
    }

    bool enable_str_to_neginf() const
    {
        return enable_str_to_neginf_ || enable_str_to_inf_;
    }

    string_type nan_to_num() const
    {
        return nan_to_num_; 
    }

    string_type inf_to_num() const
    {
        return inf_to_num_; 
    }

    string_type neginf_to_num() const
    {
        if (enable_neginf_to_num_)
        {
            return neginf_to_num_;
        }
        else if (enable_inf_to_num_)
        {
            string_type s;
            s.push_back('-');
            s.append(inf_to_num_);
            return s;
        }
        else
        {
            return neginf_to_num_; 
        }
    }

    string_type nan_to_str() const
    {
        return nan_to_str_;
    }

    string_type inf_to_str() const
    {
        return inf_to_str_; 
    }

    string_type neginf_to_str() const
    {
        if (enable_neginf_to_str_)
        {
            return neginf_to_str_;
        }
        else if (enable_inf_to_str_)
        {
            string_type s;
            s.push_back('-');
            s.append(inf_to_str_);
            return s;
        }
        else
        {
            return neginf_to_str_; // empty string
        }
    }
};

template <typename CharT>
class basic_csv_decode_options : public virtual basic_csv_options_common<CharT>
{
    friend class basic_csv_options<CharT>;
    using super_type = basic_csv_options_common<CharT>;
public:
    using typename super_type::char_type;
    using typename super_type::string_type;

private:
    bool assume_header_:1;
    bool ignore_empty_values_:1;
    bool ignore_empty_lines_:1;
    bool trim_leading_:1;
    bool trim_trailing_:1;
    bool trim_leading_inside_quotes_:1;
    bool trim_trailing_inside_quotes_:1;
    bool unquoted_empty_value_is_null_:1;
    bool infer_types_:1;
    bool lossless_number_:1;
    char_type comment_starter_{'\0'};
    csv_mapping_kind mapping_{};
    std::size_t header_lines_{0};
    std::size_t max_lines_{(std::numeric_limits<std::size_t>::max)()};
    string_type column_types_;
    string_type column_defaults_;
public:
    basic_csv_decode_options()
        : assume_header_(false),
          ignore_empty_values_(false),
          ignore_empty_lines_(true),
          trim_leading_(false),
          trim_trailing_(false),
          trim_leading_inside_quotes_(false),
          trim_trailing_inside_quotes_(false),
          unquoted_empty_value_is_null_(false),
          infer_types_(true),
          lossless_number_(false)
    {}

    basic_csv_decode_options(const basic_csv_decode_options& other) = default;

    basic_csv_decode_options(basic_csv_decode_options&& other) noexcept
        : super_type(std::move(other)),
          assume_header_(other.assume_header_),
          ignore_empty_values_(other.ignore_empty_values_),
          ignore_empty_lines_(other.ignore_empty_lines_),
          trim_leading_(other.trim_leading_),
          trim_trailing_(other.trim_trailing_),
          trim_leading_inside_quotes_(other.trim_leading_inside_quotes_),
          trim_trailing_inside_quotes_(other.trim_trailing_inside_quotes_),
          unquoted_empty_value_is_null_(other.unquoted_empty_value_is_null_),
          infer_types_(other.infer_types_),
          lossless_number_(other.lossless_number_),
          comment_starter_(other.comment_starter_),
          mapping_(other.mapping_),
          header_lines_(other.header_lines_),
          max_lines_(other.max_lines_),
          column_types_(std::move(other.column_types_)),
          column_defaults_(std::move(other.column_defaults_))
    {}
    
protected:
    basic_csv_decode_options& operator=(const basic_csv_decode_options& other) = default;
    basic_csv_decode_options& operator=(basic_csv_decode_options&& other) = default;
public:

    std::size_t header_lines() const 
    {
        return (assume_header_ && header_lines_ <= 1) ? 1 : header_lines_;
    }

    bool assume_header() const 
    {
        return assume_header_;
    }

    bool ignore_empty_values() const 
    {
        return ignore_empty_values_;
    }

    bool ignore_empty_lines() const 
    {
        return ignore_empty_lines_;
    }

    bool trim_leading() const 
    {
        return trim_leading_;
    }

    bool trim_trailing() const 
    {
        return trim_trailing_;
    }

    bool trim_leading_inside_quotes() const 
    {
        return trim_leading_inside_quotes_;
    }

    bool trim_trailing_inside_quotes() const 
    {
        return trim_trailing_inside_quotes_;
    }

    bool trim() const 
    {
        return trim_leading_ && trim_trailing_;
    }

    bool trim_inside_quotes() const 
    {
        return trim_leading_inside_quotes_ && trim_trailing_inside_quotes_;
    }

    bool unquoted_empty_value_is_null() const 
    {
        return unquoted_empty_value_is_null_;
    }

    bool infer_types() const 
    {
        return infer_types_;
    }

    bool lossless_number() const 
    {
        return lossless_number_;
    }

    char_type comment_starter() const 
    {
        return comment_starter_;
    }

    csv_mapping_kind mapping_kind() const 
    {
        return mapping_ != csv_mapping_kind() ? mapping_ : (assume_header() || this->column_names().size() > 0 ? csv_mapping_kind::n_objects : csv_mapping_kind::n_rows);
    }

    std::size_t max_lines() const 
    {
        return max_lines_;
    }

    string_type column_types() const 
    {
        return column_types_;
    }

    string_type column_defaults() const 
    {
        return column_defaults_;
    }
};

template <typename CharT>
class basic_csv_encode_options : public virtual basic_csv_options_common<CharT>
{
    friend class basic_csv_options<CharT>;
    using super_type = basic_csv_options_common<CharT>;
public:
    using typename super_type::char_type;
    using typename super_type::string_type;
private:
    quote_style_kind quote_style_{quote_style_kind::minimal};
    float_chars_format float_format_{float_chars_format::general};
    int8_t precision_{0};
    string_type line_delimiter_;
public:
    basic_csv_encode_options()
    {
        line_delimiter_.push_back('\n');
    }

    basic_csv_encode_options(const basic_csv_encode_options& other) = default;

    basic_csv_encode_options(basic_csv_encode_options&& other) noexcept
        : super_type(std::move(other)),
          quote_style_(other.quote_style_),
          float_format_(other.float_format_),
          precision_(other.precision_),
          line_delimiter_(std::move(other.line_delimiter_))
    {
    }
    
protected:
    basic_csv_encode_options& operator=(const basic_csv_encode_options& other) = default;
    basic_csv_encode_options& operator=(basic_csv_encode_options&& other) = default;
public:

    quote_style_kind quote_style() const 
    {
        return quote_style_;
    }

    float_chars_format float_format() const 
    {
        return float_format_;
    }

    int8_t precision() const 
    {
        return precision_;
    }

    string_type line_delimiter() const
    {
        return line_delimiter_;
    }
};

template <typename CharT>
class basic_csv_options final : public basic_csv_decode_options<CharT>, public basic_csv_encode_options<CharT>  
{
    using char_type = CharT;
    using string_type = std::basic_string<CharT>;

public:
    using basic_csv_decode_options<CharT>::enable_str_to_nan;
    using basic_csv_decode_options<CharT>::enable_str_to_inf;
    using basic_csv_decode_options<CharT>::enable_str_to_neginf;
    using basic_csv_decode_options<CharT>::nan_to_str;
    using basic_csv_decode_options<CharT>::inf_to_str;
    using basic_csv_decode_options<CharT>::neginf_to_str;
    using basic_csv_decode_options<CharT>::nan_to_num;
    using basic_csv_decode_options<CharT>::inf_to_num;
    using basic_csv_decode_options<CharT>::neginf_to_num;
    using basic_csv_decode_options<CharT>::flat;
    using basic_csv_decode_options<CharT>::max_nesting_depth;
    using basic_csv_decode_options<CharT>::field_delimiter;
    using basic_csv_decode_options<CharT>::subfield_delimiter;
    using basic_csv_decode_options<CharT>::quote_char;
    using basic_csv_decode_options<CharT>::quote_escape_char;
    using basic_csv_decode_options<CharT>::column_names;
    using basic_csv_decode_options<CharT>::column_mapping;
    using basic_csv_decode_options<CharT>::header_lines; 
    using basic_csv_decode_options<CharT>::assume_header; 
    using basic_csv_decode_options<CharT>::ignore_empty_values; 
    using basic_csv_decode_options<CharT>::ignore_empty_lines; 
    using basic_csv_decode_options<CharT>::trim_leading; 
    using basic_csv_decode_options<CharT>::trim_trailing; 
    using basic_csv_decode_options<CharT>::trim_leading_inside_quotes; 
    using basic_csv_decode_options<CharT>::trim_trailing_inside_quotes; 
    using basic_csv_decode_options<CharT>::trim; 
    using basic_csv_decode_options<CharT>::trim_inside_quotes; 
    using basic_csv_decode_options<CharT>::unquoted_empty_value_is_null; 
    using basic_csv_decode_options<CharT>::infer_types; 
    using basic_csv_decode_options<CharT>::lossless_number; 
    using basic_csv_decode_options<CharT>::comment_starter; 
    using basic_csv_decode_options<CharT>::mapping_kind; 
    using basic_csv_decode_options<CharT>::max_lines; 
    using basic_csv_decode_options<CharT>::column_types; 
    using basic_csv_decode_options<CharT>::column_defaults; 
    using basic_csv_encode_options<CharT>::float_format;
    using basic_csv_encode_options<CharT>::precision;
    using basic_csv_encode_options<CharT>::line_delimiter;
    using basic_csv_encode_options<CharT>::quote_style;

    static constexpr size_t default_indent = 4;

//  Constructors

    basic_csv_options() = default;
    basic_csv_options(const basic_csv_options&) = default;
    basic_csv_options(basic_csv_options&&) = default;
    basic_csv_options& operator=(const basic_csv_options&) = default;
    basic_csv_options& operator=(basic_csv_options&&) = default;

    basic_csv_options& float_format(float_chars_format value)
    {
        this->float_format_ = value;
        return *this;
    }

    basic_csv_options& precision(int8_t value)
    {
        this->precision_ = value;
        return *this;
    }

    basic_csv_options& header_lines(std::size_t value)
    {
        this->header_lines_ = value;
        return *this;
    }

    basic_csv_options& assume_header(bool value)
    {
        this->assume_header_ = value;
        return *this;
    }

    basic_csv_options& ignore_empty_values(bool value)
    {
        this->ignore_empty_values_ = value;
        return *this;
    }

    basic_csv_options& ignore_empty_lines(bool value)
    {
        this->ignore_empty_lines_ = value;
        return *this;
    }

    basic_csv_options& trim_leading(bool value)
    {
        this->trim_leading_ = value;
        return *this;
    }

    basic_csv_options& trim_trailing(bool value)
    {
        this->trim_trailing_ = value;
        return *this;
    }

    basic_csv_options& trim_leading_inside_quotes(bool value)
    {
        this->trim_leading_inside_quotes_ = value;
        return *this;
    }

    basic_csv_options& trim_trailing_inside_quotes(bool value)
    {
        this->trim_trailing_inside_quotes_ = value;
        return *this;
    }

    basic_csv_options& trim(bool value)
    {
        this->trim_leading_ = value;
        this->trim_trailing_ = value;
        return *this;
    }

    basic_csv_options& trim_inside_quotes(bool value)
    {
        this->trim_leading_inside_quotes_ = value;
        this->trim_trailing_inside_quotes_ = value;
        return *this;
    }

    basic_csv_options& unquoted_empty_value_is_null(bool value)
    {
        this->unquoted_empty_value_is_null_ = value;
        return *this;
    }

    basic_csv_options& column_names(const string_type& value)
    {
        this->column_names_ = value;
        return *this;
    }

    basic_csv_options& column_mapping(const std::vector<std::pair<std::string,std::string>>& value)
    {
        this->column_mapping_ = value;
        return *this;
    }

    basic_csv_options& column_types(const string_type& value)
    {
        this->column_types_ = value;
        return *this;
    }

    basic_csv_options& column_defaults(const string_type& value)
    {
        this->column_defaults_ = value;
        return *this;
    }

    basic_csv_options& flat(bool value)
    {
        this->flat_ = value;
        return *this;
    }

    basic_csv_options& max_nesting_depth(std::size_t value)
    {
        this->max_nesting_depth_ = value;
        return *this;
    }

    basic_csv_options& field_delimiter(char_type value)
    {
        this->field_delimiter_ = value;
        return *this;
    }

    basic_csv_options& subfield_delimiter(char_type value)
    {
        this->subfield_delimiter_ = value;
        return *this;
    }

    basic_csv_options& line_delimiter(const string_type& value)
    {
        this->line_delimiter_ = value;
        return *this;
    }

    basic_csv_options& quote_char(char_type value)
    {
        this->quote_char_ = value;
        return *this;
    }

    basic_csv_options& infer_types(bool value)
    {
        this->infer_types_ = value;
        return *this;
    }

    basic_csv_options& lossless_number(bool value) 
    {
        this->lossless_number_ = value;
        return *this;
    }

    basic_csv_options& quote_escape_char(char_type value)
    {
        this->quote_escape_char_ = value;
        return *this;
    }

    basic_csv_options& comment_starter(char_type value)
    {
        this->comment_starter_ = value;
        return *this;
    }

    basic_csv_options& quote_style(quote_style_kind value)
    {
        this->quote_style_ = value;
        return *this;
    }

    basic_csv_options& mapping_kind(csv_mapping_kind value)
    {
        this->mapping_ = value;
        return *this;
    }

    basic_csv_options& max_lines(std::size_t value)
    {
        this->max_lines_ = value;
        return *this;
    }

    basic_csv_options& nan_to_num(const string_type& value)
    {
        this->enable_nan_to_num_ = true;
        this->nan_to_str_.clear();
        this->nan_to_num_ = value;
        return *this;
    }

    basic_csv_options& inf_to_num(const string_type& value)
    {
        this->enable_inf_to_num_ = true;
        this->inf_to_str_.clear();
        this->inf_to_num_ = value;
        return *this;
    }

    basic_csv_options& neginf_to_num(const string_type& value)
    {
        this->enable_neginf_to_num_ = true;
        this->neginf_to_str_.clear();
        this->neginf_to_num_ = value;
        return *this;
    }

    basic_csv_options& nan_to_str(const string_type& value, bool enable_inverse = true)
    {
        this->enable_nan_to_str_ = true;
        this->enable_str_to_nan_ = enable_inverse;
        this->nan_to_num_.clear();
        this->nan_to_str_ = value;
        return *this;
    }

    basic_csv_options& inf_to_str(const string_type& value, bool enable_inverse = true)
    {
        this->enable_inf_to_str_ = true;
        this->enable_inf_to_str_ = enable_inverse;
        this->inf_to_num_.clear();
        this->inf_to_str_ = value;
        return *this;
    }

    basic_csv_options& neginf_to_str(const string_type& value, bool enable_inverse = true)
    {
        this->enable_neginf_to_str_ = true;
        this->enable_neginf_to_str_ = enable_inverse;
        this->neginf_to_num_.clear();
        this->neginf_to_str_ = value;
        return *this;
    }

};

using csv_options = basic_csv_options<char>;
using wcsv_options = basic_csv_options<wchar_t>;

} // namespace jsonpath
} // namespace jsoncons

#endif // JSONCONS_EXT_CSV_CSV_OPTIONS_HPP
