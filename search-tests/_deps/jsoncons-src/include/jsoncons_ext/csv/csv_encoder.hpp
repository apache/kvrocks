// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_CSV_CSV_ENCODER_HPP
#define JSONCONS_EXT_CSV_CSV_ENCODER_HPP

#include <array> // std::array
#include <limits> // std::numeric_limits
#include <memory> // std::allocator
#include <ostream>
#include <string>
#include <unordered_map> // std::unordered_map
#include <utility> // std::move
#include <vector>

#include <jsoncons/detail/write_number.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_visitor.hpp>
#include <jsoncons/sink.hpp>
#include <jsoncons_ext/csv/csv_error.hpp>
#include <jsoncons_ext/csv/csv_options.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jsoncons { namespace csv {

template <typename CharT,typename Sink=jsoncons::stream_sink<CharT>,typename Allocator=std::allocator<char>>
class basic_csv_encoder final : public basic_json_visitor<CharT>
{
public:
    using char_type = CharT;
    using typename basic_json_visitor<CharT>::string_view_type;
    using sink_type = Sink;
    using allocator_type = Allocator;
    using char_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<CharT>;
    using string_type = std::basic_string<CharT, std::char_traits<CharT>, char_allocator_type>;
    using string_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<string_type>;
    using string_string_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<std::pair<const string_type,string_type>>;
    using string_vector_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<std::pair<const string_type, std::vector<string_type, string_allocator_type>>>;
    using column_type = std::vector<string_type, string_allocator_type>;
    using column_path_column_map_type = std::unordered_map<string_type, column_type, std::hash<string_type>,std::equal_to<string_type>,string_vector_allocator_type>;
private:
    static jsoncons::basic_string_view<CharT> null_constant()
    {
        static jsoncons::basic_string_view<CharT> k = JSONCONS_STRING_VIEW_CONSTANT(CharT,"null");
        return k;
    }
    static jsoncons::basic_string_view<CharT> true_constant()
    {
        static jsoncons::basic_string_view<CharT> k = JSONCONS_STRING_VIEW_CONSTANT(CharT,"true");
        return k;
    }
    static jsoncons::basic_string_view<CharT> false_constant()
    {
        static jsoncons::basic_string_view<CharT> k = JSONCONS_STRING_VIEW_CONSTANT(CharT,"false");
        return k;
    }

    enum class stack_item_kind
    {
        flat_row_mapping,
        row_mapping,
        flat_object,
        flat_row,
        stream_flat_row,
        unmapped,
        object,
        row,
        column_mapping,
        column,
        multivalued_field,
        stream_multivalued_field,
        column_multivalued_field
    };
    
    struct stack_item
    {
        stack_item_kind item_kind_;
        std::size_t count_{0};
        string_type column_path_;

        stack_item(stack_item_kind item_kind) noexcept
           : item_kind_(item_kind)
        {
        }

        stack_item_kind item_kind() const
        {
            return item_kind_;
        }
    };

    using stack_item_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<stack_item>;

    static const stack_item& parent(const std::vector<stack_item>& stack)
    {
        JSONCONS_ASSERT(stack.size() >= 2);
        return stack[stack.size() - 2];
    }

    Sink sink_;
    bool flat_;
    std::size_t max_nesting_depth_;
    bool has_column_mapping_;
    bool has_column_names_;
    char_type field_delimiter_;
    char_type subfield_delimiter_;
    string_type line_delimiter_;
    quote_style_kind quote_style_;
    char_type quote_char_;
    char_type quote_escape_char_;
    bool enable_nan_to_num_;
    string_type nan_to_num_;
    bool enable_nan_to_str_;
    string_type nan_to_str_;
    bool enable_inf_to_num_;
    string_type inf_to_num_;
    bool enable_inf_to_str_;
    string_type inf_to_str_;
    bool enable_neginf_to_num_;
    string_type neginf_to_num_;
    bool enable_neginf_to_str_;
    string_type neginf_to_str_;
    allocator_type alloc_;

    std::vector<stack_item, stack_item_allocator_type> stack_;
    jsoncons::detail::write_double fp_;

    std::vector<string_type,string_allocator_type> column_names_;
    std::vector<string_type,string_allocator_type> column_paths_;
    std::unordered_map<string_type,string_type, std::hash<string_type>,std::equal_to<string_type>,string_string_allocator_type> column_path_name_map_;
    std::unordered_map<string_type,string_type, std::hash<string_type>,std::equal_to<string_type>,string_string_allocator_type> column_path_value_map_;
    column_path_column_map_type column_path_column_map_;

    std::size_t column_index_{0};
    string_type value_buffer_;
    typename column_path_column_map_type::iterator column_it_;

    // Noncopyable and nonmoveable
    basic_csv_encoder(const basic_csv_encoder&) = delete;
    basic_csv_encoder& operator=(const basic_csv_encoder&) = delete;
public:
    basic_csv_encoder(Sink&& sink, const Allocator& alloc = Allocator())
       : basic_csv_encoder(std::forward<Sink>(sink), basic_csv_encode_options<CharT>(), alloc)
    {
    }

    basic_csv_encoder(Sink&& sink,
        const basic_csv_encode_options<CharT>& options, 
        const Allocator& alloc = Allocator())
      : sink_(std::forward<Sink>(sink)),
        flat_(options.flat()),
        max_nesting_depth_(options.max_nesting_depth()),
        has_column_mapping_(!options.column_mapping().empty()),
        has_column_names_(!options.column_names().empty()),
        field_delimiter_(options.field_delimiter()),
        subfield_delimiter_(options.subfield_delimiter()),
        line_delimiter_(options.line_delimiter()),
        quote_style_(options.quote_style()),
        quote_char_(options.quote_char()),
        quote_escape_char_(options.quote_escape_char()),
        enable_nan_to_num_(options.enable_nan_to_num()),
        nan_to_num_(options.nan_to_num()),
        enable_nan_to_str_(options.enable_nan_to_str()),
        nan_to_str_(options.nan_to_str()),
        enable_inf_to_num_(options.enable_inf_to_num()),
        inf_to_num_(options.inf_to_num()),
        enable_inf_to_str_(options.enable_inf_to_str()),
        inf_to_str_(options.inf_to_str()),
        enable_neginf_to_num_(options.enable_neginf_to_num()),
        neginf_to_num_(options.neginf_to_num()),
        enable_neginf_to_str_(options.enable_neginf_to_str()),
        neginf_to_str_(options.neginf_to_str()),
        alloc_(alloc),
        stack_(alloc),
        fp_(options.float_format(), options.precision()),
        column_names_(alloc),
        column_paths_(alloc),
        column_path_name_map_(alloc),
        column_path_value_map_(alloc),
        column_path_column_map_(alloc),
        value_buffer_(alloc),
        column_it_(column_path_column_map_.end())
    {
        if (has_column_mapping_)
        {
            for (const auto& item : options.column_mapping())
            {
                column_paths_.emplace_back(item.first);
                column_path_name_map_.emplace(item.first, item.second);
                column_path_value_map_.emplace(item.first, string_type{alloc_});
            }
        }
        if (has_column_names_)
        {
            jsoncons::csv::detail::parse_column_names(options.column_names(), column_names_);
        }
    }

    ~basic_csv_encoder() noexcept
    {
        JSONCONS_TRY
        {
            sink_.flush();
        }
        JSONCONS_CATCH(...)
        {
        }
    }

    void reset()
    {
        stack_.clear();
        if (!has_column_mapping_)
        {
            column_paths_.clear();
            column_path_value_map_.clear();
        }
        column_index_ = 0;
    }

    void reset(Sink&& sink)
    {
        sink_ = std::move(sink);
        reset();
    }

private:

    void escape_string(const CharT* s,
                       std::size_t length,
                       CharT quote_char, CharT quote_escape_char,
                       string_type& sink)
    {
        const CharT* begin = s;
        const CharT* end = s + length;
        for (const CharT* it = begin; it != end; ++it)
        {
            CharT c = *it;
            if (c == quote_char)
            {
                sink.push_back(quote_escape_char); 
                sink.push_back(quote_char);
            }
            else
            {
                sink.push_back(c);
            }
        }
    }

    void visit_flush() override
    {
        sink_.flush();
    }

    bool visit_begin_object(semantic_tag, const ser_context&, std::error_code& ec) override
    {
        if (stack_.empty())
        {
            stack_.emplace_back(stack_item_kind::column_mapping);
            if (has_column_names_)
            {
                for (const auto& item : column_names_)
                {
                    string_type str{alloc_};
                    str.push_back('/');
                    str.append(jsonpointer::escape<char_type,char_allocator_type>(item, alloc_));
                    column_paths_.emplace_back(str);
                    column_path_value_map_.emplace(str, string_type{alloc_});
                    column_path_name_map_.emplace(std::move(str), item);
                }
                has_column_mapping_ = true;
            }
            return true;
        }
        if (JSONCONS_UNLIKELY(stack_.size() >= max_nesting_depth_))
        {
            ec = csv_errc::max_nesting_depth_exceeded;
            return false;
        } 
        
        // legacy        
        if (has_column_names_ && stack_.back().count_ == 0)
        {
            if (stack_.back().item_kind_ == stack_item_kind::flat_row_mapping || stack_.back().item_kind_ == stack_item_kind::row_mapping)
            {
                for (const auto& item : column_names_)
                {
                    string_type str{alloc_};
                    str.push_back('/');
                    str.append(jsonpointer::escape<char_type,char_allocator_type>(item, alloc_));
                    column_paths_.emplace_back(str);
                    column_path_value_map_.emplace(str, string_type{alloc_});
                    column_path_name_map_.emplace(std::move(str), item);
                }
                has_column_mapping_ = true;
            }
        }
        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::flat_row_mapping:
                stack_.emplace_back(stack_item_kind::flat_object);
                break;
            case stack_item_kind::row_mapping:
                stack_.emplace_back(stack_item_kind::object);
                return true;
            case stack_item_kind::object:
                stack_.emplace_back(stack_item_kind::object);
                break;
            case stack_item_kind::flat_object:
                if (subfield_delimiter_ == char_type())
                {
                    stack_.emplace_back(stack_item_kind::unmapped);
                }
                else
                {
                    stack_.back().column_path_ = parent(stack_).column_path_;
                    value_buffer_.clear();
                    stack_.emplace_back(stack_item_kind::multivalued_field);
                }
                break;
            case stack_item_kind::column_multivalued_field:
                break;
            case stack_item_kind::unmapped:
                stack_.emplace_back(stack_item_kind::unmapped);
                break;
            default: // error
                //std::cout << "visit_begin_object " << (int)stack_.back().item_kind_ << "\n"; 
                ec = csv_errc::source_error;
                return false;
        }
        return true;
    }

    bool visit_end_object(const ser_context&, std::error_code& ec) override
    {
        JSONCONS_ASSERT(!stack_.empty());

        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::flat_object:
            case stack_item_kind::object:
                if (parent(stack_).item_kind_ == stack_item_kind::row_mapping || parent(stack_).item_kind_ == stack_item_kind::flat_row_mapping)
                {
                    if (stack_[0].count_ == 0)
                    {
                        bool first = true;
                        for (std::size_t i = 0; i < column_paths_.size(); ++i)
                        {
                            auto it = column_path_name_map_.find(column_paths_[i]);
                            if (it != column_path_name_map_.end())
                            {
                                if (!first)
                                {
                                    sink_.push_back(field_delimiter_);
                                }
                                else
                                {
                                    first = false;
                                }
                                sink_.append(it->second.data(), it->second.length());
                            }
                        }
                        sink_.append(line_delimiter_.data(), line_delimiter_.length());
                    }
                    for (std::size_t i = 0; i < column_paths_.size(); ++i)
                    {
                        if (i > 0)
                        {
                            sink_.push_back(field_delimiter_);
                        }
                        auto it = column_path_value_map_.find(column_paths_[i]);
                        if (it != column_path_value_map_.end())
                        {
                            sink_.append(it->second.data(), it->second.length());
                            it->second.clear();
                        }
                    }
                    sink_.append(line_delimiter_.data(), line_delimiter_.length());
                }
                break;
            case stack_item_kind::column_mapping:
            {
                // Write header
                {
                    bool first = true;
                    for (std::size_t i = 0; i < column_paths_.size(); ++i)
                    {
                        auto it = column_path_name_map_.find(column_paths_[i]);
                        if (it != column_path_name_map_.end())
                        {
                            if (!first)
                            {
                                sink_.push_back(field_delimiter_);
                            }
                            sink_.append(it->second.data(), it->second.length());
                            first = false;
                        }
                    }
                    sink_.append(line_delimiter_.data(), line_delimiter_.length());
                }

                std::vector<std::pair<typename column_type::const_iterator,typename column_type::const_iterator>> columns;
                for (const auto& item : column_paths_)
                {
                    auto it = column_path_column_map_.find(item);
                    if (it != column_path_column_map_.end())
                    {
                        columns.emplace_back((*it).second.cbegin(), (*it).second.cend());
                    }
                }

                if (!columns.empty())
                {
                    const std::size_t no_cols = columns.size();
                    bool done = false;
                    while (!done)
                    {
                        std::size_t missing_cols = 0;
                        std::size_t new_missing_cols = 0;
                        bool first = true;
                        for (std::size_t i = 0; i < no_cols; ++i)
                        {
                            auto& item = columns[i];
                            if (item.first == item.second)
                            {                               
                                ++missing_cols;
                                ++new_missing_cols;
                                if (missing_cols == no_cols)
                                {
                                    done = true;
                                }
                                else if (i == (no_cols-1))
                                {
                                    while (new_missing_cols > 0)
                                    {
                                        sink_.push_back(field_delimiter_);
                                        --new_missing_cols;
                                    }
                                }
                            }
                            else
                            {
                                while (new_missing_cols > 0)
                                {
                                    sink_.push_back(field_delimiter_);
                                    --new_missing_cols;
                                }
                                if (!first)
                                {
                                    sink_.push_back(field_delimiter_);
                                }
                                else
                                {
                                    first = false;
                                }
                                sink_.append((*(item.first)).data(), (*(item.first)).size());
                                ++item.first;
                            }
                        }
                        if (!done)
                        {
                            sink_.append(line_delimiter_.data(), line_delimiter_.length());
                        }
                    }
                }
                break;
            }
            case stack_item_kind::column_multivalued_field:
                break;
            case stack_item_kind::unmapped:
                break;
            default:
                //std::cout << "visit_end_object " << (int)stack_.back().item_kind_ << "\n"; 
                ec = csv_errc::source_error;
                return false;
        }
        stack_.pop_back();
        if (!stack_.empty())
        {
            ++stack_.back().count_;
        }
        return true;
    }

    bool visit_begin_array(semantic_tag, const ser_context&, std::error_code& ec) override
    {
        if (stack_.empty())
        {
            if (flat_)
            {
                stack_.emplace_back(stack_item_kind::flat_row_mapping);
            }
            else
            {
                stack_.emplace_back(stack_item_kind::row_mapping);
            }
            return true;
        }
        if (JSONCONS_UNLIKELY(stack_.size() >= max_nesting_depth_))
        {
            ec = csv_errc::max_nesting_depth_exceeded;
            return false;
        }
        // legacy        
        if (has_column_names_ && stack_.back().count_ == 0)
        {
            if (stack_.back().item_kind_ == stack_item_kind::flat_row_mapping || stack_.back().item_kind_ == stack_item_kind::row_mapping)
            {
                std::size_t index = 0;
                for (const auto& item : column_names_)
                {
                    string_type str{alloc_};
                    str.push_back('/');
                    jsoncons::detail::from_integer(index, str);
                    column_paths_.emplace_back(str);
                    column_path_value_map_.emplace(str, string_type{alloc_});
                    column_path_name_map_.emplace(std::move(str), item);
                    ++index;
                }
                has_column_mapping_ = true;
            }
        }
        
        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::flat_row_mapping:
                if (has_column_mapping_)
                {
                    stack_.emplace_back(stack_item_kind::flat_row);
                }
                else
                {
                    stack_.emplace_back(stack_item_kind::stream_flat_row);
                }
                break;
            case stack_item_kind::row_mapping:
                stack_.emplace_back(stack_item_kind::row);
                break;
            case stack_item_kind::object:
                stack_.emplace_back(stack_item_kind::object);
                break;
            case stack_item_kind::row:
                stack_.emplace_back(stack_item_kind::row);
                break;
            case stack_item_kind::flat_row:
                if (subfield_delimiter_ == char_type())
                {
                    stack_.emplace_back(stack_item_kind::unmapped);
                }
                else
                {
                    append_array_path_component();
                    value_buffer_.clear();
                    stack_.emplace_back(stack_item_kind::multivalued_field);
                }
                break;
            case stack_item_kind::stream_flat_row:
                if (subfield_delimiter_ == char_type())
                {
                    stack_.emplace_back(stack_item_kind::unmapped);
                }
                else
                {
                    value_buffer_.clear();
                    stack_.emplace_back(stack_item_kind::stream_multivalued_field);
                }
                break;
            case stack_item_kind::flat_object:
                if (subfield_delimiter_ == char_type())
                {
                    stack_.emplace_back(stack_item_kind::unmapped);
                }
                else
                {
                    if (stack_[0].count_ == 0)
                    {
                        if (!has_column_mapping_)
                        {
                            string_type str = stack_.back().column_path_;
                            column_paths_.emplace_back(str);
                            column_path_value_map_.emplace(std::move(str), string_type{alloc_});
                        }
                    }
                    
                    value_buffer_.clear();
                    stack_.emplace_back(stack_item_kind::multivalued_field);
                }
                break;
            case stack_item_kind::multivalued_field:
            case stack_item_kind::stream_multivalued_field:
                stack_.emplace_back(stack_item_kind::unmapped);
                break;
            case stack_item_kind::column_mapping:
                stack_.emplace_back(stack_item_kind::column);
                break;
            case stack_item_kind::column:
            {
                value_buffer_.clear();
                stack_.emplace_back(stack_item_kind::column_multivalued_field);
                break;
            }
            case stack_item_kind::column_multivalued_field:
                stack_.emplace_back(stack_item_kind::unmapped);
                break;
            case stack_item_kind::unmapped:
                stack_.emplace_back(stack_item_kind::unmapped);
                break;
            default: // error
                //std::cout << "visit_begin_array " << (int)stack_.back().item_kind_ << "\n"; 
                ec = csv_errc::source_error;
                return false;
        }
        return true;
    }

    bool visit_end_array(const ser_context&, std::error_code& ec) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        
        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::row_mapping:
            case stack_item_kind::flat_row_mapping:
                break;
            case stack_item_kind::flat_row:
                if (parent(stack_).item_kind_ == stack_item_kind::flat_row_mapping)
                {
                    if (stack_[0].count_ == 0 && !column_path_name_map_.empty())
                    {
                        std::size_t col = 0;
                        for (std::size_t i = 0; i < column_paths_.size(); ++i)
                        {
                            auto it = column_path_name_map_.find(column_paths_[i]);
                            if (it != column_path_name_map_.end())
                            {
                                if (col > 0)
                                {
                                    sink_.push_back(field_delimiter_);
                                }
                                sink_.append(it->second.data(), it->second.length());
                                ++col;
                            }
                        }
                        sink_.append(line_delimiter_.data(), line_delimiter_.length());
                    }

                    for (std::size_t i = 0; i < column_paths_.size(); ++i)
                    {
                        if (i > 0)
                        {
                            sink_.push_back(field_delimiter_);
                        }
                        auto it = column_path_value_map_.find(column_paths_[i]);
                        if (it != column_path_value_map_.end())
                        {
                            sink_.append(it->second.data(), it->second.length());
                            it->second.clear();
                        }
                    }
                    sink_.append(line_delimiter_.data(), line_delimiter_.length());
                }
                break;
            case stack_item_kind::stream_flat_row:
                if (parent(stack_).item_kind_ == stack_item_kind::flat_row_mapping)
                {
                    sink_.append(line_delimiter_.data(), line_delimiter_.length());
                }
                break;
            case stack_item_kind::multivalued_field:
            {
                auto it = column_path_value_map_.find(parent(stack_).column_path_);
                if (it != column_path_value_map_.end())
                {
                    it->second = value_buffer_;
                }
                break;
            }
            case stack_item_kind::stream_multivalued_field:
            {
                if (parent(stack_).count_ > 0)
                {
                    sink_.push_back(field_delimiter_);
                }
                sink_.append(value_buffer_.data(), value_buffer_.size());
                break;
            }
            case stack_item_kind::row:
                if (parent(stack_).item_kind_ == stack_item_kind::row_mapping)
                {
                    if (stack_[0].count_ == 0)
                    {
                        std::size_t col = 0;
                        for (std::size_t i = 0; i < column_paths_.size(); ++i)
                        {
                            auto it = column_path_name_map_.find(column_paths_[i]);
                            if (it != column_path_name_map_.end())
                            {
                                if (col > 0)
                                {
                                    sink_.push_back(field_delimiter_);
                                }
                                sink_.append(it->second.data(), it->second.length());
                                ++col;
                            }
                        }
                        sink_.append(line_delimiter_.data(), 
                            line_delimiter_.length());
                    }
                    
                    for (std::size_t i = 0; i < column_paths_.size(); ++i)
                    {
                        if (i > 0)
                        {
                            sink_.push_back(field_delimiter_);
                        }
                        auto it = column_path_value_map_.find(column_paths_[i]);
                        if (it != column_path_value_map_.end())
                        {
                            sink_.append(it->second.data(), it->second.length());
                            it->second.clear();
                        }
                    }
                    sink_.append(line_delimiter_.data(), line_delimiter_.length());
                }
                break;
            case stack_item_kind::column:
                ++column_index_;
                break;
            case stack_item_kind::column_multivalued_field:
                column_it_->second.emplace_back(value_buffer_.data(),value_buffer_.length());
                break;
            case stack_item_kind::unmapped:
                break;
            default:
                //std::cout << "visit_end_array " << (int)stack_.back().item_kind_ << "\n"; 
                ec = csv_errc::source_error;
                return false;
        }
        stack_.pop_back();

        if (!stack_.empty())
        {
            ++stack_.back().count_;
        }
        return true;
    }

    bool visit_key(const string_view_type& name, const ser_context&, std::error_code&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::flat_object:
            {
                stack_.back().column_path_ = parent(stack_).column_path_;
                stack_.back().column_path_.push_back('/');
                stack_.back().column_path_.append(jsonpointer::escape<char_type,char_allocator_type>(name, alloc_));
                if (!has_column_mapping_)
                {
                    column_path_name_map_.emplace(stack_.back().column_path_, name);
                }
                break;
            }
            case stack_item_kind::object:
            {
                stack_.back().column_path_ = parent(stack_).column_path_;
                stack_.back().column_path_.push_back('/');
                stack_.back().column_path_.append(jsonpointer::escape<char_type,char_allocator_type>(name, alloc_));
                if (!has_column_mapping_)
                {
                    column_path_name_map_.emplace(stack_.back().column_path_, stack_.back().column_path_);
                }
                break;
            }
            case stack_item_kind::column_mapping:
            {
                stack_.back().column_path_.clear();
                stack_.back().column_path_.push_back('/');
                stack_.back().column_path_.append(std::string(name));
                if (!has_column_mapping_)
                {
                    
                    string_type str = stack_.back().column_path_;
                    column_paths_.emplace_back(str);
                    column_path_name_map_.emplace(std::move(str), name);
                }
                column_it_ = column_path_column_map_.emplace(stack_.back().column_path_, column_type{alloc_}).first;
                break;
            }
            default:
                break;
        }
        return true;
    }
    
    void append_array_path_component()
    {
        stack_.back().column_path_ = parent(stack_).column_path_;
        stack_.back().column_path_.push_back('/');
        jsoncons::detail::from_integer(stack_.back().count_, stack_.back().column_path_);
        if (stack_[0].count_ == 0)
        {
            if (!has_column_mapping_)
            {
                string_type str = stack_.back().column_path_;
                if (stack_.back().item_kind_ == stack_item_kind::row)
                {
                    column_path_name_map_.emplace(str, stack_.back().column_path_);
                }
                column_paths_.emplace_back(str);
                column_path_value_map_.emplace(std::move(str), string_type{alloc_});
            }
        }
    }

    bool visit_null(semantic_tag, const ser_context&, std::error_code&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::flat_object:
            case stack_item_kind::object:
            {
                if (stack_[0].count_ == 0)
                {
                    if (!has_column_mapping_)
                    {
                        string_type str = stack_.back().column_path_;
                        column_paths_.emplace_back(str);
                        column_path_value_map_.emplace(std::move(str), string_type{alloc_});
                    }
                }
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_null_value(it->second);
                }
                break;
            }
            case stack_item_kind::flat_row:
            case stack_item_kind::row:
            {
                append_array_path_component();
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_null_value(it->second);
                }
                break;
            }
            case stack_item_kind::stream_flat_row:
            {
                if (stack_.back().count_ > 0)
                {
                    sink_.push_back(field_delimiter_);
                }
                value_buffer_.clear();
                write_null_value(value_buffer_);
                sink_.append(value_buffer_.data(), value_buffer_.size());
                break;
            }
            case stack_item_kind::column_multivalued_field:
            case stack_item_kind::multivalued_field:
            case stack_item_kind::stream_multivalued_field:
            {
                if (!value_buffer_.empty())
                {
                    value_buffer_.push_back(subfield_delimiter_);
                }
                write_null_value(value_buffer_);
                break;
            }
            case stack_item_kind::column:
            {
                (*column_it_).second.emplace_back();
                write_null_value((*column_it_).second.back());
                break;
            }
            default:
                break;
        }
        ++stack_.back().count_;
        return true;
    }

    bool visit_string(const string_view_type& sv, semantic_tag, const ser_context&, std::error_code&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::flat_object:
            case stack_item_kind::object:
            {
                if (stack_[0].count_ == 0)
                {
                    if (!has_column_mapping_)
                    {
                        string_type str = stack_.back().column_path_;
                        column_paths_.emplace_back(str);
                        column_path_value_map_.emplace(std::move(str), string_type{alloc_});
                    }
                }

                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_string_value(sv, it->second);
                }
                break;
            }
            case stack_item_kind::flat_row:
            case stack_item_kind::row:
            {
                append_array_path_component();
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_string_value(sv, it->second);
                }
                break;
            }
            case stack_item_kind::stream_flat_row:
            {
                if (stack_.back().count_ > 0)
                {
                    sink_.push_back(field_delimiter_);
                }
                value_buffer_.clear();
                write_string_value(sv, value_buffer_);
                sink_.append(value_buffer_.data(), value_buffer_.size());
                break;
            }
            case stack_item_kind::column_multivalued_field:
            case stack_item_kind::multivalued_field:
            case stack_item_kind::stream_multivalued_field:
            {
                if (!value_buffer_.empty())
                {
                    value_buffer_.push_back(subfield_delimiter_);
                }
                write_string_value(sv, value_buffer_);
                break;
            }
            case stack_item_kind::column:
            {
                (*column_it_).second.emplace_back();
                write_string_value(sv, (*column_it_).second.back());
                break;
            }
            default:
                break;
        }
        ++stack_.back().count_;
        return true;
    }

    bool visit_byte_string(const byte_string_view& b, 
                              semantic_tag tag, 
                              const ser_context& context,
                              std::error_code& ec) override
    {
        JSONCONS_ASSERT(!stack_.empty());

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
        byte_string_chars_format format = jsoncons::detail::resolve_byte_string_chars_format(encoding_hint,byte_string_chars_format::none,byte_string_chars_format::base64url);

        std::basic_string<CharT> s;
        switch (format)
        {
            case byte_string_chars_format::base16:
            {
                encode_base16(b.begin(),b.end(),s);
                visit_string(s, semantic_tag::none, context, ec);
                break;
            }
            case byte_string_chars_format::base64:
            {
                encode_base64(b.begin(),b.end(),s);
                visit_string(s, semantic_tag::none, context, ec);
                break;
            }
            case byte_string_chars_format::base64url:
            {
                encode_base64url(b.begin(),b.end(),s);
                visit_string(s, semantic_tag::none, context, ec);
                break;
            }
            default:
            {
                JSONCONS_UNREACHABLE();
            }
        }

        return true;
    }

    bool visit_double(double val, 
                         semantic_tag, 
                         const ser_context& context,
                         std::error_code& ec) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::flat_object:
            case stack_item_kind::object:
            {
                if (stack_[0].count_ == 0)
                {
                    if (!has_column_mapping_)
                    {
                        string_type str = stack_.back().column_path_;
                        column_paths_.emplace_back(str);
                        column_path_value_map_.emplace(std::move(str), string_type{alloc_});
                    }
                }
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_double_value(val, context, it->second, ec);
                }
                break;
            }
            case stack_item_kind::flat_row:
            case stack_item_kind::row:
            {
                append_array_path_component();
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_double_value(val, context, it->second, ec);
                }
                break;
            }
            case stack_item_kind::stream_flat_row:
            {
                if (stack_.back().count_ > 0)
                {
                    sink_.push_back(field_delimiter_);
                }
                value_buffer_.clear();
                write_double_value(val, context, value_buffer_, ec);
                if (ec)
                {
                    return false;
                }
                sink_.append(value_buffer_.data(), value_buffer_.size());
                break;
            }
            case stack_item_kind::multivalued_field:
            case stack_item_kind::column_multivalued_field:
            case stack_item_kind::stream_multivalued_field:
            {
                if (!value_buffer_.empty())
                {
                    value_buffer_.push_back(subfield_delimiter_);
                }
                write_double_value(val, context, value_buffer_, ec);
                break;
            }
            case stack_item_kind::column:
            {
                (*column_it_).second.emplace_back();
                write_double_value(val, context, (*column_it_).second.back(), ec);
                break;
            }
            default:
                break;
        }
        ++stack_.back().count_;
        return true;
    }

    bool visit_int64(int64_t val, 
                        semantic_tag, 
                        const ser_context&,
                        std::error_code&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::flat_object:
            case stack_item_kind::object:
            {
                if (stack_[0].count_ == 0)
                {
                    if (!has_column_mapping_)
                    {
                        string_type str = stack_.back().column_path_;
                        column_paths_.emplace_back(str);
                        column_path_value_map_.emplace(std::move(str), string_type{alloc_});
                    }
                }
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_int64_value(val, it->second);
                }
                break;
            }
            case stack_item_kind::flat_row:
            case stack_item_kind::row:
            {
                append_array_path_component();
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_int64_value(val, it->second);
                }
                break;
            }
            case stack_item_kind::stream_flat_row:
            {
                if (stack_.back().count_ > 0)
                {
                    sink_.push_back(field_delimiter_);
                }
                value_buffer_.clear();
                write_int64_value(val, value_buffer_);
                sink_.append(value_buffer_.data(), value_buffer_.size());
                break;
            }
            case stack_item_kind::column_multivalued_field:
            case stack_item_kind::multivalued_field:
            case stack_item_kind::stream_multivalued_field:
            {
                if (!value_buffer_.empty())
                {
                    value_buffer_.push_back(subfield_delimiter_);
                }
                write_int64_value(val, value_buffer_);
                break;
            }
            case stack_item_kind::column:
            {
                (*column_it_).second.emplace_back();
                write_int64_value(val, (*column_it_).second.back());
                break;
            }
            default:
                break;
        }
        ++stack_.back().count_;
        return true;
    }

    bool visit_uint64(uint64_t val, 
                      semantic_tag, 
                      const ser_context&,
                      std::error_code&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::flat_object:
            case stack_item_kind::object:
            {
                if (stack_[0].count_ == 0)
                {
                    if (!has_column_mapping_)
                    {
                        string_type str = stack_.back().column_path_;
                        column_paths_.emplace_back(str);
                        column_path_value_map_.emplace(std::move(str), string_type{alloc_});
                    }
                }
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_uint64_value(val, it->second);
                }
                break;
            }
            case stack_item_kind::flat_row:
            case stack_item_kind::row:
            {
                append_array_path_component();
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_uint64_value(val, it->second);
                }
                break;
            }
            case stack_item_kind::stream_flat_row:
            {
                if (stack_.back().count_ > 0)
                {
                    sink_.push_back(field_delimiter_);
                }
                value_buffer_.clear();
                write_uint64_value(val, value_buffer_);
                sink_.append(value_buffer_.data(), value_buffer_.size());
                break;
            }
            case stack_item_kind::multivalued_field:
            case stack_item_kind::column_multivalued_field:
            case stack_item_kind::stream_multivalued_field:
            {
                if (!value_buffer_.empty())
                {
                    value_buffer_.push_back(subfield_delimiter_);
                }
                write_uint64_value(val, value_buffer_);
                break;
            }
            case stack_item_kind::column:
            {
                (*column_it_).second.emplace_back();
                write_uint64_value(val, (*column_it_).second.back());
                break;
            }
            default:
                break;
        }
        ++stack_.back().count_;
        return true;
    }

    bool visit_bool(bool val, semantic_tag, const ser_context&, std::error_code&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        switch (stack_.back().item_kind_)
        {
            case stack_item_kind::flat_object:
            case stack_item_kind::object:
            {
                if (stack_[0].count_ == 0)
                {
                    if (!has_column_mapping_)
                    {
                        string_type str = stack_.back().column_path_;
                        column_paths_.emplace_back(str);
                        column_path_value_map_.emplace(std::move(str), string_type{alloc_});
                    }
                }
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_bool_value(val, it->second);
                }
                break;
            }
            case stack_item_kind::flat_row:
            case stack_item_kind::row:
            {
                append_array_path_component();
                auto it = column_path_value_map_.find(stack_.back().column_path_);
                if (it != column_path_value_map_.end())
                {
                    write_bool_value(val, it->second);
                }
                break;
            }
            case stack_item_kind::stream_flat_row:
            {
                if (stack_.back().count_ > 0)
                {
                    sink_.push_back(field_delimiter_);
                }
                value_buffer_.clear();
                write_bool_value(val, value_buffer_);
                sink_.append(value_buffer_.data(), value_buffer_.size());
                break;
            }
            case stack_item_kind::multivalued_field:
            case stack_item_kind::column_multivalued_field:
            case stack_item_kind::stream_multivalued_field:
            {
                if (!value_buffer_.empty())
                {
                    value_buffer_.push_back(subfield_delimiter_);
                }
                write_bool_value(val, value_buffer_);
                break;
            }
            case stack_item_kind::column:
            {
                (*column_it_).second.emplace_back();
                write_bool_value(val, (*column_it_).second.back());
                break;
            }
            default:
                break;
        }
        ++stack_.back().count_;
        return true;
    }

    void write_string_value(const string_view_type& value, string_type& str)
    {
        const char* s = value.data();
        const std::size_t length = value.length();
        
        bool quote = false;
        if (quote_style_ == quote_style_kind::all || quote_style_ == quote_style_kind::nonnumeric ||
            (quote_style_ == quote_style_kind::minimal &&
            (std::char_traits<CharT>::find(s, length, field_delimiter_) != nullptr || std::char_traits<CharT>::find(s, length, quote_char_) != nullptr)))
        {
            quote = true;
            str.push_back(quote_char_);
        }
        escape_string(s, length, quote_char_, quote_escape_char_, str);
        if (quote)
        {
            str.push_back(quote_char_);
        }
    }

    void write_double_value(double val, const ser_context& context, string_type& str, std::error_code& ec)
    {
        if (!std::isfinite(val))
        {
            if ((std::isnan)(val))
            {
                if (enable_nan_to_num_)
                {
                    str.append(nan_to_num_.data(), nan_to_num_.length());
                }
                else if (enable_nan_to_str_)
                {
                    visit_string(nan_to_str_, semantic_tag::none, context, ec);
                }
                else
                {
                    str.append(null_constant().data(), null_constant().size());
                }
            }
            else if (val == std::numeric_limits<double>::infinity())
            {
                if (enable_inf_to_num_)
                {
                    str.append(inf_to_num_.data(), inf_to_num_.length());
                }
                else if (enable_inf_to_str_)
                {
                    visit_string(inf_to_str_, semantic_tag::none, context, ec);
                }
                else
                {
                    str.append(null_constant().data(), null_constant().size());
                }
            }
            else
            {
                if (enable_neginf_to_num_)
                {
                    str.append(neginf_to_num_.data(), neginf_to_num_.length());
                }
                else if (enable_neginf_to_str_)
                {
                    visit_string(neginf_to_str_, semantic_tag::none, context, ec);
                }
                else
                {
                    str.append(null_constant().data(), null_constant().size());
                }
            }
        }
        else
        {
            fp_(val, str);
        }
    }

    void write_int64_value(int64_t val, string_type& str)
    {
        jsoncons::detail::from_integer(val,str);
    }

    void write_uint64_value(uint64_t val, string_type& str)
    {
        jsoncons::detail::from_integer(val,str);
    }

    void write_bool_value(bool val, string_type& str) 
    {
        if (val)
        {
            str.append(true_constant().data(), true_constant().size());
        }
        else
        {
            str.append(false_constant().data(), false_constant().size());
        }
    }
 
    void write_null_value(string_type& str) 
    {
        str.append(null_constant().data(), null_constant().size());
    }
};

using csv_stream_encoder = basic_csv_encoder<char>;
using csv_string_encoder = basic_csv_encoder<char,jsoncons::string_sink<std::string>>;
using csv_wstream_encoder = basic_csv_encoder<wchar_t>;
using wcsv_string_encoder = basic_csv_encoder<wchar_t,jsoncons::string_sink<std::wstring>>;

}}

#endif
