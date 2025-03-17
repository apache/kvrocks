// Copyright 2013-2025 Daniel Parker
// 
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_COMMON_KEYWORD_VALIDATOR_HPP
#define JSONCONS_EXT_JSONSCHEMA_COMMON_KEYWORD_VALIDATOR_HPP

#include <cassert>
#include <cstddef>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/utility/byte_string.hpp>
#include <jsoncons/conv_error.hpp>
#include <jsoncons/json_reader.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/utility/unicode_traits.hpp>
#include <jsoncons/utility/uri.hpp>

#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonschema/common/format.hpp>
#include <jsoncons_ext/jsonschema/common/uri_wrapper.hpp>
#include <jsoncons_ext/jsonschema/common/validator.hpp>

#if defined(JSONCONS_HAS_STD_REGEX)
#include <regex>
#endif

namespace jsoncons {
namespace jsonschema {
    
    template <typename Json>
    class schema_validator;

    template <typename Json>
    class ref
    {
    public:
        virtual ~ref() = default;
        virtual void set_referred_schema(const schema_validator<Json>* target) = 0;
    };

    template <typename Json>
    class keyword_base : public validation_message_factory
    {
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::string keyword_name_;
        const Json* schema_ptr_;
        uri schema_location_;
        std::string custom_message_;
    public:

        keyword_base(const std::string& keyword_name, const Json& schema, const uri& schema_location,
            const std::string& custom_message)
            : keyword_name_(keyword_name), schema_ptr_(std::addressof(schema)), 
              schema_location_(schema_location), custom_message_(custom_message)
        {
        }

        virtual ~keyword_base() = default;

        keyword_base(const keyword_base&) = delete;
        keyword_base(keyword_base&&) = default;
        keyword_base& operator=(const keyword_base&) = delete;
        keyword_base& operator=(keyword_base&&) = default;

        const std::string& keyword_name() const 
        {
            return keyword_name_;
        }

        const Json& schema() const
        {
            return *schema_ptr_;
        }

        const uri& schema_location() const 
        {
            return schema_location_;
        }

        validation_message make_validation_message(const jsonpointer::json_pointer& eval_path,
            const jsonpointer::json_pointer& instance_location,
            const std::string& message) const override
        {
            return validation_message(keyword_name_, 
                eval_path,
                schema_location_, 
                instance_location, 
                custom_message_.empty() ? message : custom_message_);
        }

        validation_message make_validation_message(const jsonpointer::json_pointer& eval_path,
            const jsonpointer::json_pointer& instance_location,
            const std::string& message,
            const std::vector<validation_message>& details) const override
        {
            return validation_message(keyword_name_, 
                eval_path,
                schema_location_, 
                instance_location, 
                custom_message_.empty() ? message : custom_message_,
                details);
        }
    };

    template <typename Json>
    class keyword_validator : public keyword_base<Json>, public virtual validator_base<Json> 
    {
    public:
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;

        keyword_validator(const std::string& keyword_name, const Json& schema, const uri& schema_location,
            const std::string& custom_message)
            : keyword_base<Json>(keyword_name, schema, schema_location, custom_message)
        {
        }

        keyword_validator(const keyword_validator&) = delete;
        keyword_validator(keyword_validator&&) = default;
        keyword_validator& operator=(const keyword_validator&) = delete;
        keyword_validator& operator=(keyword_validator&&) = default;
        
        bool always_fails() const override
        {
            return false;
        }          

        bool always_succeeds() const override
        {
            return false;
        }

        const uri& schema_location() const override
        {
            return keyword_base<Json>::schema_location();
        }
    };

    template <typename Json>
    class ref_validator : public keyword_validator<Json>, public virtual ref<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using schema_validator_ptr_type = std::unique_ptr<schema_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        const schema_validator<Json>* referred_schema_;

    public:
        ref_validator(const Json& schema, const ref_validator& other)
            : keyword_validator<Json>(other.keyword_name(), schema, other.schema_location()),
                  referred_schema_{other.referred_schema_}
        {
        }
        
        ref_validator(const Json& schema, const uri& schema_location, const std::string& custom_message = std::string{}) 
            : keyword_validator<Json>("$ref", schema, schema_location, custom_message), referred_schema_{nullptr}
        {
            //std::cout << "ref_validator: " << this->schema_location().string() << "\n";
        }

        ref_validator(const Json& schema, const uri& schema_location, const schema_validator<Json>* referred_schema, 
            const std::string& custom_message = std::string{})
            : keyword_validator<Json>("$ref", schema, schema_location, custom_message), referred_schema_(referred_schema)
        {
            //std::cout << "ref_validator2: " << this->schema_location().string() << "\n";
        }

        const schema_validator<Json>* referred_schema() const {return referred_schema_;}
        
        void set_referred_schema(const schema_validator<Json>* target) final { referred_schema_ = target; }

        uri get_base_uri() const
        {
            return this->schema_location();
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            eval_context<Json> this_context(context, this->keyword_name());

            if (!referred_schema_)
            {
                return reporter.error(validation_message(this->keyword_name(), 
                    this_context.eval_path(),
                    this->schema_location(), 
                    instance_location, 
                    "Unresolved schema reference " + this->schema_location().string()));
            }

            return referred_schema_->validate(this_context, instance, instance_location, results, reporter, patch);
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final 
        {
            if (!referred_schema_)
            {
                return walk_result::advance;
            }
            eval_context<Json> this_context(context, this->keyword_name());
            return referred_schema_->walk(this_context, instance, instance_location, reporter);           
        }
    };
    
    template <typename Json>
    class recursive_ref_validator : public keyword_validator<Json>, public virtual ref<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using schema_validator_ptr_type = std::unique_ptr<schema_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        const schema_validator<Json> *tentative_target_; 

    public:
        recursive_ref_validator(const Json& schema, const uri& schema_location, const std::string& custom_message) 
            : keyword_validator<Json>("$recursiveRef", schema, schema_location, custom_message),
              tentative_target_(nullptr)
        {}

        uri get_base_uri() const
        {
            return this->schema_location();
        }

        void set_referred_schema(const schema_validator<Json>* target) final { tentative_target_ = target; }

    private:

        walk_result do_validate(const eval_context<Json>& context, 
            const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            auto rit = context.dynamic_scope().rbegin();
            auto rend = context.dynamic_scope().rend();

            const schema_validator<Json>* schema_ptr = tentative_target_; 
 
            JSONCONS_ASSERT(schema_ptr != nullptr);

            if (schema_ptr->recursive_anchor())
            {
                while (rit != rend)
                {
                    if ((*rit)->recursive_anchor())
                    {
                        schema_ptr = *rit; 
                    }
                    ++rit;
                }
            }

            eval_context<Json> this_context(context, this->keyword_name());
            if (schema_ptr == nullptr)
            {
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    "Unresolved schema reference " + this->schema_location().string()));
                return result;
            }

            return schema_ptr->validate(this_context, instance, instance_location, results, reporter, patch);
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            auto rit = context.dynamic_scope().rbegin();
            auto rend = context.dynamic_scope().rend();

            const schema_validator<Json>* schema_ptr = tentative_target_; 

            JSONCONS_ASSERT(schema_ptr != nullptr);

            if (schema_ptr->recursive_anchor())
            {
                while (rit != rend)
                {
                    if ((*rit)->recursive_anchor())
                    {
                        schema_ptr = *rit; 
                    }
                    ++rit;
                }
            }

            //std::cout << "recursive_ref_validator.do_validate " << "keywordLocation: << " << this->schema_location().string() << ", instanceLocation:" << instance_location.string() << "\n";

            if (schema_ptr == nullptr)
            {
                return walk_result::advance;
            }
            eval_context<Json> this_context(context, this->keyword_name());
            return schema_ptr->walk(this_context, instance, instance_location, reporter);
        }
    };

    template <typename Json>
    class dynamic_ref_validator : public keyword_validator<Json>, public virtual ref<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using schema_validator_ptr_type = std::unique_ptr<schema_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        uri_wrapper value_;
        const schema_validator<Json>* tentative_target_;

    public:
        dynamic_ref_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, const uri_wrapper& value) 
            : keyword_validator<Json>("$dynamicRef", schema, schema_location, custom_message), value_(value),
            tentative_target_(nullptr)
        {
            //std::cout << "dynamic_ref_validator path: " << schema_location.string() << ", value: " << value.string() << "\n";
        }

        void set_referred_schema(const schema_validator<Json>* target) final { tentative_target_ = target; }

        const jsoncons::uri& value() const { return value_.uri(); }

        uri get_base_uri() const
        {
            return this->schema_location();
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            //std::cout << "dynamic_ref_validator [" << context.eval_path().string() << "," << this->schema_location().string() << "]";
            //std::cout << "results:\n";
            //for (const auto& s : results)
            //{
            //    std::cout << "    " << s << "\n";
            //}
            //std::cout << "\n";

            auto rit = context.dynamic_scope().rbegin();
            auto rend = context.dynamic_scope().rend();

            //std::cout << "dynamic_ref_validator::do_validate " << this->value().string() << "\n";

            const schema_validator<Json> *schema_ptr = tentative_target_;

            JSONCONS_ASSERT(schema_ptr != nullptr);

            if (value_.has_plain_name_fragment() && schema_ptr->dynamic_anchor())
            {
                while (rit != rend)
                {
                    auto p = (*rit)->get_schema_for_dynamic_anchor(schema_ptr->dynamic_anchor()->fragment()); 
                    //std::cout << "  (2) [" << (*rit)->schema_location().string() << "] " << ((*rit)->dynamic_anchor() ? (*rit)->dynamic_anchor()->value().string() : "") << "\n";

                    if (p != nullptr) 
                    {
                        //std::cout << "Match found " << p->schema_location().string() << "\n";
                        schema_ptr = p;
                    }

                    ++rit;
                }
            }
            
            //assert(schema_ptr != tentative_target_);

            //std::cout << "dynamic_ref_validator.do_validate " << "keywordLocation: << " << this->schema_location().string() << ", instanceLocation:" << instance_location.string() << "\n";

            eval_context<Json> this_context(context, this->keyword_name());
            return schema_ptr->validate(this_context, instance, instance_location, results, reporter, patch);
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            auto rit = context.dynamic_scope().rbegin();
            auto rend = context.dynamic_scope().rend();

            const schema_validator<Json> *schema_ptr = tentative_target_;

            JSONCONS_ASSERT(schema_ptr != nullptr);

            if (value_.has_plain_name_fragment() && schema_ptr->dynamic_anchor())
            {
                while (rit != rend)
                {
                    auto p = (*rit)->get_schema_for_dynamic_anchor(schema_ptr->dynamic_anchor()->fragment()); 
                    if (p != nullptr) 
                    {
                        schema_ptr = p;
                    }
                    ++rit;
                }
            }

            eval_context<Json> this_context(context, this->keyword_name());
            return schema_ptr->walk(this_context, instance, instance_location, reporter);
        }
    };

    // contentEncoding

    template <typename Json>
    class content_encoding_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::string content_encoding_;

    public:
        content_encoding_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, const std::string& content_encoding)
            : keyword_validator<Json>("contentEncoding", schema, schema_location, custom_message), 
              content_encoding_(content_encoding)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_string())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            if (content_encoding_ == "base64")
            {
                auto s = instance.template as<jsoncons::string_view>();
                std::string content;
                auto retval = jsoncons::decode_base64(s.begin(), s.end(), content);
                if (retval.ec != jsoncons::conv_errc::success)
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        "Content is not a base64 string"));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (!content_encoding_.empty())
            {
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    "unable to check for contentEncoding '" + content_encoding_ + "'"));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // contentMediaType

    template <typename Json>
    class content_media_type_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::string content_media_type_;
        std::string content_encoding_;

    public:
        content_media_type_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, 
            const std::string& content_media_type,
            const std::string& content_encoding)
            : keyword_validator<Json>("contentMediaType", schema, schema_location, custom_message), 
              content_media_type_(content_media_type), content_encoding_(content_encoding)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_string())
            {
                return walk_result::advance;
            }
            
            std::string str = instance.as_string();
            if (content_encoding_ == "base64")
            {
                std::string content;
                auto retval = jsoncons::decode_base64(str.begin(), str.end(), content);
                if (retval.ec != jsoncons::conv_errc::success)
                {
                    return walk_result::advance;
                }
                str = std::move(content);
            }

            eval_context<Json> this_context(context, this->keyword_name());

            if (content_media_type_ == "application/json")
            {
                json_string_reader reader(str);
                std::error_code ec;
                reader.read(ec);

                if (ec)
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        std::string("Content is not JSON: ") + ec.message()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // format 

    template <typename Json>
    class format_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        validate_format validate_;

    public:
        format_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, const validate_format& validate)
            : keyword_validator<Json>("format", schema, schema_location, custom_message), validate_(validate)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_string())
            {
                return walk_result::advance;
            }

            if (validate_ != nullptr) 
            {
                eval_context<Json> this_context(context, this->keyword_name());
                auto s = instance.template as<std::string>();

                walk_result result = validate_(*this, this_context.eval_path(), instance_location, s, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // pattern 

#if defined(JSONCONS_HAS_STD_REGEX)
    template <typename Json>
    class pattern_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::string pattern_string_;
        std::regex regex_;

    public:
        pattern_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
            const std::string& pattern_string, const std::regex& regex)
            : keyword_validator<Json>("pattern", schema, schema_location, custom_message), 
              pattern_string_(pattern_string), regex_(regex)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_string())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            auto s = instance.template as<std::string>();
            if (!std::regex_search(s, regex_))
            {
                std::string message("String '");
                message.append(s);
                message.append("' does not match pattern '");
                message.append(pattern_string_);
                message.append("'.");
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    std::move(message)));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };
#else
    template <typename Json>
    class pattern_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

    public:
        pattern_validator(const Json& schema, const uri& schema_location, const std::string& custom_message)
            : keyword_validator<Json>("pattern", schema, schema_location, custom_message)
        {
        }

    private:

        walk_result do_validate(const Json&, 
            const jsonpointer::json_pointer&,
            evaluation_results& /*results*/, 
            error_reporter&,
            Json& /*patch*/) const final
        {
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };
#endif

    // maxLength

    template <typename Json>
    class max_length_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::size_t max_length_{0};
    public:
        max_length_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, std::size_t max_length)
            : keyword_validator<Json>("maxLength", schema, schema_location, custom_message), max_length_(max_length)
        {
        }
        
        ~max_length_validator() = default;

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_string())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            auto sv = instance.as_string_view();
            std::size_t length = unicode_traits::count_codepoints(sv.data(), sv.size());
            if (length > max_length_)
            {
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    std::string("Number of characters must be at most ") + std::to_string(max_length_)));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }          
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // minLength

    template <typename Json>
    class min_length_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::size_t min_length_;

    public:
        min_length_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, std::size_t min_length)
            : keyword_validator<Json>("minLength", schema, schema_location, custom_message), min_length_(min_length)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_string())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            auto sv = instance.as_string_view();
            std::size_t length = unicode_traits::count_codepoints(sv.data(), sv.size());
            if (length < min_length_) 
            {
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    std::string("Number of characters must be at least ") + std::to_string(min_length_)));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // maxItems

    template <typename Json>
    class max_items_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::size_t max_items_;
    public:
        max_items_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, std::size_t max_items)
            : keyword_validator<Json>("maxItems", schema, schema_location, custom_message), max_items_(max_items)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            if (instance.size() > max_items_)
            {
                std::string message("Maximum number of items is " + std::to_string(max_items_));
                message.append(" but found " + std::to_string(instance.size()));
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    std::move(message)));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }          
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // minItems

    template <typename Json>
    class min_items_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::size_t min_items_;
    public:
        min_items_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, 
            std::size_t min_items)
            : keyword_validator<Json>("minItems", schema, schema_location, custom_message), min_items_(min_items)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            if (instance.size() < min_items_)
            {
                std::string message("Minimum number of items is " + std::to_string(min_items_));
                message.append(" but found " + std::to_string(instance.size()));
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    std::move(message)));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }          
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // items

    template <typename Json>
    class items_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        schema_validator_ptr_type schema_val_;
    public:
        items_validator(const std::string& keyword_name, const Json& schema, const uri& schema_location, 
            const std::string& custom_message, 
            schema_validator_ptr_type&& schema_val)
            : keyword_validator<Json>(keyword_name, schema, schema_location, custom_message), 
              schema_val_(std::move(schema_val))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter,
            Json& patch) const final
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            if (instance.size() > 0 && schema_val_)
            {
                if (schema_val_->always_fails())
                {
                    jsonpointer::json_pointer item_location = instance_location / 0;
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        item_location, 
                        "Item at index '0' but the schema does not allow any items."));
                    return result;
                }
                else if (schema_val_->always_succeeds())
                {
                    if (context.require_evaluated_items())
                    {
                        results.evaluated_items.insert(range{0,instance.size()});
                    }
                }
                else
                {
                    std::size_t index = 0;
                    std::size_t start = 0;
                    std::size_t end = 0;
                    for (const auto& item : instance.array_range()) 
                    {
                        jsonpointer::json_pointer item_location = instance_location / index;
                        std::size_t errors = reporter.error_count();
                        walk_result result = schema_val_->validate(this_context, item, item_location, results, reporter, patch);
                        if (result == walk_result::abort)
                        {
                            return result;
                        }
                        if (errors == reporter.error_count())
                        {
                            if (context.require_evaluated_items())
                            {
                                if (end == start)
                                {
                                    start = end = index;
                                }
                                ++end;
                            }
                        }
                        else
                        {
                            if (start < end)
                            {
                                results.evaluated_items.insert(range{start, end});
                                start = end;
                            }
                        }
                        ++index;
                    }
                    if (start < end)
                    {
                        results.evaluated_items.insert(range{start, end});
                        start = end;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final 
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }

            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }

            if (schema_val_) 
            {
                std::size_t index = 0;
                for (const auto& item : instance.array_range()) 
                {
                    jsonpointer::json_pointer item_location = instance_location / index;
                    result = schema_val_->walk(context, item, item_location, reporter);
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                    ++index;
                }
            }
            return walk_result::advance;
        }
    };

    // items

    // uniqueItems

    template <typename Json>
    class unique_items_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        bool are_unique_;
    public:
        unique_items_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, bool are_unique)
            : keyword_validator<Json>("uniqueItems", schema, schema_location, custom_message), are_unique_(are_unique)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            if (are_unique_ && !array_has_unique_items(instance))
            {
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    "Array items are not unique"));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        static bool array_has_unique_items(const Json& a) 
        {
            for (auto it = a.array_range().begin(); it != a.array_range().end(); ++it) 
            {
                for (auto jt = it+1; jt != a.array_range().end(); ++jt) 
                {
                    if (*it == *jt) 
                    {
                        return false; // contains duplicates 
                    }
                }
            }
            return true; // elements are unique
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // not

    template <typename Json>
    class not_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        schema_validator_ptr_type schema_val_;

    public:
        not_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
            schema_validator_ptr_type&& schema_val)
            : keyword_validator<Json>("not", schema, schema_location, custom_message), 
              schema_val_(std::move(schema_val))
        {
        }

        bool always_fails() const final
        {
            return schema_val_ ? schema_val_->always_succeeds() : false;;
        }          

        bool always_succeeds() const final
        {
            return schema_val_ ? schema_val_->always_fails() : false;;
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            eval_context<Json> this_context(context, this->keyword_name());

            evaluation_results local_results;
            collecting_error_listener local_reporter;
            walk_result result = schema_val_->validate(this_context, instance, instance_location, local_results, local_reporter, patch);
            if (result == walk_result::abort)
            {
                return result;
            }

            if (local_reporter.errors.empty())
            {
                result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    "Instance must not be valid against schema"));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            else
            {
                results.merge(local_results);
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class any_of_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::vector<schema_validator_ptr_type> validators_;

    public:
        any_of_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
             std::vector<schema_validator_ptr_type>&& validators)
            : keyword_validator<Json>("anyOf", schema, schema_location, custom_message),
              validators_(std::move(validators))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            //std::cout << "any_of_validator.do_validate " << context.eval_path().string() << ", " << instance << "\n";

            collecting_error_listener local_reporter;

            eval_context<Json> this_context(context, this->keyword_name());

            evaluation_results local_results1;
            std::size_t count = 0;
            for (std::size_t i = 0; i < validators_.size(); ++i) 
            {
                evaluation_results local_results2;
                eval_context<Json> item_context(this_context, i);

                std::size_t errors = local_reporter.errors.size();
                walk_result result = validators_[i]->validate(item_context, instance, instance_location, local_results2, local_reporter, patch);
                if (result == walk_result::abort)
                {
                    return result;
                }
                if (errors == local_reporter.errors.size())
                {
                    local_results1.merge(local_results2);
                    ++count;
                }
                //std::cout << "success: " << i << " " << success << "\n";
            }

            if (count > 0)
            {
                results.merge(local_results1);
            }
            else 
            {
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    "Must be valid against at least one schema, but found no matching schemas", 
                    local_reporter.errors));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }
            eval_context<Json> this_context(context, this->keyword_name());

            for (std::size_t i = 0; i < validators_.size(); ++i) 
            {
                eval_context<Json> item_context(this_context, i);

                result = validators_[i]->walk(item_context, instance, instance_location, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            return walk_result::advance;
        }
    };

    template <typename Json>
    class one_of_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::vector<schema_validator_ptr_type> validators_;

    public:
        one_of_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
             std::vector<schema_validator_ptr_type>&& validators)
            : keyword_validator<Json>("oneOf", schema, schema_location, custom_message),
              validators_(std::move(validators))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            //std::cout << "any_of_validator.do_validate " << context.eval_path().string() << ", " << instance << "\n";

            collecting_error_listener local_reporter;

            eval_context<Json> this_context(context, this->keyword_name());

            evaluation_results local_results1;
            std::vector<std::size_t> indices;
            for (std::size_t i = 0; i < validators_.size(); ++i) 
            {
                evaluation_results local_results2;
                eval_context<Json> item_context(this_context, i);

                std::size_t errors = local_reporter.errors.size();
                walk_result result = validators_[i]->validate(item_context, instance, instance_location, local_results2, local_reporter, patch);
                if (result == walk_result::abort)
                {
                    return result;
                }
                if (errors == local_reporter.errors.size())
                {
                    local_results1.merge(local_results2);
                    indices.push_back(i);
                }
                //std::cout << "success: " << i << " " << success << "\n";
            }

            
            if (indices.size() == 1)
            {
                results.merge(local_results1);
            }
            else 
            {
                std::string message;
                if (indices.size() == 0)
                {
                    message = "Must be valid against exactly one schema, but found no matching schemas";
                }
                else
                {
                    message = "Must be valid against exactly one schema, but found " + std::to_string(indices.size()) + " matching schemas at indices ";
                    for (std::size_t i = 0; i < indices.size(); ++i)
                    {
                        if (i > 0)
                        {
                            message.push_back(',');
                        }
                        message.append(std::to_string(i));
                    }
                }
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    message, 
                    local_reporter.errors));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            for (std::size_t i = 0; i < validators_.size(); ++i) 
            {
                eval_context<Json> item_context(this_context, i);

                result = validators_[i]->walk(item_context, instance, instance_location, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            return walk_result::advance;
        }
    };

    template <typename Json>
    class all_of_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::vector<schema_validator_ptr_type> validators_;

    public:
        all_of_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
             std::vector<schema_validator_ptr_type>&& validators)
            : keyword_validator<Json>("allOf", schema, schema_location, custom_message),
              validators_(std::move(validators))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            //std::cout << this->keyword_name() << " [" << context.eval_path().string() << ", " << this->schema_location().string() << "]\n";

            evaluation_results local_results1;
            collecting_error_listener local_reporter;

            eval_context<Json> this_context(context, this->keyword_name());

            std::size_t count = 0;
            for (std::size_t i = 0; i < validators_.size(); ++i) 
            {
                evaluation_results local_results2;
                eval_context<Json> item_context(this_context, i);

                std::size_t errors = local_reporter.errors.size();
                walk_result result = validators_[i]->validate(item_context, instance, instance_location, local_results2, local_reporter, patch);
                if (result == walk_result::abort)
                {
                    return result;
                }
                //std::cout << "local_results2:\n";
                //for (const auto& s : local_results2.evaluated_items)
                //{
                //    std::cout << "    " << s << "\n";
                //}
                if (errors == local_reporter.errors.size())
                {
                    local_results1.merge(local_results2);
                    ++count;
                }
                //std::cout << "success: " << i << " " << success << "\n";
            }

            //std::cout << "local_results1:\n";
            //for (const auto& s : local_results1.evaluated_items)
            //{
            //    std::cout << "    " << s << "\n";
            //}

            if (count == validators_.size())
            {
                results.merge(local_results1);
            }
            else 
            {
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    "Must be valid against all schemas, but found unmatched schemas", 
                    local_reporter.errors));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }
            eval_context<Json> this_context(context, this->keyword_name());

            for (std::size_t i = 0; i < validators_.size(); ++i) 
            {
                eval_context<Json> item_context(this_context, i);

                result = validators_[i]->walk(item_context, instance, instance_location, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            return walk_result::advance;
        }
    };

    template <typename Json>
    class maximum_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        Json value_;
        std::string message_;

    public:
        maximum_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, const Json& value)
            : keyword_validator<Json>("maximum", schema, schema_location, custom_message), value_(value),
              message_{"Maximum value is " + value.template as<std::string>() + " but found"}
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter, 
            Json& /*patch*/) const final 
        {
            eval_context<Json> this_context(context, this->keyword_name());

            if (instance.is_int64() && value_.is_int64())
            {
                if (instance.template as<int64_t>() > value_.template as<int64_t>())
                {
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_uint64() && value_.is_uint64())
            {
                if (instance.template as<uint64_t>() > value_.template as<uint64_t>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_string_view() && instance.tag() == semantic_tag::bigint)
            {
                auto sv1 = instance.as_string_view();
                bigint n1 = bigint::from_string(sv1.data(), sv1.length());
                auto s2 = value_.as_string();
                bigint n2 = bigint::from_string(s2.data(), s2.length());
                if (n1 > n2)
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_number())
            {
                if (instance.template as<double>() > value_.template as<double>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class exclusive_maximum_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        Json value_;
        std::string message_;

    public:
        exclusive_maximum_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, const Json& value)
            : keyword_validator<Json>("exclusiveMaximum", schema, schema_location, custom_message), value_(value),
              message_{"Exclusive maximum value is " + value.template as<std::string>() + " but found "}
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter, 
            Json& /*patch*/) const final 
        {
            eval_context<Json> this_context(context, this->keyword_name());

            if (instance.is_int64() && value_.is_int64())
            {
                if (instance.template as<int64_t>() >= value_.template as<int64_t>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_uint64() && value_.is_uint64())
            {
                if (instance.template as<uint64_t>() >= value_.template as<uint64_t>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_string_view() && instance.tag() == semantic_tag::bigint)
            {
                auto sv1 = instance.as_string_view();
                bigint n1 = bigint::from_string(sv1.data(), sv1.length());
                auto s2 = value_.as_string();
                bigint n2 = bigint::from_string(s2.data(), s2.length());
                if (n1 >= n2)
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_number())
            {
                if (instance.template as<double>() >= value_.template as<double>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class minimum_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        Json value_;
        std::string message_;

    public:
        minimum_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, const Json& value)
            : keyword_validator<Json>("minimum", schema, schema_location, custom_message), value_(value),
              message_{"Minimum value is " + value.template as<std::string>() + " but found "}
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter, 
            Json& /*patch*/) const final
        {
            eval_context<Json> this_context(context, this->keyword_name());

            if (instance.is_int64() && value_.is_int64())
            {
                if (instance.template as<int64_t>() < value_.template as<int64_t>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_uint64() && value_.is_uint64())
            {
                if (instance.template as<uint64_t>() < value_.template as<uint64_t>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_string_view() && instance.tag() == semantic_tag::bigint)
            {
                auto sv1 = instance.as_string_view();
                bigint n1 = bigint::from_string(sv1.data(), sv1.length());
                auto s2 = value_.as_string();
                bigint n2 = bigint::from_string(s2.data(), s2.length());
                if (n1 < n2)
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_number())
            {
                if (instance.template as<double>() < value_.template as<double>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class exclusive_minimum_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        Json value_;
        std::string message_;

    public:
        exclusive_minimum_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, const Json& value)
            : keyword_validator<Json>("exclusiveMinimum", schema, schema_location, custom_message), value_(value),
              message_{"Exclusive minimum value is " + value.template as<std::string>() + " but found "}
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter, 
            Json& /*patch*/) const final 
        {
            eval_context<Json> this_context(context, this->keyword_name());

            if (instance.is_int64() && value_.is_int64())
            {
                if (instance.template as<int64_t>() <= value_.template as<int64_t>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_uint64() && value_.is_uint64())
            {
                if (instance.template as<uint64_t>() <= value_.template as<uint64_t>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_string_view() && instance.tag() == semantic_tag::bigint)
            {
                auto sv1 = instance.as_string_view();
                bigint n1 = bigint::from_string(sv1.data(), sv1.length());
                auto s2 = value_.as_string();
                bigint n2 = bigint::from_string(s2.data(), s2.length());
                if (n1 <= n2)
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (instance.is_number())
            {
                if (instance.template as<double>() <= value_.template as<double>())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        message_ + instance.template as<std::string>()));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class multiple_of_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        double value_;

    public:
        multiple_of_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, double value)
            : keyword_validator<Json>("multipleOf", schema, schema_location, custom_message), value_(value)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter, 
            Json& /*patch*/) const final
        {
            if (!instance.is_number())
            {
                return walk_result::advance;
            }
            eval_context<Json> this_context(context, this->keyword_name());

            double value = instance.template as<double>();
            if (value != 0) // Exclude zero
            {
                if (!is_multiple_of(value, static_cast<double>(value_)))
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        instance.template as<std::string>() + " is not a multiple of " + std::to_string(value_)));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }

        static bool is_multiple_of(double x, double multiple_of) 
        {
            double rem = std::remainder(x, multiple_of);
            double eps = std::nextafter(x, 0) - x;
            return std::fabs(rem) < std::fabs(eps);
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class required_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::vector<std::string> items_;

    public:
        required_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
            const std::vector<std::string>& items)
            : keyword_validator<Json>("required", schema, schema_location, custom_message), items_(items)
        {
        }

        required_validator(const required_validator&) = delete;
        required_validator(required_validator&&) = default;
        required_validator& operator=(const required_validator&) = delete;
        required_validator& operator=(required_validator&&) = default;

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter, 
            Json& /*patch*/) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            for (const auto& key : items_)
            {
                if(instance.find(key) == instance.object_range().end())
                {
                    walk_result result = reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        "Required property '" + key + "' not found."));
                    if(result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // maxProperties

    template <typename Json>
    class max_properties_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::size_t max_properties_;
    public:
        max_properties_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, 
            std::size_t max_properties)
            : keyword_validator<Json>("maxProperties", schema, schema_location, custom_message), max_properties_(max_properties)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }
            
            if (instance.size() > max_properties_)
            {
                eval_context<Json> this_context(context, this->keyword_name());

                std::string message("Maximum number of properties is " + std::to_string(max_properties_));
                message.append(" but found " + std::to_string(instance.size()));
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    std::move(message)));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // minProperties

    template <typename Json>
    class min_properties_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::size_t min_properties_;
    public:
        min_properties_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, std::size_t min_properties)
            : keyword_validator<Json>("minProperties", schema, schema_location, custom_message), min_properties_(min_properties)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }
            if (instance.size() < min_properties_)
            {
                eval_context<Json> this_context(context, this->keyword_name());

                std::string message("Minimum number of properties is " + std::to_string(min_properties_));
                message.append(" but found " + std::to_string(instance.size()));
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    std::move(message)));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class conditional_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        schema_validator_ptr_type if_val_;
        schema_validator_ptr_type then_val_;
        schema_validator_ptr_type else_val_;

    public:
        conditional_validator(const Json& schema, const uri& schema_location,
            const std::string& custom_message,
            schema_validator_ptr_type&& if_val,
            schema_validator_ptr_type&& then_val,
            schema_validator_ptr_type&& else_val
        ) : keyword_validator<Json>("", schema, std::move(schema_location), custom_message), 
              if_val_(std::move(if_val)), 
              then_val_(std::move(then_val)), 
              else_val_(std::move(else_val))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            if (if_val_) 
            {
                collecting_error_listener local_reporter;
                evaluation_results local_results;
                
                eval_context<Json> if_context(context, "if");
                walk_result result = if_val_->validate(if_context, instance, instance_location, local_results, local_reporter, patch);
                if (result == walk_result::abort)
                {
                    return result;
                }
                //std::cout << "if: evaluated properties\n";
                //for (auto& item : results.evaluated_properties)
                //{
                //    std::cout << "  " << item << "\n";
                //}
                if (local_reporter.errors.empty()) 
                {
                    results.merge(local_results);
                    if (then_val_)
                    {
                        eval_context<Json> then_context(context, "then");
                        result = then_val_->validate(then_context, instance, instance_location, results, reporter, patch);
                        if (result == walk_result::abort)
                        {
                            return result;
                        }
                        //std::cout << "then: evaluated properties\n";
                        //for (auto& item : results.evaluated_properties)
                        //{
                        //    std::cout << "  " << item << "\n";
                        //}
                    }
                } 
                else 
                {
                    if (else_val_)
                    {
                        eval_context<Json> else_context(context, "else");
                        result = else_val_->validate(else_context, instance, instance_location, results, reporter, patch);
                        if (result == walk_result::abort)
                        {
                            return result;
                        }
                        //std::cout << "else: evaluated properties\n";
                        //for (auto& item : results.evaluated_properties)
                        //{
                        //    std::cout << "  " << item << "\n";
                        //}
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final 
        {
            walk_result result = walk_result::advance;
            if (if_val_) 
            {
                eval_context<Json> if_context(context, "if");
                result = if_val_->walk(if_context, instance, instance_location, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
                
                if (then_val_)
                {
                    eval_context<Json> then_context(context, "then");
                    result = then_val_->walk(then_context, instance, instance_location, reporter);
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
                if (else_val_)
                {
                    eval_context<Json> else_context(context, "else");
                    result = else_val_->walk(else_context, instance, instance_location, reporter);
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }
    };

    // enum_validator

    template <typename Json>
    class enum_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        Json value_;

    public:
        enum_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, const Json& sch)
            : keyword_validator<Json>("enum", schema, schema_location, custom_message), value_(sch)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            eval_context<Json> this_context(context, this->keyword_name());

            bool in_range = false;
            for (const auto& item : value_.array_range())
            {
                if (item == instance) 
                {
                    in_range = true;
                    break;
                }
            }

            if (!in_range)
            {
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    "'" + instance.template as<std::string>() + "' is not a valid enum value."));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // const_validator

    template <typename Json>
    class const_validator : public keyword_validator<Json>
    {        
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        Json value_;

    public:
        const_validator(const Json& schema, const uri& schema_location, const std::string& custom_message, const Json& sch)
            : keyword_validator<Json>("const", schema, schema_location, custom_message), value_(sch)
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter,
            Json& /*patch*/) const final
        {
            if (value_ != instance)
            {
                eval_context<Json> this_context(context, this->keyword_name());

                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    "Instance is not const"));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    enum class json_schema_type{null,object,array,string,boolean,integer,number};

    inline
    std::string to_string(json_schema_type type)
    {
        switch (type)
        {
            case json_schema_type::null:
                return "null";
            case json_schema_type::object:
                return "object";
            case json_schema_type::array:
                return "array";
            case json_schema_type::string:  // OK
                return "string";
            case json_schema_type::boolean: // OK
                return "boolean";
            case json_schema_type::integer:
                return "integer";
            case json_schema_type::number:
                return "number";
            default:
                return "unknown";
        }

    }

    template <typename Json>
    class type_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::vector<json_schema_type> expected_types_;

    public:
        type_validator(const type_validator&) = delete;
        type_validator& operator=(const type_validator&) = delete;
        type_validator(type_validator&&) = default;
        type_validator& operator=(type_validator&&) = default;

        type_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
            std::vector<json_schema_type>&& expected_types)
            : keyword_validator<Json>("type", schema, std::move(schema_location), custom_message),
              expected_types_(std::move(expected_types))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/,
            error_reporter& reporter, 
            Json& /*patch*/) const final
        {
            //std::cout << "type_validator.do_validate " << context.eval_path().string() << instance << "\n";
            //for (auto& type : expected_types_ )
            //{
            //    std::cout << "    " << to_string(type) << "\n";
            //}

            eval_context<Json> this_context(context, this->keyword_name());

            bool is_type_found = expected_types_.empty();

            auto end = expected_types_.end();
            for (auto it = expected_types_.begin(); it != end && !is_type_found; ++it)
            {
                switch (*it)
                {
                    case json_schema_type::null:
                        if (instance.is_null()) // OK
                        {
                            is_type_found = true;
                        }
                        break;
                    case json_schema_type::object:
                        if (instance.is_object()) // OK
                        {
                            is_type_found = true;
                        }
                        break;
                    case json_schema_type::array:
                        if (instance.is_array())  // OK
                        {
                            is_type_found = true;
                        }
                        break;
                    case json_schema_type::string:  // OK
                        if (instance.is_string() && instance.tag() != semantic_tag::bigint)
                        {
                            is_type_found = true;
                        }
                        break;
                    case json_schema_type::boolean: // OK
                        if (instance.is_bool())
                        {
                            is_type_found = true;
                        }
                        break;
                    case json_schema_type::integer:
                        if (instance.is_int64() || instance.is_uint64())
                        {
                            is_type_found = true;
                        }
                        else if (instance.is_double() && static_cast<double>(instance.template as<int64_t>()) == instance.template as<double>())
                        {
                            is_type_found = true;
                        }
                        else if (instance.is_string() && instance.tag() == semantic_tag::bigint)
                        {
                            is_type_found = true;
                        }
                        break;
                    case json_schema_type::number:
                        if (instance.is_number())
                        {
                            is_type_found = true;
                        }
                        break;
                    default:
                        break;
                }
            }

            if (!is_type_found)
            {
                std::string message = "Expected ";
                for (std::size_t i = 0; i < expected_types_.size(); ++i)
                {
                        if (i > 0)
                        { 
                            message.append(", ");
                            if (i+1 == expected_types_.size())
                            { 
                                message.append("or ");
                            }
                        }
                        message.append(to_string(expected_types_[i]));
                }
                message.append(", found ");
                message.append(to_schema_type(instance.type()));

                return reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    message));
            }
            return walk_result::advance;
        }
        
        std::string to_schema_type(json_type type) const
        {
            switch (type)
            {
                case json_type::null_value:
                {
                    return "null";
                }
                case json_type::bool_value:
                {
                    return "boolean";
                }
                case json_type::int64_value:
                case json_type::uint64_value:
                {
                    return "integer";
                }
                case json_type::half_value:
                case json_type::double_value:
                {
                    return "number";
                }
                case json_type::string_value:
                {
                    return "string";
                }
                case json_type::array_value:
                {
                    return "array";
                }
                case json_type::object_value:
                {
                    return "object";
                }
                default:
                {
                    return "unsupported type";
                }
            }
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class properties_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::map<std::string, schema_validator_ptr_type> properties_;
    public:
        properties_validator(const properties_validator&) = delete;
        properties_validator& operator=(const properties_validator&) = delete;
        properties_validator(properties_validator&&) = default;
        properties_validator& operator=(properties_validator&&) = default;

        properties_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
            std::map<std::string, schema_validator_ptr_type>&& properties)
            : keyword_validator<Json>("properties", schema, std::move(schema_location), custom_message),
              properties_(std::move(properties))
        {
        }

        walk_result validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch,
            std::unordered_set<std::string>& allowed_properties) const 
        {
            //std::cout << "properties_validator begin[" << context.eval_path().string() << "," << this->schema_location().string() << "]\n";
            if (!instance.is_object())
            {
                return walk_result::advance;
            }

            //std::cout << "results:\n";
            //for (const auto& s : results)
            //{
            //    std::cout << "    " << s << "\n";
            //}
            //std::cout << "\n";

            eval_context<Json> this_context(context, this->keyword_name());

            for (const auto& prop : instance.object_range()) 
            {
                auto prop_it = properties_.find(prop.key());

                // check if it is in "properties"
                if (prop_it != properties_.end()) 
                {
                    eval_context<Json> prop_context{this_context, prop.key(), evaluation_flags{}};
                    jsonpointer::json_pointer prop_location = instance_location / prop.key();

                    std::size_t errors = reporter.error_count();
                    walk_result result = (*prop_it).second->validate(prop_context, prop.value(), prop_location, results, reporter, patch);
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                    allowed_properties.insert(prop.key());
                    if (errors == reporter.error_count())
                    {
                        if (context.require_evaluated_properties())
                        {
                            results.evaluated_properties.insert(prop.key());
                        }
                    }
                }
            }
            // Any property that doesn't match any of the property names in the properties keyword is ignored by this keyword.

            // reverse search
            for (auto const& prop : properties_) 
            {
                //std::cout << "   prop:" << prop.first << "\n";
                const auto finding = instance.find(prop.first);
                if (finding == instance.object_range().end()) 
                { 
                    // If prop is not in instance
                    auto default_value = prop.second->get_default_value();
                    if (default_value) 
                    { 
                        // If default value is available, update patch
                        jsonpointer::json_pointer prop_location = instance_location / prop.first;

                        update_patch(patch, prop_location, std::move(*default_value));
                    }
                }
            }
            //std::cout << "properties_validator end[" << context.eval_path().string() << "," << this->schema_location().string() << "]";
            //std::cout << "results:\n";
            //for (const auto& s : results)
            //{
            //    std::cout << "    " << s << "\n";
            //}
            //std::cout << "\n";
            return walk_result::advance;
        }

        walk_result walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter,
            std::unordered_set<std::string>& allowed_properties) const
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }

            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }

            for (const auto& prop : instance.object_range()) 
            {
                auto prop_it = properties_.find(prop.key());

                if (prop_it != properties_.end()) 
                {
                    jsonpointer::json_pointer prop_location = instance_location / prop.key();
                    result = (*prop_it).second->walk(context, prop.value(), prop_location, reporter);
                    allowed_properties.insert(prop.key());
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            std::unordered_set<std::string> allowed_properties;
            return validate(context, instance, instance_location, results, reporter, patch, allowed_properties);
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final 
        {
            std::unordered_set<std::string> allowed_properties;
            return walk(context, instance, instance_location, reporter, allowed_properties);
        }

        void update_patch(Json& patch, const jsonpointer::json_pointer& instance_location, Json&& default_value) const
        {
            Json j;
            j.try_emplace("op", "add"); 
            j.try_emplace("path", instance_location.string()); 
            j.try_emplace("value", std::forward<Json>(default_value)); 
            patch.push_back(std::move(j));
        }
    };

    template <typename Json>
    class pattern_properties_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::vector<std::pair<std::regex, schema_validator_ptr_type>> pattern_properties_;

    public:
        pattern_properties_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
            std::vector<std::pair<std::regex, schema_validator_ptr_type>>&& pattern_properties)
            : keyword_validator<Json>("patternProperties", schema, std::move(schema_location), custom_message),
              pattern_properties_(std::move(pattern_properties))
        {
        }

        walk_result validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, 
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch,
            std::unordered_set<std::string>& allowed_properties) const 
        {
            (void)context;
            (void)instance;
            (void)instance_location;
            (void)results;
            (void)reporter;
            (void)patch;
            (void)allowed_properties;
#if defined(JSONCONS_HAS_STD_REGEX)
            if (!instance.is_object())
            {
                return walk_result::advance;
            }
            eval_context<Json> this_context(context, this->keyword_name());
            for (const auto& prop : instance.object_range()) 
            {
                eval_context<Json> prop_context{this_context, prop.key(), evaluation_flags{}};
                jsonpointer::json_pointer prop_location = instance_location / prop.key();

                // check all matching "patternProperties"
                for (auto& schema_pp : pattern_properties_)
                {
                    if (std::regex_search(prop.key(), schema_pp.first)) 
                    {
                        allowed_properties.insert(prop.key());
                        std::size_t errors = reporter.error_count();
                        walk_result result = schema_pp.second->validate(prop_context, prop.value() , prop_location, results, reporter, patch);
                        if (result == walk_result::abort)
                        {
                            return result;
                        }
                        if (errors == reporter.error_count())
                        {
                            if (context.require_evaluated_properties())
                            {
                                results.evaluated_properties.insert(prop.key());
                            }
                        }
                    }
                }
            }
#endif
            return walk_result::advance;
        }

        walk_result walk(const eval_context<Json>& context, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter,
            std::unordered_set<std::string>& allowed_properties) const
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }
            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }
            (void)context;
#if defined(JSONCONS_HAS_STD_REGEX)
            eval_context<Json> this_context(context, this->keyword_name());
            for (const auto& prop : instance.object_range()) 
            {
                eval_context<Json> prop_context{this_context, prop.key(), evaluation_flags{}};
                jsonpointer::json_pointer prop_location = instance_location / prop.key();

                // check all matching "patternProperties"
                for (auto& schema_pp : pattern_properties_)
                {
                    if (std::regex_search(prop.key(), schema_pp.first)) 
                    {
                        allowed_properties.insert(prop.key());
                        result = schema_pp.second->walk(prop_context, prop.value() , prop_location, reporter);
                        if (result == walk_result::abort)
                        {
                            return result;
                        }
                    }
                }
            }
#endif
            return walk_result::advance;
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            std::unordered_set<std::string> allowed_properties;
            return validate(context, instance, instance_location, results, reporter, patch, allowed_properties);
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            std::unordered_set<std::string> allowed_properties;
            return walk(context,instance, instance_location, reporter, allowed_properties);
        }
    };

    template <typename Json>
    class additional_properties_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::unique_ptr<properties_validator<Json>> properties_; 
        std::unique_ptr<pattern_properties_validator<Json>> pattern_properties_;
        schema_validator_ptr_type additional_properties_;

    public:
        additional_properties_validator(const Json& schema, 
            const uri& schema_location,
            const std::string& custom_message,
            std::unique_ptr<properties_validator<Json>>&& properties,
            std::unique_ptr<pattern_properties_validator<Json>>&& pattern_properties,
            schema_validator_ptr_type&& additional_properties
        )
            : keyword_validator<Json>("additionalProperties", schema, std::move(schema_location),
                custom_message), 
              properties_(std::move(properties)),
              pattern_properties_(std::move(pattern_properties)),
              additional_properties_(std::move(additional_properties))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }

            std::unordered_set<std::string> allowed_properties;

            if (properties_)
            {
                walk_result result = properties_->validate(context, instance, instance_location, results, reporter, patch, allowed_properties);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            if (pattern_properties_)
            {
                walk_result result = pattern_properties_->validate(context, instance, instance_location, results, reporter, patch, allowed_properties);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            if (additional_properties_)
            {
                eval_context<Json> this_context(context, this->keyword_name());
                if (additional_properties_->always_fails())
                {
                    for (const auto& prop : instance.object_range()) 
                    {
                        eval_context<Json> prop_context{this_context, prop.key(), evaluation_flags{}};
                        jsonpointer::json_pointer prop_location = instance_location / prop.key();
                        // check if it is in "allowed properties"
                        auto prop_it = allowed_properties.find(prop.key());
                        if (prop_it == allowed_properties.end()) 
                        {
                            walk_result result = reporter.error(this->make_validation_message(
                                prop_context.eval_path(),
                                prop_location, 
                                "Additional property '" + prop.key() + "' not allowed by schema."));
                            if (result == walk_result::abort)
                            {
                                return result;
                            }
                            break;
                        }
                    }
                }
                else if (additional_properties_->always_succeeds())
                {
                    if (context.require_evaluated_properties())
                    {
                        for (const auto& prop : instance.object_range()) 
                        {
                            results.evaluated_properties.insert(prop.key());
                        }
                    }
                }
                else
                {
                    for (const auto& prop : instance.object_range()) 
                    {
                        // check if it is in "allowed properties"
                        auto prop_it = allowed_properties.find(prop.key());
                        if (prop_it == allowed_properties.end()) 
                        {
                            eval_context<Json> prop_context{this_context, prop.key(), evaluation_flags{}};
                            jsonpointer::json_pointer prop_location = instance_location / prop.key();

                            // finally, check "additionalProperties" 
                            //std::cout << "additional_properties_validator a_prop_or_pattern_matched " << a_prop_or_pattern_matched << ", " << bool(additional_properties_);
                            
                            //std::cout << " !!!additionalProperties!!!";
                            collecting_error_listener local_reporter;

                            walk_result result = additional_properties_->validate(prop_context, prop.value() , prop_location, results, local_reporter, patch);
                            if (result == walk_result::abort)
                            {
                                return result;
                            }
                            if (!local_reporter.errors.empty())
                            {
                                result = reporter.error(this->make_validation_message(
                                    this_context.eval_path(),
                                    instance_location, 
                                    "Additional property '" + prop.key() + "' found but was invalid."));
                                if (result == walk_result::abort)
                                {
                                    return result;
                                }
                            }
                            else if (context.require_evaluated_properties())
                            {
                                results.evaluated_properties.insert(prop.key());
                            }
                            
                        }
                        //std::cout << "\n";
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }
            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }

            std::unordered_set<std::string> allowed_properties;
            if (properties_)
            {
                result = properties_->walk(context, instance, instance_location, reporter, allowed_properties);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            if (pattern_properties_)
            {
                result = pattern_properties_->walk(context, instance, instance_location, reporter, allowed_properties);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            if (additional_properties_)
            {
                eval_context<Json> this_context(context, this->keyword_name());
                for (const auto& prop : instance.object_range()) 
                {
                    // check if it is in "allowed properties"
                    auto prop_it = allowed_properties.find(prop.key());
                    if (prop_it == allowed_properties.end()) 
                    {
                        eval_context<Json> prop_context{this_context, prop.key(), evaluation_flags{}};
                        jsonpointer::json_pointer prop_location = instance_location / prop.key();

                        result = additional_properties_->walk(prop_context, prop.value() , prop_location, reporter);
                        if (result == walk_result::abort)
                        {
                            return result;
                        }
                    }
                }
            }
            return walk_result::advance;
        }
    };

    template <typename Json>
    class dependent_required_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::map<std::string, keyword_validator_ptr_type> dependent_required_;

    public:
        dependent_required_validator(const Json& schema, const uri& schema_location,
            std::map<std::string, keyword_validator_ptr_type>&& dependent_required,
            const std::string& custom_message = std::string{}
        )
            : keyword_validator<Json>("dependentRequired", schema, std::move(schema_location),
                custom_message), 
              dependent_required_(std::move(dependent_required))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            for (const auto& dep : dependent_required_) 
            {
                auto prop = instance.find(dep.first);
                if (prop != instance.object_range().end()) 
                {
                    // if dependency-prop is present in instance
                    jsonpointer::json_pointer prop_location = instance_location / dep.first;
                    walk_result result = dep.second->validate(this_context, instance, prop_location, results, reporter, patch); // validate
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }

            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            for (const auto& dep : dependent_required_) 
            {
                auto prop = instance.find(dep.first);
                if (prop != instance.object_range().end()) 
                {
                    // if dependency-prop is present in instance
                    result = dep.second->walk(this_context, instance, instance_location / dep.first, reporter); 
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }
    };

    template <typename Json>
    class dependent_schemas_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::map<std::string, schema_validator_ptr_type> dependent_schemas_;

    public:
        dependent_schemas_validator(const Json& schema, const uri& schema_location,
            const std::string& custom_message,
            std::map<std::string, schema_validator_ptr_type>&& dependent_schemas
        )
            : keyword_validator<Json>("dependentSchemas", schema, std::move(schema_location),
                custom_message), 
              dependent_schemas_(std::move(dependent_schemas))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            for (const auto& dep : dependent_schemas_) 
            {
                auto prop = instance.find(dep.first);
                if (prop != instance.object_range().end()) 
                {
                    // if dependency-prop is present in instance
                    jsonpointer::json_pointer prop_location = instance_location / dep.first;
                    walk_result result = dep.second->validate(this_context, instance, prop_location, results, reporter, patch); // validate
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }
            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            for (const auto& dep : dependent_schemas_) 
            {
                auto prop = instance.find(dep.first);
                if (prop != instance.object_range().end()) 
                {
                    // if dependency-prop is present in instance
                    result = dep.second->walk(this_context, instance, instance_location / dep.first, reporter);
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }
    };

    template <typename Json>
    class property_names_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        schema_validator_ptr_type schema_val_;

    public:
        property_names_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
            schema_validator_ptr_type&& schema_val)
            : keyword_validator<Json>("propertyNames", schema, schema_location, custom_message), 
                schema_val_{ std::move(schema_val) }
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            if (instance.size() > 0 && schema_val_)
            {
                if (schema_val_->always_fails())
                {
                    jsonpointer::json_pointer item_location = instance_location / 0;
                    return reporter.error(this->make_validation_message(
                        this_context.eval_path(),
                        instance_location, 
                        "Instance has properties but the schema does not allow any property names."));
                }
                else if (schema_val_->always_succeeds())
                {
                    return walk_result::advance;
                }
                else
                {
                    for (const auto& prop : instance.object_range()) 
                    {
                        jsonpointer::json_pointer prop_location = instance_location / prop.key();

                        walk_result result = schema_val_->validate(this_context, prop.key() , instance_location, results, reporter, patch);
                        if (result == walk_result::abort)
                        {
                            return result;
                        }
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }
            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            if (instance.size() > 0 && schema_val_)
            {
                for (const auto& prop : instance.object_range()) 
                {
                    result = schema_val_->walk(this_context, prop.key(), instance_location / prop.key(), reporter);
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }
    };

    template <typename Json>
    class dependencies_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::map<std::string, keyword_validator_ptr_type> dependent_required_;
        std::map<std::string, schema_validator_ptr_type> dependent_schemas_;

    public:
        dependencies_validator(const Json& schema, const uri& schema_location,
            std::map<std::string, keyword_validator_ptr_type>&& dependent_required,
            std::map<std::string, schema_validator_ptr_type>&& dependent_schemas,
            const std::string& custom_message = std::string{}
        )
            : keyword_validator<Json>("dependencies", schema, std::move(schema_location),
                custom_message), 
              dependent_required_(std::move(dependent_required)),
              dependent_schemas_(std::move(dependent_schemas))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            if (!instance.is_object())
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            for (const auto& dep : dependent_required_) 
            {
                auto prop = instance.find(dep.first);
                if (prop != instance.object_range().end()) 
                {
                    // if dependency-prop is present in instance
                    jsonpointer::json_pointer prop_location = instance_location / dep.first;
                    walk_result result = dep.second->validate(this_context, instance, prop_location, results, reporter, patch); // validate
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }

            for (const auto& dep : dependent_schemas_) 
            {
                auto prop = instance.find(dep.first);
                if (prop != instance.object_range().end()) 
                {
                    // if dependency-prop is present in instance
                    jsonpointer::json_pointer prop_location = instance_location / dep.first;
                    walk_result result = dep.second->validate(this_context, instance, prop_location, results, reporter, patch); // validate
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class max_contains_keyword : public keyword_base<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::size_t max_value_;
    public:
        max_contains_keyword(const Json& schema, const uri& schema_location, const std::string& custom_message, std::size_t max_value)
            : keyword_base<Json>("maxContains", schema, schema_location, custom_message), max_value_(max_value)
        {
        }

        walk_result validate(const eval_context<Json>& context, 
            const jsonpointer::json_pointer& instance_location,
            std::size_t count, 
            error_reporter& reporter) const 
        {
            eval_context<Json> this_context(context, this->keyword_name());

            if (count > max_value_)
            {
                std::string message("A schema can match a contains constraint at most " + std::to_string(max_value_) + " times");
                message.append(" but it matched " + std::to_string(count) + " times.");
                
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    std::move(message)));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    // minItems

    template <typename Json>
    class min_contains_keyword : public keyword_base<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::size_t min_value_;
    public:
        min_contains_keyword(const Json& schema, const uri& schema_location, const std::string& custom_message, 
            std::size_t min_value)
            : keyword_base<Json>("minContains", schema, schema_location, custom_message), min_value_(min_value)
        {
        }

        walk_result validate(const eval_context<Json>& context, 
            const jsonpointer::json_pointer& instance_location,
            std::size_t count, 
            error_reporter& reporter) const 
        {
            eval_context<Json> this_context(context, this->keyword_name());

            if (count < min_value_)
            {
                std::string message("A schema must match a contains constraint at least " + std::to_string(min_value_) + " times");
                message.append(" but it matched " + std::to_string(count) + " times.");
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(),
                    instance_location, 
                    std::move(message)));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const 
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class contains_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        schema_validator_ptr_type schema_validator_;
        std::unique_ptr<max_contains_keyword<Json>> max_contains_;
        std::unique_ptr<min_contains_keyword<Json>> min_contains_;

    public:
        contains_validator(const Json& schema, const uri& schema_location, const std::string& custom_message,
            schema_validator_ptr_type&& schema_validator,
            std::unique_ptr<max_contains_keyword<Json>>&& max_contains,
            std::unique_ptr<min_contains_keyword<Json>>&& min_contains)
            : keyword_validator<Json>("contains", schema, std::move(schema_location), custom_message), 
              schema_validator_(std::move(schema_validator)),
              max_contains_(std::move(max_contains)),
              min_contains_(std::move(min_contains))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }

            if (!schema_validator_) 
            {
                return walk_result::advance;
            }

            eval_context<Json> this_context(context, this->keyword_name());

            std::size_t contains_count = 0;
            collecting_error_listener local_reporter;

            std::size_t index = 0;
            std::size_t start = 0;
            std::size_t end = 0;
            for (const auto& item : instance.array_range()) 
            {
                std::size_t errors = local_reporter.errors.size();
                walk_result result = schema_validator_->validate(this_context, item, instance_location / index, results, local_reporter, patch);
                if (result == walk_result::abort)
                {
                    return result;
                }
                if (errors == local_reporter.errors.size())
                {
                    if (context.require_evaluated_items())
                    {
                        if (end == start)
                        {
                            start = end = index;
                        }
                        ++end;
                    }
                    ++contains_count;
                }
                else
                {
                    if (start < end)
                    {
                        results.evaluated_items.insert(range{start, end});
                        start = end;
                    }
                }
                ++index;
            }
            if (start < end)
            {
                results.evaluated_items.insert(range{start, end});
                start = end;
            }
            
            if (max_contains_ || min_contains_)
            {
                if (max_contains_)
                {
                    walk_result result = max_contains_->validate(this_context, instance_location, contains_count, reporter);
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
                if (min_contains_)
                {
                    walk_result result = min_contains_->validate(this_context, instance_location, contains_count, reporter);
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            else if (contains_count == 0)
            {
                walk_result result = reporter.error(this->make_validation_message(
                    this_context.eval_path(), 
                    instance_location, 
                    "Expected at least one array item to match 'contains' schema.",
                    local_reporter.errors));
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const override 
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }

            if (!schema_validator_) 
            {
                return walk_result::advance;
            }

            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }

            eval_context<Json> this_context(context, this->keyword_name());
            
            for (std::size_t index = 0; index < instance.size(); ++index)
            {
                result = schema_validator_->walk(this_context, instance.at(index), instance_location / index, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            if (max_contains_)
            {
                result = max_contains_->walk(this_context, instance, instance_location, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            if (min_contains_)
            {
                result = min_contains_->walk(this_context, instance, instance_location, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }
    };

    template <typename Json>
    class items_keyword : public keyword_base<Json>
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        schema_validator_ptr_type items_val_;
    public:
        items_keyword(const std::string& keyword_name, const Json& schema, 
            const uri& schema_location, const std::string& custom_message, 
            schema_validator_ptr_type&& items_val)
            : keyword_base<Json>(keyword_name, schema, schema_location, custom_message), items_val_(std::move(items_val))
        {
        }

        walk_result validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter,
            Json& patch,
            std::size_t data_index) const 
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }
            if (data_index < instance.size() && items_val_)
            {
                eval_context<Json> items_context(context, this->keyword_name());
                if (items_val_->always_fails())
                {
                    jsonpointer::json_pointer item_location = instance_location / data_index;
                    walk_result result = reporter.error(this->make_validation_message(
                        items_context.eval_path(), 
                        item_location,
                        "Extra item at index '" + std::to_string(data_index) + "' but the schema does not allow extra items."));
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
                else if (items_val_->always_succeeds())
                {
                    results.evaluated_items.insert(range{0,instance.size()});
                }
                else
                {
                    std::size_t start = 0;
                    std::size_t end = 0;
                    for (; data_index < instance.size(); ++data_index)
                    {
                        jsonpointer::json_pointer item_location = instance_location / data_index;
                        std::size_t errors = reporter.error_count();
                        walk_result result = items_val_->validate(items_context, instance[data_index], item_location, results, reporter, patch);
                        if (result == walk_result::abort)
                        {
                            return result;
                        }
                        if (errors == reporter.error_count())
                        {
                            if (context.require_evaluated_items())
                            {
                                if (end == start)
                                {
                                    start = end = data_index;
                                }
                                ++end;
                            }
                        }
                        else
                        {
                            if (start < end)
                            {
                                results.evaluated_items.insert(range{start, end});
                                start = end;
                            }
                        }
                    }
                    
                    if (start < end)
                    {
                        results.evaluated_items.insert(range{start, end});
                        start = end;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result walk(const eval_context<Json>& context, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter,
            std::size_t data_index) const
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }
            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }
            if (data_index < instance.size() && items_val_)
            {
                eval_context<Json> items_context(context, this->keyword_name());
                for (; data_index < instance.size(); ++data_index)
                {
                    jsonpointer::json_pointer item_location = instance_location / data_index;
                    result = items_val_->walk(items_context, instance[data_index], item_location, reporter);
                    if (result == walk_result::abort)
                    {
                        return result;
                    }
                }
            }
            return walk_result::advance;
        }
    };

    template <typename Json>
    class prefix_items_validator : public keyword_validator<Json>
    {
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::vector<schema_validator_ptr_type> prefix_item_validators_;
        std::unique_ptr<items_keyword<Json>> items_val_;
    public:
        prefix_items_validator(const std::string& keyword_name, const Json& schema, const uri& schema_location, const std::string& custom_message, 
            std::vector<schema_validator_ptr_type>&& prefix_item_validators,
            std::unique_ptr<items_keyword<Json>>&& items_val)
            : keyword_validator<Json>(keyword_name, schema, schema_location, custom_message), 
              prefix_item_validators_(std::move(prefix_item_validators)), 
              items_val_(std::move(items_val))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter,
            Json& patch) const final
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }
        
            std::size_t data_index = 0;
        
            eval_context<Json> prefix_items_context(context, this->keyword_name());

            std::size_t start = 0;
            std::size_t end = 0;
            for (std::size_t schema_index=0; 
                  schema_index < prefix_item_validators_.size() && data_index < instance.size(); 
                  ++schema_index, ++data_index) 
            {
                auto& val = prefix_item_validators_[schema_index];
                eval_context<Json> item_context{prefix_items_context, schema_index, evaluation_flags{}};
                jsonpointer::json_pointer item_location = instance_location / data_index;
                std::size_t errors = reporter.error_count();
                walk_result result = val->validate(item_context, instance[data_index], item_location, results, reporter, patch);
                if (result == walk_result::abort)
                {
                    return result;
                }
                if (errors == reporter.error_count())
                {
                    if (context.require_evaluated_items())
                    {
                        if (end == start)
                        {
                            start = end = data_index;
                        }
                        ++end;
                    }
                }
                else
                {
                    if (start < end)
                    {
                        results.evaluated_items.insert(range{start, end});
                        start = end;
                    }
                }

            }
            if (start < end)
            {
                results.evaluated_items.insert(range{start, end});
            }
            
            if (data_index < instance.size() && items_val_)
            {
                walk_result result = items_val_->validate(context, instance, instance_location, results, reporter, patch, data_index);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final 
        {
            if (!instance.is_array())
            {
                return walk_result::advance;
            }

            walk_result result = reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
            if (result == walk_result::abort)
            {
                return result;
            }

            std::size_t data_index = 0;

            eval_context<Json> prefix_items_context(context, this->keyword_name());

            for (std::size_t schema_index=0; 
                  schema_index < prefix_item_validators_.size() && data_index < instance.size(); 
                  ++schema_index, ++data_index) 
            {
                auto& val = prefix_item_validators_[schema_index];
                eval_context<Json> item_context{prefix_items_context, schema_index, evaluation_flags{}};
                jsonpointer::json_pointer item_location = instance_location / data_index;
                result = val->walk(item_context, instance[data_index], item_location, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            if (data_index < instance.size() && items_val_)
            {
                items_val_->walk(context, instance, instance_location, reporter, data_index);
            }
            return walk_result::advance;
        }
    };

    template <typename Json>
    class unevaluated_properties_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        schema_validator_ptr_type schema_val_;

    public:
        unevaluated_properties_validator(const Json& schema, 
            const uri& schema_location,
            const std::string& custom_message,
            schema_validator_ptr_type&& schema_val)
            : keyword_validator<Json>("unevaluatedProperties", schema, std::move(schema_location),
                custom_message), 
              schema_val_(std::move(schema_val))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            //std::cout << "unevaluated_properties_validator [" << context.eval_path().string() << "," << this->schema_location().string() << "]";
            //std::cout << "results:\n";
            //for (const auto& s : results.evaluated_properties_)
            //{
            //    std::cout << "    " << s << "\n";
            //}
            //std::cout << "\n";
            if (!instance.is_object())
            {
                return walk_result::advance;
            }

            if (schema_val_)
            {
                eval_context<Json> this_context(context, this->keyword_name());
                if (schema_val_->always_fails())
                {
                    for (const auto& prop : instance.object_range()) 
                    {
                        // check if it is in "evaluated_properties"
                        auto prop_it = results.evaluated_properties.find(prop.key());
                        if (prop_it == results.evaluated_properties.end()) 
                        {
                            eval_context<Json> prop_context{this_context, prop.key(), evaluation_flags{}};
                            jsonpointer::json_pointer prop_location = instance_location / prop.key();

                            walk_result result = reporter.error(this->make_validation_message(
                                prop_context.eval_path(), 
                                prop_location,
                                "Unevaluated property '" + prop.key() + "' but the schema does not allow unevaluated properties."));
                            if (result == walk_result::abort)
                            {
                                return result;
                            }
                            break;
                        }
                    }
                }
                else if (schema_val_->always_succeeds())
                {
                    if (context.require_evaluated_properties())
                    {
                        for (const auto& prop : instance.object_range()) 
                        {
                            results.evaluated_properties.insert(prop.key());
                        }
                    }
                }
                else
                {
                    for (const auto& prop : instance.object_range()) 
                    {
                        // check if it is in "evaluated_properties"
                        auto prop_it = results.evaluated_properties.find(prop.key());
                        if (prop_it == results.evaluated_properties.end()) 
                        {
                            //std::cout << "Not in evaluated properties: " << prop.key() << "\n";
                            const std::size_t error_count = reporter.error_count();
                            walk_result result = schema_val_->validate(this_context, prop.value() , instance_location, results, reporter, patch);
                            if (result == walk_result::abort)
                            {
                                return result;
                            }
                            if (reporter.error_count() == error_count)
                            {
                                if (context.require_evaluated_properties())
                                {
                                    results.evaluated_properties.insert(prop.key());
                                }
                            }
                        }
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

    template <typename Json>
    class unevaluated_items_validator : public keyword_validator<Json>
    {
        using keyword_validator_ptr_type = typename keyword_validator<Json>::keyword_validator_ptr_type;
        using schema_validator_ptr_type = typename schema_validator<Json>::schema_validator_ptr_type;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        schema_validator_ptr_type schema_val_;

    public:
        unevaluated_items_validator(const Json& schema, const uri& schema_location,
            const std::string& custom_message,
            schema_validator_ptr_type&& schema_val)
            : keyword_validator<Json>("unevaluatedProperties", schema, std::move(schema_location),
                custom_message), 
              schema_val_(std::move(schema_val))
        {
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            //std::cout << this->keyword_name() << " [" << context.eval_path().string() << ", " << this->schema_location().string() << "]";
            //std::cout << "results:\n";
            //for (const auto& s : results.evaluated_items)
            //{
            //    std::cout << "    " << s << "\n";
            //}
            //std::cout << "\n";
            if (!instance.is_array())
            {
                return walk_result::advance;
            }

            if (schema_val_)
            {
                eval_context<Json> this_context(context, this->keyword_name());
                if (schema_val_->always_fails())
                {
                    for (std::size_t index = 0; index < instance.size(); ++index) 
                    {
                        // check if it is in "evaluated_items"
                        if (!results.evaluated_items.contains(index)) 
                        {
                            eval_context<Json> item_context{this_context, index, evaluation_flags{}};
                            jsonpointer::json_pointer item_location = instance_location / index;
                            //std::cout << "Not in evaluated properties: " << item.key() << "\n";
                            walk_result result = reporter.error(this->make_validation_message(
                                item_context.eval_path(), 
                                item_location,
                                "Unevaluated item at index '" + std::to_string(index) + "' but the schema does not allow unevaluated items."));
                            if (result == walk_result::abort)
                            {
                                return result;
                            }
                            break;
                        }
                    }
                }
                else if (schema_val_->always_succeeds())
                {
                    if (context.require_evaluated_items())
                    {
                        results.evaluated_items.insert(range{0,instance.size()});
                    }
                }
                else
                {
                    std::size_t index = 0;
                    std::size_t start = 0;
                    std::size_t end = 0;
                    for (const auto& item : instance.array_range())
                    {
                        // check if it is in "evaluated_items"
                        if (!results.evaluated_items.contains(index))
                        {
                            eval_context<Json> item_context{this_context, index, evaluation_flags{}};
                            jsonpointer::json_pointer item_location = instance_location / index;
                            //std::cout << "Not in evaluated properties: " << item.key() << "\n";
                            const std::size_t error_count = reporter.error_count();
                            walk_result result = schema_val_->validate(item_context, item, item_location, results, reporter, patch);
                            if (result == walk_result::abort)
                            {
                                return result;
                            }
                            if (reporter.error_count() == error_count)
                            {
                                if (context.require_evaluated_items())
                                {
                                    if (end == start)
                                    {
                                        start = end = index;
                                    }
                                    ++end;
                                }
                            }
                            else
                            {
                                if (start < end)
                                {
                                    results.evaluated_items.insert(range{start, end});
                                    start = end;
                                }
                            }
                        }
                        ++index;
                    }
                    if (start < end)
                    {
                        results.evaluated_items.insert(range{start, end});
                        start = end;
                    }
                }
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance,
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            return reporter(this->keyword_name(), this->schema(), this->schema_location(), instance, instance_location);
        }
    };

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_COMMON_KEYWORD_VALIDATOR_HPP
