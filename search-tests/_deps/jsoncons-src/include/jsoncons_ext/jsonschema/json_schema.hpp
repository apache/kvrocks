// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_JSON_SCHEMA_HPP
#define JSONCONS_EXT_JSONSCHEMA_JSON_SCHEMA_HPP

#include <functional>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>

#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonschema/common/schema_validator.hpp>
#include <jsoncons_ext/jsonschema/jsonschema_error.hpp>

namespace jsoncons {
namespace jsonschema {

    class validation_message_to_json_events 
    {
        json_visitor* visitor_ptr_;
    public:
        validation_message_to_json_events(json_visitor& visitor)
            : visitor_ptr_(std::addressof(visitor))
        {
        }

        walk_result operator()(const validation_message& message)
        {
            write_error(message);
            return walk_result::advance;
        }

        void write_error(const validation_message& message)
        {
            visitor_ptr_->begin_object();

            visitor_ptr_->key("valid");
            visitor_ptr_->bool_value(false);

            visitor_ptr_->key("evaluationPath");
            visitor_ptr_->string_value(message.eval_path().string());

            visitor_ptr_->key("schemaLocation");
            visitor_ptr_->string_value(message.schema_location().string());

            visitor_ptr_->key("instanceLocation");
            visitor_ptr_->string_value(message.instance_location().string());

            visitor_ptr_->key("error");
            visitor_ptr_->string_value(message.message());

            if (!message.details().empty())
            {
                visitor_ptr_->key("details");
                visitor_ptr_->begin_array();
                for (const auto& detail : message.details())
                {
                    write_error(detail);
                }
                visitor_ptr_->end_array();
            }

            visitor_ptr_->end_object();
        }
    };

    class throwing_error_listener : public error_reporter
    {
        walk_result do_error(const validation_message& msg) override
        {
            JSONCONS_THROW(validation_error(msg.instance_location().string() + ": " + msg.message()));
        }
    };
  
    class fail_early_reporter : public error_reporter
    {
        walk_result do_error(const validation_message&) override
        {
            return walk_result::abort;
        }
    };

    using error_reporter_t = std::function<walk_result(const validation_message& msg)>;

    struct error_reporter_adaptor : public error_reporter
    {
        error_reporter_t reporter_;

        error_reporter_adaptor(const error_reporter_t& reporter)
            : reporter_(reporter)
        {
        }
    private:
        walk_result do_error(const validation_message& e) override
        {
            return reporter_(e);
        }
    };
       
    template <typename Json>
    class json_validator;
    
    template <typename Json>
    class json_schema
    {
        using keyword_validator_ptr_type = std::unique_ptr<keyword_validator<Json>>;
        using document_schema_validator_type = std::unique_ptr<document_schema_validator<Json>>;

        document_schema_validator_type root_;
        
        friend class json_validator<Json>;
    public:
        json_schema(document_schema_validator_type&& root)
            : root_(std::move(root))
        {
            if (root_ == nullptr)
                JSONCONS_THROW(schema_error("There is no root schema to validate an instance against"));
        }

        json_schema(const json_schema&) = delete;
        json_schema(json_schema&&) = default;
        json_schema& operator=(const json_schema&) = delete;
        json_schema& operator=(json_schema&&) = default;

        // Validate input JSON against a JSON Schema with a default throwing error reporter
        Json validate(const Json& instance) const
        {
            throwing_error_listener reporter;
            jsonpointer::json_pointer instance_location{};
            Json patch(json_array_arg);

            eval_context<Json> context;
            evaluation_results results;
            root_->validate(context, instance, instance_location, results, reporter, patch);
            return patch;
        }

        // Validate input JSON against a JSON Schema 
        bool is_valid(const Json& instance) const
        {
            fail_early_reporter reporter;
            jsonpointer::json_pointer instance_location{};
            Json patch(json_array_arg);

            eval_context<Json> context;
            evaluation_results results;
            root_->validate(context, instance, instance_location, results, reporter, patch);
            return reporter.error_count() == 0;
        }

        // Validate input JSON against a JSON Schema with a provided error reporter
        template <typename MsgReporter>
        typename std::enable_if<extension_traits::is_unary_function_object_exact<MsgReporter,walk_result,validation_message>::value,void>::type
        validate(const Json& instance, const MsgReporter& reporter) const
        {
            jsonpointer::json_pointer instance_location{};
            Json patch(json_array_arg);

            error_reporter_adaptor adaptor(reporter);
            eval_context<Json> context;
            evaluation_results results;
            root_->validate(context, instance, instance_location, results, adaptor, patch);
        }

        // Validate input JSON against a JSON Schema with a provided error reporter
        template <typename MsgReporter>
        typename std::enable_if<extension_traits::is_unary_function_object_exact<MsgReporter,walk_result,validation_message>::value,void>::type
        validate(const Json& instance, MsgReporter&& reporter, Json& patch) const
        {
            jsonpointer::json_pointer instance_location{};
            patch = Json(json_array_arg);

            error_reporter_adaptor adaptor(std::forward<MsgReporter>(reporter));
            eval_context<Json> context;
            evaluation_results results;
            root_->validate(context, instance, instance_location, results, adaptor, patch);
        }

        // Validate input JSON against a JSON Schema with a provided error reporter
        void validate(const Json& instance, Json& patch) const
        {
            jsonpointer::json_pointer instance_location{};
            patch = Json(json_array_arg);

            fail_early_reporter reporter;
            eval_context<Json> context;
            evaluation_results results;
            root_->validate(context, instance, instance_location, results, reporter, patch);
        }

        // Validate input JSON against a JSON Schema with a provided json_visitor
        void validate(const Json& instance, json_visitor& visitor) const
        {
            visitor.begin_array();
            jsonpointer::json_pointer instance_location{};
            Json patch{json_array_arg};

            validation_message_to_json_events adaptor{ visitor };
            eval_context<Json> context;
            evaluation_results results;
            error_reporter_adaptor reporter(adaptor);
            root_->validate(context, instance, instance_location, results, reporter, patch);
            visitor.end_array();
            visitor.flush();
        }
        
        template <typename WalkReporter>
        void walk(const Json& instance, const WalkReporter& reporter) const
        {
            jsonpointer::json_pointer instance_location{};

            root_->walk(eval_context<Json>{}, instance, instance_location, reporter);
        }
        
    private:
        // Validate input JSON against a JSON Schema with a provided error reporter
        void validate2(const Json& instance, error_reporter& reporter, Json& patch) const
        {
            jsonpointer::json_pointer instance_location{};
            patch = Json(json_array_arg);

            eval_context<Json> context;
            evaluation_results results;
            root_->validate(context, instance, instance_location, results, reporter, patch);
        }
    };


} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_SCHEMA_HPP
