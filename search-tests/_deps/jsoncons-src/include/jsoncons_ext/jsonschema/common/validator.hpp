// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_COMMON_VALIDATOR_HPP
#define JSONCONS_EXT_JSONSCHEMA_COMMON_VALIDATOR_HPP

#include <cstddef>
#include <functional>
#include <string>
#include <unordered_set>
#include <vector>

#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/utility/uri.hpp>
#include <jsoncons_ext/jsonschema/common/eval_context.hpp>
#include <jsoncons_ext/jsonschema/jsonschema_error.hpp>
#include <jsoncons_ext/jsonschema/validation_message.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jsoncons {
namespace jsonschema {
    
    enum class walk_result {advance, abort};

    template <typename Json>
    struct json_schema_traits    
    {
        using walk_reporter_type = std::function<walk_result(const std::string& keyword,
            const Json& schema, const uri& schema_location,
            const Json& instance, const jsonpointer::json_pointer& instance_location)>;      
    };

    // Interface for validation error handlers
    class error_reporter
    {
        std::size_t error_count_{0};
    public:
        error_reporter()
        {
        }

        virtual ~error_reporter() = default;

        walk_result error(const validation_message& msg)
        {
            ++error_count_;
            return do_error(msg);
        }

        std::size_t error_count() const
        {
            return error_count_;
        }

    private:
        virtual walk_result do_error(const validation_message& /* e */) = 0;
    };

    struct collecting_error_listener : public error_reporter
    {
        std::vector<validation_message> errors;

    private:
        walk_result do_error(const validation_message& msg) final
        {
            errors.push_back(msg);
            return walk_result::advance;
        }
    };

    class range
    {
        std::size_t start_{0};
        std::size_t end_{0};
    public:
        range()
        {
        }
    
        range(std::size_t start, std::size_t end)
            : start_(start), end_(end)
        {
        }
        
        std::size_t start() const
        {
            return start_;
        }
    
        std::size_t end() const
        {
            return end_;
        }
        
        bool contains(std::size_t index) const
        {
            return index >= start_ && index < end_; 
        }
    };
    
    class range_collection
    {
        std::vector<range> ranges_;
    public:    
        using const_iterator = std::vector<range>::const_iterator;
        using value_type = range;
        
        range_collection()
        {
        }
        range_collection(const range_collection& other) = default;
        range_collection(range_collection&& other) = default;

        range_collection& operator=(const range_collection& other) = default;
        range_collection& operator=(range_collection&& other) = default;
        
        std::size_t size() const
        {
            return ranges_.size();
        }
    
        range operator[](std::size_t index) const
        {
            return ranges_[index];
        }
        
        const_iterator begin() const
        {
            return ranges_.cbegin();
        }

        const_iterator end() const
        {
            return ranges_.cend();
        }
        
        void insert(range index_range)
        {
            ranges_.push_back(index_range);
        }
    
        bool contains(std::size_t index) const
        {
            bool found = false;
            std::size_t length = ranges_.size();
            for (std::size_t i = 0; i < length && !found; ++i)
            {
                if (ranges_[i].contains(index))
                {
                    found = true;
                }
            }
            return found;
        }
    };
    
    struct evaluation_results
    {
        std::unordered_set<std::string> evaluated_properties;
        range_collection evaluated_items;

        void merge(const evaluation_results& results)
        {
            for (auto&& name : results.evaluated_properties)
            {
                evaluated_properties.insert(name);
            }
            for (auto index_range : results.evaluated_items)
            {
                evaluated_items.insert(index_range);
            }
        }
        void merge(std::unordered_set<std::string>&& properties)
        {
            auto end = std::make_move_iterator(properties.end());
            for (auto it = std::make_move_iterator(properties.begin()); it != end; ++it)
            {
                evaluated_properties.insert(*it);
            }
        }
        void merge(const range_collection& ranges)
        {
            for (auto index_range : ranges)
            {
                evaluated_items.insert(index_range);
            }
        }
    };

    template <typename Json>
    class validator_base 
    {
    public:
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        virtual ~validator_base() = default;

        virtual const uri& schema_location() const = 0;

        walk_result validate(const eval_context<Json>& context,
            const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const 
        {
            return do_validate(context, instance, instance_location, results, reporter, patch);
        }

        walk_result walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const 
        {
            return do_walk(context, instance, instance_location, reporter);
        }
        
        virtual bool always_fails() const = 0;

        virtual bool always_succeeds() const = 0;


    private:
        virtual walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const = 0;

        virtual walk_result do_walk(const eval_context<Json>& /*context*/, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const = 0;
   };

    class validation_message_factory
    {
    public:
        virtual ~validation_message_factory() = default;
        
        virtual validation_message make_validation_message(const jsonpointer::json_pointer& eval_path,
            const jsonpointer::json_pointer& instance_location,
            const std::string& message) const = 0;

        virtual validation_message make_validation_message(const jsonpointer::json_pointer& eval_path,
            const jsonpointer::json_pointer& instance_location,
            const std::string& message,
            const std::vector<validation_message>& details) const = 0;
    };

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_KEYWORD_VALIDATOR_HPP
