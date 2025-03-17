// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_DRAFT202012_SCHEMA_VALIDATOR_FACTORY_202012_HPP
#define JSONCONS_EXT_JSONSCHEMA_DRAFT202012_SCHEMA_VALIDATOR_FACTORY_202012_HPP

#include <cassert>
#include <iostream>
#include <set>
#include <string>
#include <unordered_map>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/utility/uri.hpp>

#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonschema/common/compilation_context.hpp>
#include <jsoncons_ext/jsonschema/common/schema_validator_factory_base.hpp>
#include <jsoncons_ext/jsonschema/common/keyword_validator_factory.hpp>
#include <jsoncons_ext/jsonschema/common/schema_validator.hpp>
#include <jsoncons_ext/jsonschema/draft202012/schema_draft202012.hpp>
#include <jsoncons_ext/jsonschema/json_schema.hpp>

#if defined(JSONCONS_HAS_STD_REGEX)
#include <regex>
#endif

namespace jsoncons {
namespace jsonschema {
namespace draft202012 {

    template <typename Json>
    class schema_validator_factory_202012 : public schema_validator_factory_base<Json> 
    {
    public:
        using schema_store_type = typename schema_validator_factory_base<Json>::schema_store_type;
        using validator_factory_factory_type = typename schema_validator_factory_base<Json>::validator_factory_factory_type;
        using keyword_validator_ptr_type = typename std::unique_ptr<keyword_validator<Json>>;
        using schema_validator_ptr_type = typename std::unique_ptr<schema_validator<Json>>;
        using dynamic_ref_validator_type = dynamic_ref_validator<Json>;
        using anchor_uri_map_type = std::unordered_map<std::string,uri_wrapper>;

        using keyword_factory_type = std::function<keyword_validator_ptr_type(const compilation_context<Json>& context, 
            const Json& sch, const Json& parent, anchor_uri_map_type&)>;

        std::unordered_map<std::string,keyword_factory_type> validation_factory_map_;
        
        static const std::string& core_id()
        {
            static std::string id = "https://json-schema.org/draft/2020-12/vocab/core";
            return id;
        }
        static const std::string& applicator_id()
        {
            static std::string id = "https://json-schema.org/draft/2020-12/vocab/applicator";
            return id;
        }
        static const std::string& unevaluated_id()
        {
            static std::string id = "https://json-schema.org/draft/2020-12/vocab/unevaluated";
            return id;
        }
        static const std::string& validation_id()
        {
            static std::string id = "https://json-schema.org/draft/2020-12/vocab/validation";
            return id;
        }
        static const std::string& meta_data_id()
        {
            static std::string id = "https://json-schema.org/draft/2020-12/vocab/meta-data";
            return id;
        }
        static const std::string& format_annotation_id()
        {
            static std::string id = "https://json-schema.org/draft/2020-12/format-annotation";
            return id;
        }
        static const std::string& content_id()
        {
            static std::string id = "https://json-schema.org/draft/2020-12/vocab/content";
            return id;
        }
        
        bool include_applicator_{true};
        bool include_unevaluated_{true};
        bool include_validation_{true};
        bool include_format_{true};
        keyword_validator_factory<Json> factory_;

    public:
        schema_validator_factory_202012(Json&& sch, const validator_factory_factory_type& factory_factory, 
            evaluation_options options, schema_store_type* schema_store_ptr,
            const std::vector<resolve_uri_type<Json>>& resolve_funcs,
            const std::unordered_map<std::string,bool>& vocabulary) 
            : schema_validator_factory_base<Json>(schema_version::draft202012(), std::move(sch),
                factory_factory, options, schema_store_ptr, resolve_funcs, vocabulary),
              factory_(this)
        {
            if (!vocabulary.empty())
            {
                auto it = vocabulary.find(applicator_id());
                if (it == vocabulary.end() || !((*it).second))
                {
                    include_applicator_ = false;
                }
                it = vocabulary.find(unevaluated_id());
                if (it == vocabulary.end() || !((*it).second))
                {
                    include_unevaluated_ = false;
                }
                it = vocabulary.find(validation_id());
                if (it == vocabulary.end() || !((*it).second))
                {
                    include_validation_ = false;
                }
                it = vocabulary.find(format_annotation_id());
                if (it == vocabulary.end() || !((*it).second))
                {
                    include_format_ = false;
                }
            }
            init();
        }

        schema_validator_factory_202012(const schema_validator_factory_202012&) = delete;
        schema_validator_factory_202012& operator=(const schema_validator_factory_202012&) = delete;
        schema_validator_factory_202012(schema_validator_factory_202012&&) = default;
        schema_validator_factory_202012& operator=(schema_validator_factory_202012&&) = default;

        void init()
        {
            // validation
            validation_factory_map_.emplace("type", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_type_validator(context, sch, parent);});
/*            
            validation_factory_map_.emplace("contentEncoding", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_content_encoding_validator(context, sch, parent);});
            validation_factory_map_.emplace("contentMediaType", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_content_media_type_validator(context, sch, parent);});
*/                
#if defined(JSONCONS_HAS_STD_REGEX)
            validation_factory_map_.emplace("pattern", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_pattern_validator(context, sch, parent);});
#endif
            validation_factory_map_.emplace("maxItems", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_max_items_validator(context, sch, parent);});
            validation_factory_map_.emplace("minItems", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_min_items_validator(context, sch, parent);});
            validation_factory_map_.emplace("maxProperties", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_max_properties_validator(context, sch, parent);});
            validation_factory_map_.emplace("minProperties", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_min_properties_validator(context, sch, parent);});
            validation_factory_map_.emplace("contains", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict)
                        {return factory_.make_contains_validator(context, sch, parent, anchor_dict);});
            validation_factory_map_.emplace("uniqueItems", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_unique_items_validator(context, sch, parent);});
            validation_factory_map_.emplace("maxLength", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_max_length_validator(context, sch, parent);});
            validation_factory_map_.emplace("minLength", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_min_length_validator(context, sch, parent);});
            validation_factory_map_.emplace("not", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict){return factory_.make_not_validator(context, sch, parent, anchor_dict);});
            validation_factory_map_.emplace("maximum", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_maximum_validator(context, sch, parent);});
            validation_factory_map_.emplace("exclusiveMaximum", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_exclusive_maximum_validator(context, sch, parent);});
            validation_factory_map_.emplace("minimum", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_minimum_validator(context, sch, parent);});
            validation_factory_map_.emplace("exclusiveMinimum", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_exclusive_minimum_validator(context, sch, parent);});
            validation_factory_map_.emplace("multipleOf", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_multiple_of_validator(context, sch, parent);});
            validation_factory_map_.emplace("const", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_const_validator(context, sch, parent);});
            validation_factory_map_.emplace("enum", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_enum_validator(context, sch, parent);});
            validation_factory_map_.emplace("allOf", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict){return factory_.make_all_of_validator(context, sch, parent, anchor_dict);});
            validation_factory_map_.emplace("anyOf", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict){return factory_.make_any_of_validator(context, sch, parent, anchor_dict);});
            validation_factory_map_.emplace("oneOf", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict){return factory_.make_one_of_validator(context, sch, parent, anchor_dict);});          
            if (this->options().compatibility_mode())
            {
                validation_factory_map_.emplace("dependencies", 
                    [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict){return factory_.make_dependencies_validator(context, sch, parent, anchor_dict);});
            }
            validation_factory_map_.emplace("required", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_required_validator(context, sch, parent);});
            validation_factory_map_.emplace("dependentRequired", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_dependent_required_validator(context, sch, parent);});
        }

        schema_validator_ptr_type make_schema_validator(const compilation_context<Json>& context, 
            const Json& sch, jsoncons::span<const std::string> keys, anchor_uri_map_type& anchor_dict) override
        {
            auto new_context = make_compilation_context(context, sch, keys);
            //std::cout << "this->make_cross_draft_schema_validator " << context.get_base_uri().string() << ", " << new_context.get_base_uri().string() << "\n\n";

            schema_validator_ptr_type schema_validator_ptr;

            switch (sch.type())
            {
                case json_type::bool_value:
                {
                    schema_validator_ptr = this->make_boolean_schema(new_context, sch);
                    schema_validator<Json>* p = schema_validator_ptr.get();
                    for (const auto& uri : new_context.uris()) 
                    { 
                        this->insert_schema(uri, p);
                    }          
                    break;
                }
                case json_type::object_value:
                {
                    schema_validator_ptr = make_object_schema_validator(new_context, sch, anchor_dict);
                    schema_validator<Json>* p = schema_validator_ptr.get();
                    for (const auto& uri : new_context.uris()) 
                    { 
                        this->insert_schema(uri, p);
                        /*for (const auto& item : sch.object_range())
                        {
                            if (known_keywords().find(item.key()) == known_keywords().end())
                            {
                                std::cout << "  " << item.key() << "\n";
                                this->insert_unknown_keyword(uri, item.key(), item.value()); // save unknown keywords for later reference
                            }
                        }*/
                    }          
                    break;
                }
                default:
                    JSONCONS_THROW(schema_error(new_context.get_base_uri().string() + ": Invalid JSON-type for a schema, expected: boolean or object"));
                    break;
            }
            
            return schema_validator_ptr;
        }

        schema_validator_ptr_type make_object_schema_validator(const compilation_context<Json>& context, 
            const Json& sch, anchor_uri_map_type& anchor_dict)
        {
            jsoncons::optional<jsoncons::uri> id = context.id();
            Json default_value{jsoncons::null_type()};
            std::vector<keyword_validator_ptr_type> validators;
            std::unique_ptr<unevaluated_properties_validator<Json>> unevaluated_properties_val;
            std::unique_ptr<unevaluated_items_validator<Json>> unevaluated_items_val;
            jsoncons::optional<jsoncons::uri> dynamic_anchor;
            std::map<std::string,schema_validator_ptr_type> defs;
            anchor_uri_map_type local_anchor_dict;

            auto it = sch.find("$dynamicAnchor"); 
            if (it != sch.object_range().end()) 
            {
                std::string value = (*it).value().template as<std::string>();
                jsoncons::uri new_uri(context.get_base_uri(), uri_fragment_part, value);
                dynamic_anchor = jsoncons::optional<jsoncons::uri>(new_uri);
                local_anchor_dict.emplace(value, context.get_base_uri());
            }

            if (this->options().compatibility_mode())
            {
                it = sch.find("definitions");
                if (it != sch.object_range().end()) 
                {
                    for (const auto& def : (*it).value().object_range())
                    {
                        std::string sub_keys[] = { "definitions", def.key() };
                        defs.emplace(def.key(), this->make_cross_draft_schema_validator(context, def.value(), sub_keys, local_anchor_dict));
                    }
                }
            }
            it = sch.find("$defs");
            if (it != sch.object_range().end()) 
            {
                for (const auto& def : (*it).value().object_range())
                {
                    std::string sub_keys[] = { "$defs", def.key() };
                    defs.emplace(def.key(), this->make_cross_draft_schema_validator(context, def.value(), sub_keys, local_anchor_dict));
                }
            }

            it = sch.find("default");
            if (it != sch.object_range().end()) 
            {
                default_value = (*it).value();
            }

            it = sch.find("$ref");
            if (it != sch.object_range().end()) // this schema has a reference
            {
                uri relative((*it).value().template as<std::string>()); 
                auto ref = context.get_base_uri().resolve(relative)                   ;
                validators.push_back(this->get_or_create_reference(sch, uri_wrapper(ref)));
            }

            it = sch.find("$dynamicRef");
            if (it != sch.object_range().end()) // this schema has a reference
            {
                std::string value = (*it).value().template as<std::string>();
                uri relative(value); 
                auto ref = context.get_base_uri().resolve(relative)                   ;
                auto orig = jsoncons::make_unique<dynamic_ref_validator_type>(sch, ref.base(), 
                    context.get_custom_message("$dynamicRef"), uri_wrapper{ref});
                this->unresolved_refs_.emplace_back(ref, orig.get());
                validators.push_back(std::move(orig));
            }

            if (include_applicator_)
            {
                it = sch.find("propertyNames");
                if (it != sch.object_range().end()) 
                {
                    validators.emplace_back(factory_.make_property_names_validator(context, (*it).value(), sch, local_anchor_dict));
                }

                it = sch.find("dependentSchemas");
                if (it != sch.object_range().end()) 
                {
                    validators.emplace_back(factory_.make_dependent_schemas_validator(context, (*it).value(), sch, local_anchor_dict));
                }

                schema_validator_ptr_type if_validator;
                schema_validator_ptr_type then_validator;
                schema_validator_ptr_type else_validator;

                it = sch.find("if");
                if (it != sch.object_range().end()) 
                {
                    std::string sub_keys[] = { "if" };
                    if_validator = this->make_cross_draft_schema_validator(context, (*it).value(), sub_keys, local_anchor_dict);
                }

                it = sch.find("then");
                if (it != sch.object_range().end()) 
                {
                    std::string sub_keys[] = { "then" };
                    then_validator = this->make_cross_draft_schema_validator(context, (*it).value(), sub_keys, local_anchor_dict);
                }

                it = sch.find("else");
                if (it != sch.object_range().end()) 
                {
                    std::string sub_keys[] = { "else" };
                    else_validator = this->make_cross_draft_schema_validator(context, (*it).value(), sub_keys, local_anchor_dict);
                }
                if (if_validator || then_validator || else_validator)
                {
                    validators.emplace_back(jsoncons::make_unique<conditional_validator<Json>>(
                        sch, context.get_base_uri(), context.get_custom_message("conditional"),
                        std::move(if_validator), std::move(then_validator), std::move(else_validator)));
                }
                // Object validators

                std::unique_ptr<properties_validator<Json>> properties;
                it = sch.find("properties");
                if (it != sch.object_range().end()) 
                {
                    properties = factory_.make_properties_validator(context, (*it).value(), sch, local_anchor_dict);
                }
                std::unique_ptr<pattern_properties_validator<Json>> pattern_properties;

        #if defined(JSONCONS_HAS_STD_REGEX)
                it = sch.find("patternProperties");
                if (it != sch.object_range().end())
                {
                    pattern_properties = factory_.make_pattern_properties_validator(context, (*it).value(), sch, local_anchor_dict);
                }
        #endif

                it = sch.find("additionalProperties");
                if (it != sch.object_range().end()) 
                {
                    validators.emplace_back(factory_.make_additional_properties_validator(context, (*it).value(), sch, 
                        std::move(properties), std::move(pattern_properties), local_anchor_dict));
                }
                else
                {
                    if (properties)
                    {
                        validators.emplace_back(std::move(properties));
                    }
    #if defined(JSONCONS_HAS_STD_REGEX)
                    if (pattern_properties)
                    {
                        validators.emplace_back(std::move(pattern_properties));
                    }
    #endif
                }

                it = sch.find("prefixItems");
                if (it != sch.object_range().end()) 
                {

                    if ((*it).value().type() == json_type::array_value) 
                    {
                        validators.emplace_back(factory_.make_prefix_items_validator(context, (*it).value(), sch, local_anchor_dict));
                    } 
                }
                else
                {
                    it = sch.find("items");
                    if (it != sch.object_range().end()) 
                    {
                        if ((*it).value().type() == json_type::object_value || (*it).value().type() == json_type::bool_value)
                        {
                            validators.emplace_back(factory_.make_items_validator("items", context, (*it).value(), sch, local_anchor_dict));
                        }
                    }
                }
            }

            if (include_validation_)
            {
                for (const auto& key_value : sch.object_range())
                {
                    auto factory_it = validation_factory_map_.find(key_value.key());
                    if (factory_it != validation_factory_map_.end())
                    {
                        auto validator = (*factory_it).second(context, key_value.value(), sch, local_anchor_dict);
                        if (validator)
                        {   
                            validators.emplace_back(std::move(validator));
                        }
                    }
                }
            }

            if (include_format_)
            {
                if (this->options().require_format_validation())
                {
                    it = sch.find("format");
                    if (it != sch.object_range().end()) 
                    {
                        validators.emplace_back(factory_.make_format_validator(context, (*it).value(), sch));
                    }
                }
            }
            
            if (include_unevaluated_)
            {
                it = sch.find("unevaluatedProperties");
                if (it != sch.object_range().end()) 
                {
                    unevaluated_properties_val = factory_.make_unevaluated_properties_validator(context, (*it).value(), sch, local_anchor_dict);
                }
                it = sch.find("unevaluatedItems");
                if (it != sch.object_range().end()) 
                {
                    unevaluated_items_val = factory_.make_unevaluated_items_validator(context, (*it).value(), sch, local_anchor_dict);
                }
            }

            if (!id)
            {
                for (const auto& member : local_anchor_dict)
                {
                    anchor_dict[member.first] = member.second;
                }
            }
        
            std::unordered_map<std::string,std::unique_ptr<ref_validator<Json>>> anchor_schema_map;
            for (const auto& member : local_anchor_dict)
            {
                anchor_schema_map.emplace(member.first, this->get_or_create_reference(sch, member.second));
            }
            return jsoncons::make_unique<object_schema_validator<Json>>(context.get_base_uri(), 
                std::move(id),
                std::move(validators), std::move(unevaluated_properties_val), std::move(unevaluated_items_val), 
                std::move(defs), std::move(default_value), std::move(dynamic_anchor), std::move(anchor_schema_map));
        }

    private:

        compilation_context<Json> make_compilation_context(const compilation_context<Json>& parent, 
            const Json& sch, jsoncons::span<const std::string> keys) const override
        {
            // Exclude uri's that are not plain name identifiers
            std::vector<uri_wrapper> new_uris;
            for (const auto& uri : parent.uris())
            {
                if (!uri.has_plain_name_fragment())
                {
                    new_uris.push_back(uri);
                }
            }

            // Append the keys for this sub-schema to the uri's
            for (const auto& key : keys)
            {
                for (auto& uri : new_uris)
                {
                    auto new_u = uri.append(key);
                    uri = uri_wrapper(new_u);
                }
            }

            jsoncons::optional<uri> id;
            std::unordered_map<std::string,std::string> custom_messages{parent.custom_messages()};
            std::string custom_message;
            if (sch.is_object())
            {
                auto it = sch.find("$id"); // If $id is found, this schema can be referenced by the id
                if (it != sch.object_range().end()) 
                {
                    std::string str = (*it).value().template as<std::string>();
                    uri relative(str); 
                    if (relative.has_fragment())
                    {
                        JSONCONS_THROW(schema_error(str + ": Draft 2019-09 does not allow $id with fragment"));
                    }
                    auto resolved = parent.get_base_uri().resolve(relative);
                    id = resolved;
                    uri_wrapper new_uri{resolved};
                    //std::cout << "$id: " << id << ", " << new_uri.string() << "\n";
                    // Add it to the list if it is not already there
                    if (std::find(new_uris.begin(), new_uris.end(), new_uri) == new_uris.end())
                    {
                        new_uris.emplace_back(new_uri); 
                    }
                }
                it = sch.find("$anchor"); 
                if (it != sch.object_range().end()) 
                {
                    auto anchor = (*it).value().template as<std::string>();
                    if (!this->validate_anchor(anchor))
                    {
                        JSONCONS_THROW(schema_error("Invalid $anchor " + anchor));
                    }
                    auto uri = !new_uris.empty() ? new_uris.back().uri() : jsoncons::uri{"#"};
                    jsoncons::uri new_uri(uri, uri_fragment_part, anchor);
                    uri_wrapper identifier{ new_uri };
                    if (std::find(new_uris.begin(), new_uris.end(), identifier) == new_uris.end())
                    {
                        new_uris.emplace_back(std::move(identifier)); 
                    }                  
                }
                it = sch.find("$dynamicAnchor"); 
                if (it != sch.object_range().end()) 
                {
                    auto anchor = (*it).value().template as<std::string>();
                    if (!this->validate_anchor(anchor))
                    {
                        JSONCONS_THROW(schema_error("Invalid $dynamicAnchor " + anchor));
                    }
                    auto uri = !new_uris.empty() ? new_uris.back().uri() : jsoncons::uri{"#"};
                    jsoncons::uri new_uri(uri, uri_fragment_part, anchor);
                    uri_wrapper identifier{ new_uri };
                    if (std::find(new_uris.begin(), new_uris.end(), identifier) == new_uris.end())
                    {
                        new_uris.emplace_back(std::move(identifier)); 
                    }
                }

                if (this->options().enable_custom_error_message())
                {
                    it = sch.find("errorMessage"); 
                    if (it != sch.object_range().end()) 
                    {
                        const auto& value = it->value();
                        if (value.is_object())
                        {
                            for (const auto& item : value.object_range())
                            {
                                custom_messages[item.key()] =  item.value().template as<std::string>();
                            }
                        }
                        else if (value.is_string())
                        {
                            custom_message = value.template as<std::string>();
                        }
                    }
                }
            }

            //std::cout << "Absolute URI: " << parent.get_base_uri().string() << "\n";
            //for (const auto& uri : new_uris)
            //{
            //    std::cout << "    " << uri.string() << "\n";
            //}


            return compilation_context<Json>(new_uris, id, custom_messages, custom_message);
        }

    private:
        static const std::unordered_set<std::string>& known_keywords()
        {
            static std::unordered_set<std::string> keywords{
                "$anchor",       
                "$dynamicAnchor",
                "$dynamicRef",   
                "$id",                 
                "$ref",                
                "additionalItems",     
                "additionalProperties",
                "allOf",               
                "anyOf",               
                "const",               
                "contains",            
                "contentEncoding",     
                "contentMediaType",    
                "default",    
                "$defs",
                "dependencies", 
                "dependentRequired",           
                "dependentSchemas",    
                "description",        
                "enum",                
                "exclusiveMaximum",
                "exclusiveMinimum",
                "if",
                "then",
                "else",        
                "items",               
                "maximum",             
                "maxItems",            
                "maxLength",           
                "maxProperties",       
                "minimum",             
                "minItems",            
                "minLength",           
                "minProperties",       
                "multipleOf",          
                "not",                 
                "oneOf",               
                "pattern",             
                "patternProperties",   
                "prefixItems",    
                "properties",          
                "propertyNames",       
                "readOnly",            
                "required", 
                "title",               
                "type",                
                "uniqueItems",         
                "unevaluatedItems",
                "unevaluatedProperties",        
                "writeOnly"           
            };
            return keywords;
        }
    };

} // namespace draft202012
} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_DRAFT7_KEYWORD_FACTORY_HPP
