// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_DRAFT201909_SCHEMA_DRAFT201909_HPP
#define JSONCONS_EXT_JSONSCHEMA_DRAFT201909_SCHEMA_DRAFT201909_HPP

namespace jsoncons {
namespace jsonschema {
namespace draft201909 {
    
    template <typename Json>
    struct schema_draft201909
    {
        static Json get_schema() 
        {
            static Json sch = Json::parse(R"(
{
    "$schema": "https://json-schema.org/draft/2019-09/schema",
    "$id": "https://json-schema.org/draft/2019-09/schema",
    "$vocabulary": {
        "https://json-schema.org/draft/2019-09/vocab/core": true,
        "https://json-schema.org/draft/2019-09/vocab/applicator": true,
        "https://json-schema.org/draft/2019-09/vocab/validation": true,
        "https://json-schema.org/draft/2019-09/vocab/meta-data": true,
        "https://json-schema.org/draft/2019-09/vocab/format": false,
        "https://json-schema.org/draft/2019-09/vocab/content": true
    },
    "$recursiveAnchor": true,

    "title": "Core and Validation specifications meta-schema",
    "allOf": [
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "$id": "https://json-schema.org/draft/2019-09/meta/core",
          "$vocabulary": {
            "https://json-schema.org/draft/2019-09/vocab/core": true
          },
          "$recursiveAnchor": true,

          "title": "Core vocabulary meta-schema",
          "type": ["object", "boolean"],
          "properties": {
            "$id": {
              "type": "string",
              "format": "uri-reference",
              "$comment": "Non-empty fragments not allowed.",
              "pattern": "^[^#]*#?$"
            },
            "$schema": {
              "type": "string",
              "format": "uri"
            },
            "$anchor": {
              "type": "string",
              "pattern": "^[A-Za-z][-A-Za-z0-9.:_]*$"
            },
            "$ref": {
              "type": "string",
              "format": "uri-reference"
            },
            "$recursiveRef": {
              "type": "string",
              "format": "uri-reference"
            },
            "$recursiveAnchor": {
              "type": "boolean",
              "default": false
            },
            "$vocabulary": {
              "type": "object",
              "propertyNames": {
                "type": "string",
                "format": "uri"
              },
              "additionalProperties": {
                "type": "boolean"
              }
            },
            "$comment": {
              "type": "string"
            },
            "$defs": {
              "type": "object",
              "additionalProperties": {"$recursiveRef": "#"},
              "default": {}
            }
          }
        },
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "$id": "https://json-schema.org/draft/2019-09/meta/applicator",
          "$vocabulary": {
            "https://json-schema.org/draft/2019-09/vocab/applicator": true
          },
          "$recursiveAnchor": true,

          "title": "Applicator vocabulary meta-schema",
          "type": ["object", "boolean"],
          "properties": {
            "additionalItems": {"$recursiveRef": "#"},
            "unevaluatedItems": {"$recursiveRef": "#"},
            "items": {
              "anyOf": [{"$recursiveRef": "#"}, {"$ref": "#/$defs/schemaArray"}]
            },
            "contains": {"$recursiveRef": "#"},
            "additionalProperties": {"$recursiveRef": "#"},
            "unevaluatedProperties": {"$recursiveRef": "#"},
            "properties": {
              "type": "object",
              "additionalProperties": {"$recursiveRef": "#"},
              "default": {}
            },
            "patternProperties": {
              "type": "object",
              "additionalProperties": {"$recursiveRef": "#"},
              "propertyNames": {"format": "regex"},
              "default": {}
            },
            "dependentSchemas": {
              "type": "object",
              "additionalProperties": {
                "$recursiveRef": "#"
              }
            },
            "propertyNames": {"$recursiveRef": "#"},
            "if": {"$recursiveRef": "#"},
            "then": {"$recursiveRef": "#"},
            "else": {"$recursiveRef": "#"},
            "allOf": {"$ref": "#/$defs/schemaArray"},
            "anyOf": {"$ref": "#/$defs/schemaArray"},
            "oneOf": {"$ref": "#/$defs/schemaArray"},
            "not": {"$recursiveRef": "#"}
          },
          "$defs": {
            "schemaArray": {
              "type": "array",
              "minItems": 1,
              "items": {"$recursiveRef": "#"}
            }
          }
        },
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "$id": "https://json-schema.org/draft/2019-09/meta/validation",
          "$vocabulary": {
            "https://json-schema.org/draft/2019-09/vocab/validation": true
          },
          "$recursiveAnchor": true,

          "title": "Validation vocabulary meta-schema",
          "type": ["object", "boolean"],
          "properties": {
            "multipleOf": {
              "type": "number",
              "exclusiveMinimum": 0
            },
            "maximum": {
              "type": "number"
            },
            "exclusiveMaximum": {
              "type": "number"
            },
            "minimum": {
              "type": "number"
            },
            "exclusiveMinimum": {
              "type": "number"
            },
            "maxLength": {"$ref": "#/$defs/nonNegativeInteger"},
            "minLength": {"$ref": "#/$defs/nonNegativeIntegerDefault0"},
            "pattern": {
              "type": "string",
              "format": "regex"
            },
            "maxItems": {"$ref": "#/$defs/nonNegativeInteger"},
            "minItems": {"$ref": "#/$defs/nonNegativeIntegerDefault0"},
            "uniqueItems": {
              "type": "boolean",
              "default": false
            },
            "maxContains": {"$ref": "#/$defs/nonNegativeInteger"},
            "minContains": {
              "$ref": "#/$defs/nonNegativeInteger",
              "default": 1
            },
            "maxProperties": {"$ref": "#/$defs/nonNegativeInteger"},
            "minProperties": {"$ref": "#/$defs/nonNegativeIntegerDefault0"},
            "required": {"$ref": "#/$defs/stringArray"},
            "dependentRequired": {
              "type": "object",
              "additionalProperties": {
                "$ref": "#/$defs/stringArray"
              }
            },
            "const": true,
            "enum": {
              "type": "array",
              "items": true
            },
            "type": {
              "anyOf": [
                {"$ref": "#/$defs/simpleTypes"},
                {
                  "type": "array",
                  "items": {"$ref": "#/$defs/simpleTypes"},
                  "minItems": 1,
                  "uniqueItems": true
                }
              ]
            }
          },
          "$defs": {
            "nonNegativeInteger": {
              "type": "integer",
              "minimum": 0
            },
            "nonNegativeIntegerDefault0": {
              "$ref": "#/$defs/nonNegativeInteger",
              "default": 0
            },
            "simpleTypes": {
              "enum": ["array", "boolean", "integer", "null", "number", "object", "string"]
            },
            "stringArray": {
              "type": "array",
              "items": {"type": "string"},
              "uniqueItems": true,
              "default": []
            }
          }
        },
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "$id": "https://json-schema.org/draft/2019-09/meta/meta-data",
          "$vocabulary": {
            "https://json-schema.org/draft/2019-09/vocab/meta-data": true
          },
          "$recursiveAnchor": true,

          "title": "Meta-data vocabulary meta-schema",

          "type": ["object", "boolean"],
          "properties": {
            "title": {
              "type": "string"
            },
            "description": {
              "type": "string"
            },
            "default": true,
            "deprecated": {
              "type": "boolean",
              "default": false
            },
            "readOnly": {
              "type": "boolean",
              "default": false
            },
            "writeOnly": {
              "type": "boolean",
              "default": false
            },
            "examples": {
              "type": "array",
              "items": true
            }
          }
        },
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "$id": "https://json-schema.org/draft/2019-09/meta/format",
          "$vocabulary": {
            "https://json-schema.org/draft/2019-09/vocab/format": true
          },
          "$recursiveAnchor": true,

          "title": "Format vocabulary meta-schema",
          "type": ["object", "boolean"],
          "properties": {
            "format": {"type": "string"}
          }
        },
        {
          "$schema": "https://json-schema.org/draft/2019-09/schema",
          "$id": "https://json-schema.org/draft/2019-09/meta/content",
          "$vocabulary": {
            "https://json-schema.org/draft/2019-09/vocab/content": true
          },
          "$recursiveAnchor": true,

          "title": "Content vocabulary meta-schema",

          "type": ["object", "boolean"],
          "properties": {
            "contentMediaType": {"type": "string"},
            "contentEncoding": {"type": "string"},
            "contentSchema": {"$recursiveRef": "#"}
          }
        }
    ],
    "type": ["object", "boolean"],
    "properties": {
        "definitions": {
            "$comment": "While no longer an official keyword as it is replaced by $defs, this keyword is retained in the meta-schema to prevent incompatible extensions as it remains in common use.",
            "type": "object",
            "additionalProperties": { "$recursiveRef": "#" },
            "default": {}
        },
        "dependencies": {
            "$comment": "\"dependencies\" is no longer a keyword, but schema authors should avoid redefining it to facilitate a smooth transition to \"dependentSchemas\" and \"dependentRequired\"",
            "type": "object",
            "additionalProperties": {
                "anyOf": [
                    { "$recursiveRef": "#" },
                    { "$ref": "meta/validation#/$defs/stringArray" }
                ]
            }
        }
    }
}
 )"); 

            return sch;
        }
    };

} // namespace draft201909
} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_DRAFT201909_SCHEMA_DRAFT201909_HPP
