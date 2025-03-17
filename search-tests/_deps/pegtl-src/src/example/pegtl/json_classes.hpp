// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#ifndef TAO_PEGTL_SRC_EXAMPLES_PEGTL_JSON_CLASSES_HPP
#define TAO_PEGTL_SRC_EXAMPLES_PEGTL_JSON_CLASSES_HPP

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace example
{
   enum class json_type
   {
      array,
      boolean,
      null,
      number,
      object,
      string
   };

   class json_base
   {
   protected:
      explicit json_base( const json_type in_type )
         : type( in_type )
      {}

      virtual ~json_base() = default;

   public:
      json_base( const json_base& ) = delete;
      json_base( json_base&& ) = delete;

      json_base& operator=( const json_base& ) = delete;
      json_base& operator=( json_base&& ) = delete;

      virtual void stream( std::ostream& ) const = 0;

      const json_type type;
   };

   inline std::ostream& operator<<( std::ostream& o, const json_base& j )
   {
      j.stream( o );
      return o;
   }

   inline std::ostream& operator<<( std::ostream& o, const std::shared_ptr< json_base >& j )
   {
      return j ? ( o << *j ) : ( o << "NULL" );
   }

   struct array_json
      : public json_base
   {
      array_json()
         : json_base( json_type::array )
      {}

      std::vector< std::shared_ptr< json_base > > data;

      void stream( std::ostream& o ) const override
      {
         o << '[';
         if( !data.empty() ) {
            auto iter = data.begin();
            o << *iter;
            while( ++iter != data.end() ) {
               o << ',' << *iter;
            }
         }
         o << ']';
      }
   };

   struct boolean_json
      : public json_base
   {
      explicit boolean_json( const bool in_data )
         : json_base( json_type::boolean ),
           data( in_data )
      {}

      bool data;

      void stream( std::ostream& o ) const override
      {
         o << ( data ? "true" : "false" );
      }
   };

   struct null_json
      : public json_base
   {
      null_json()
         : json_base( json_type::null )
      {}

      void stream( std::ostream& o ) const override
      {
         o << "null";
      }
   };

   struct number_json
      : public json_base
   {
      explicit number_json( const long double in_data )
         : json_base( json_type::number ),
           data( in_data )
      {}

      long double data;

      void stream( std::ostream& o ) const override
      {
         o << data;
      }
   };

   inline std::string json_escape( const std::string& data )
   {
      std::string r = "\"";

      r.reserve( data.size() + 4 );

      static const char* h = "0123456789abcdef";

      const auto* d = reinterpret_cast< const unsigned char* >( data.data() );

      for( std::size_t i = 0; i < data.size(); ++i ) {
         switch( const auto c = d[ i ] ) {
            case '\b':
               r += "\\b";
               break;
            case '\f':
               r += "\\f";
               break;
            case '\n':
               r += "\\n";
               break;
            case '\r':
               r += "\\r";
               break;
            case '\t':
               r += "\\t";
               break;
            case '\\':
               r += "\\\\";
               break;
            case '\"':
               r += "\\\"";
               break;
            default:
               if( ( c < 32 ) || ( c == 127 ) ) {
                  r += "\\u00";
                  r += h[ ( c & 0xf0 ) >> 4 ];
                  r += h[ c & 0x0f ];
                  continue;
               }
               r += c;  // Assume valid UTF-8.
               break;
         }
      }
      r += '"';
      return r;
   }

   struct string_json
      : public json_base
   {
      explicit string_json( const std::string& in_data )
         : json_base( json_type::string ),
           data( in_data )
      {}

      std::string data;

      void stream( std::ostream& o ) const override
      {
         o << json_escape( data );
      }
   };

   struct object_json
      : public json_base
   {
      object_json()
         : json_base( json_type::object )
      {}

      std::map< std::string, std::shared_ptr< json_base > > data;

      void stream( std::ostream& o ) const override
      {
         o << '{';
         if( !data.empty() ) {
            auto iter = data.begin();
            o << json_escape( iter->first ) << ':' << iter->second;
            while( ++iter != data.end() ) {
               o << ',' << json_escape( iter->first ) << ':' << iter->second;
            }
         }
         o << '}';
      }
   };

}  // namespace example

#endif
