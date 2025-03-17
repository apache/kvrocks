// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Copyright (c) 2021 Daniel Deptford
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <cassert>
#include <cctype>
#include <cstdlib>

#if defined( _MSC_VER )
#include <string.h>
#define TAO_PEGTL_STRCASECMP _stricmp
#else
#include <strings.h>
#define TAO_PEGTL_STRCASECMP strcasecmp
#endif

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/parse_tree.hpp>
#include <tao/pegtl/contrib/peg.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   namespace peg
   {
      using node_ptr = std::unique_ptr< parse_tree::node >;

      namespace
      {
         std::string prefix = "tao::pegtl::";

         std::unordered_set< std::string > keywords = {
            "alignas",
            "alignof",
            "and",
            "and_eq",
            "asm",
            "auto",
            "bitand",
            "bitor",
            "bool",
            "break",
            "case",
            "catch",
            "char",
            "char8_t",
            "char16_t",
            "char32_t",
            "class",
            "compl",
            "concept",
            "const",
            "consteval",
            "constexpr",
            "constinit",
            "const_cast",
            "continue",
            "co_await",
            "co_return",
            "co_yield",
            "decltype",
            "default",
            "delete",
            "do",
            "double",
            "dynamic_cast",
            "else",
            "enum",
            "explicit",
            "export",
            "extern",
            "false",
            "float",
            "for",
            "friend",
            "goto",
            "if",
            "inline",
            "int",
            "long",
            "mutable",
            "namespace",
            "new",
            "noexcept",
            "not",
            "not_eq",
            "nullptr",
            "operator",
            "or",
            "or_eq",
            "private",
            "protected",
            "public",
            "register",
            "reinterpret_cast",
            "return",
            "requires",
            "short",
            "signed",
            "sizeof",
            "static",
            "static_assert",
            "static_cast",
            "struct",
            "switch",
            "template",
            "this",
            "thread_local",
            "throw",
            "true",
            "try",
            "typedef",
            "typeid",
            "typename",
            "union",
            "unsigned",
            "using",
            "virtual",
            "void",
            "volatile",
            "wchar_t",
            "while",
            "xor",
            "xor_eq"
         };

         using identifiers_t = std::vector< std::string >;
         identifiers_t identifiers_defined;
         identifiers_t identifiers;

         identifiers_t::reverse_iterator find_identifier( identifiers_t& r, const std::string& v, const identifiers_t::reverse_iterator& rbegin )
         {
            return std::find_if( rbegin, r.rend(), [ & ]( const identifiers_t::value_type& p ) { return TAO_PEGTL_STRCASECMP( p.c_str(), v.c_str() ) == 0; } );
         }

         identifiers_t::reverse_iterator find_identifier( identifiers_t& r, const std::string& v )
         {
            return find_identifier( r, v, r.rbegin() );
         }

         char char_node_to_char( const node_ptr& n )
         {
            const char ch = n->string_view().at( 0 );

            if( ch == '\\' ) {
               static const std::unordered_map< char, char > mappings( {
                  { 'n', '\n' },
                  { 'r', '\r' },
                  { 't', '\t' },
                  { '\'', '\'' },
                  { '\"', '\"' },
                  { '[', '[' },
                  { ']', ']' },
                  { '\\', '\\' },
               } );

               auto iter = mappings.find( n->string_view().at( 1 ) );
               if( iter != std::end( mappings ) ) {
                  return iter->second;
               }

               return static_cast< char >( std::stoi( n->string().substr( 1 ) ) );
            }

            return ch;
         }

         void append_char_node( std::string& s, const node_ptr& n )
         {
            if( !s.empty() ) {
               s += ", ";
            }
            s += '\'';

            const char c = char_node_to_char( n );

            static const std::unordered_map< char, std::string > escapes( {
               { '\b', "b" },
               { '\f', "f" },
               { '\n', "n" },
               { '\r', "r" },
               { '\t', "t" },
               { '\v', "v" },
               { '\\', "\\" },
               { '\'', "\'" },
            } );

            auto iter = escapes.find( c );
            if( iter != std::end( escapes ) ) {
               s += '\\';
               s += iter->second;
            }
            else {
               s += c;
            }

            s += '\'';
         }

      }  // namespace

#if defined( __cpp_exceptions )
      // Using must_if<> we define a control class which is used for
      // the parsing run instead of the default control class.
      //
      // This improves the errors reported to the user.
      //
      // The following turns local errors into global errors, i.e.
      // if one of the rules for which a custom error message is
      // defined fails, it throws a parse_error exception (aka global
      // failure) instead of returning false (aka local failure).

      // clang-format off
      template< typename > inline constexpr const char* error_message = nullptr;

      template<> inline constexpr auto error_message< peg::grammar::Char > = "unterminated character literal";
      template<> inline constexpr auto error_message< peg::grammar::Expression > = "unterminated expression";
      template<> inline constexpr auto error_message< peg::grammar::Grammar > = "unterminated grammar";
      template<> inline constexpr auto error_message< peg::grammar::Range > = "unterminated range";
      // clang-format on

      struct error
      {
         template< typename Rule >
         static constexpr auto message = error_message< Rule >;
      };

      template< typename Rule >
      using control = must_if< error >::control< Rule >;
#else
      template< typename Rule >
      using control = normal< Rule >;
#endif

      // Since we are going to generate a parse tree, we define a
      // selector that decides which rules will be included in our
      // parse tree, which rules will be omitted from the parse tree,
      // and which of the nodes will store the matched content.
      // Additionally, some nodes will fold when they have exactly
      // one child node. (see fold_one below)

      template< typename Rule >
      struct selector
         : pegtl::parse_tree::selector<
              Rule,
              pegtl::parse_tree::store_content::on<
                 grammar::Definition,
                 grammar::Prefix,
                 grammar::Suffix,
                 grammar::Sequence,
                 grammar::Expression,
                 grammar::Class,
                 grammar::Literal,
                 grammar::Identifier,
                 grammar::IdentStart,
                 grammar::Range,
                 grammar::Char,
                 grammar::AND,
                 grammar::NOT,
                 grammar::QUESTION,
                 grammar::STAR,
                 grammar::PLUS,
                 grammar::DOT >,
              pegtl::parse_tree::fold_one::on< grammar::IdentCont > >
      {
         template< typename... States >
         static void transform( node_ptr& n )
         {
            // As we use the PEG grammar taken directly from the original PEG
            // paper, some nodes may have excess content from nodes not included
            // in the parse tree (e.g. Comment, Space, etc).
            if( !n->children.empty() ) {
               n->m_end = n->children.back()->m_end;
            }
         }
      };

      std::string to_string( const node_ptr& n );
      std::string to_string( const std::vector< node_ptr >& v );

      namespace
      {
         std::string get_identifier( const node_ptr& n )
         {
            assert( n->is_type< grammar::Identifier >() );
            std::string v = n->string();
            std::replace( v.begin(), v.end(), '-', '_' );
            return v;
         }

         std::string get_identifier( const node_ptr& n, const bool print_forward_declarations )
         {
            std::string v = get_identifier( n );
            const auto it = find_identifier( identifiers, v );
            if( it != identifiers.rend() ) {
               return *it;
            }
            if( keywords.count( v ) != 0 || v.find( "__" ) != std::string::npos ) {
#if defined( __cpp_exceptions )
               throw parse_error( '\'' + n->string() + "' is a reserved identifier", n->begin() );
#else
               std::cerr << '\'' + n->string() + "' is a reserved identifier" << std::endl;
               std::terminate();
#endif
            }
            if( print_forward_declarations && find_identifier( identifiers_defined, v ) != identifiers_defined.rend() ) {
               std::cout << "struct " << v << ";\n";
            }
            identifiers.push_back( v );
            return v;
         }

         std::unordered_map< std::string, parse_tree::node* > previous_identifiers;

      }  // namespace

      template<>
      struct selector< grammar::Definition >
         : std::true_type
      {
         template< typename... States >
         static void transform( node_ptr& n )
         {
            const auto idname = get_identifier( n->children.front() );
            assert( n->children.back()->is_type< grammar::Expression >() );
            if( !previous_identifiers.try_emplace( idname, n.get() ).second ) {
#if defined( __cpp_exceptions )
               throw parse_error( "identifier '" + idname + "' is already defined", n->begin() );
#else
               std::cerr << "identifier '" + idname + "' is already defined" << std::endl;
               std::terminate();
#endif
            }
         }
      };

      // Finally, the generated parse tree for each node is converted to
      // a C++ source code string.

      struct stringifier
      {
         using function_t = std::string ( * )( const node_ptr& n );
         function_t default_ = nullptr;

         std::unordered_map< std::string_view, function_t > map_;

         template< typename T >
         void add( const function_t& f )
         {
            map_.try_emplace( demangle< T >(), f );
         }

         std::string operator()( const node_ptr& n ) const
         {
            const auto it = map_.find( n->type );
            if( it != map_.end() ) {
               return it->second( n );
            }

            return default_( n );
         }
      };

      stringifier make_stringifier()
      {
         stringifier nrv;
         nrv.default_ = []( const node_ptr& n ) -> std::string {
#if defined( __cpp_exceptions )
            throw parse_error( "missing to_string() for " + std::string( n->type ), n->begin() );
#else
            std::cerr << "missing to_string() for " + std::string( n->type ) << std::endl;
            std::terminate();
#endif
         };

         nrv.add< grammar::Identifier >( []( const node_ptr& n ) { return get_identifier( n, true ); } );

         nrv.add< grammar::Definition >( []( const node_ptr& n ) {
            return "struct " + get_identifier( n->children.front(), false ) + " : " + to_string( n->children.back() ) + " {};";
         } );

         nrv.add< grammar::Char >( []( const node_ptr& n ) {
            std::string s;
            append_char_node( s, n );
            return s;
         } );

         nrv.add< grammar::Sequence >( []( const node_ptr& n ) {
            if( n->children.size() == 1 ) {
               return to_string( n->children.front() );
            }

            return prefix + "seq< " + to_string( n->children ) + " >";
         } );

         nrv.add< grammar::Expression >( []( const node_ptr& n ) {
            if( n->children.size() == 1 ) {
               return to_string( n->children.front() );
            }

            return prefix + "sor< " + to_string( n->children ) + " >";
         } );

         nrv.add< grammar::Range >( []( const node_ptr& n ) {
            if( n->children.size() == 1 ) {
               return prefix + "one< " + to_string( n->children.front() ) + " >";
            }

            return prefix + "range< " + to_string( n->children.front() ) + ", " + to_string( n->children.back() ) + " >";
         } );

         nrv.add< grammar::Class >( []( const node_ptr& n ) {
            if( n->children.size() == 1 ) {
               return to_string( n->children.front() );
            }

            return prefix + "sor < " + to_string( n->children ) + " >";
         } );

         nrv.add< grammar::Literal >( []( const node_ptr& n ) {
            if( n->children.size() == 1 ) {
               return prefix + "one< " + to_string( n->children.front() ) + " >";
            }

            return prefix + "string< " + to_string( n->children ) + " >";
         } );

         nrv.add< grammar::Prefix >( []( const node_ptr& n ) {
            auto sub = to_string( n->children.back() );

            if( n->children.front()->is_type< grammar::AND >() ) {
               return prefix + "at< " + sub + " >";
            }

            if( n->children.front()->is_type< grammar::NOT >() ) {
               return prefix + "not_at< " + sub + " >";
            }

            assert( n->children.size() == 1 );
            return sub;
         } );

         nrv.add< grammar::Suffix >( []( const node_ptr& n ) {
            auto sub = to_string( n->children.front() );

            if( n->children.back()->is_type< grammar::QUESTION >() ) {
               return prefix + "opt< " + sub + " >";
            }

            if( n->children.back()->is_type< grammar::STAR >() ) {
               return prefix + "star< " + sub + " >";
            }

            if( n->children.back()->is_type< grammar::PLUS >() ) {
               return prefix + "plus< " + sub + " >";
            }

            assert( n->children.size() == 1 );
            return sub;
         } );

         nrv.add< grammar::DOT >( []( const node_ptr& /*unused*/ ) {
            return prefix + "any";
         } );

         return nrv;
      }

      std::string to_string( const node_ptr& n )
      {
         static stringifier s = make_stringifier();
         return s( n );
      }

      std::string to_string( const std::vector< node_ptr >& v )
      {
         std::string result;
         for( const auto& c : v ) {
            if( !result.empty() ) {
               result += ", ";
            }
            result += to_string( c );
         }
         return result;
      }

   }  // namespace peg

}  // namespace TAO_PEGTL_NAMESPACE

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   using namespace TAO_PEGTL_NAMESPACE;

   if( argc != 2 ) {
      std::cerr << "Usage: " << argv[ 0 ] << " SOURCE\n";
      return 1;
   }

   file_input in( argv[ 1 ] );
#if defined( __cpp_exceptions )
   try {
      const auto root = parse_tree::parse< peg::grammar::Grammar, peg::selector, nothing, peg::control >( in );

      for( const auto& definition : root->children ) {
         peg::identifiers_defined.push_back( peg::get_identifier( definition->children.front() ) );
      }

      for( const auto& rule : root->children ) {
         std::cout << peg::to_string( rule ) << '\n';
      }
   }
   catch( const parse_error& e ) {
      const auto p = e.positions().front();
      std::cerr << e.what() << '\n'
                << in.line_at( p ) << '\n'
                << std::setw( p.column ) << '^' << '\n';
   }
#else
   if( const auto root = parse_tree::parse< peg::grammar::Grammar, peg::selector, nothing, peg::control >( in ) ) {
      for( const auto& definition : root->children ) {
         peg::identifiers_defined.push_back( peg::get_identifier( definition->children.front() ) );
      }

      for( const auto& rule : root->children ) {
         std::cout << peg::to_string( rule ) << '\n';
      }
   }
   else {
      std::cerr << "error occurred" << std::endl;
      return 1;
   }
#endif
   return 0;
}
