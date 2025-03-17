// Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <algorithm>
#include <exception>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <set>
#include <sstream>
#include <string>
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
#include <tao/pegtl/contrib/abnf.hpp>
#include <tao/pegtl/contrib/parse_tree.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   namespace abnf
   {
      using node_ptr = std::unique_ptr< parse_tree::node >;

      namespace
      {
         std::string prefix = "tao::pegtl::";

         std::set< std::string > keywords = {
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

         using rules_t = std::vector< std::string >;
         rules_t rules_defined;
         rules_t rules;

         // clang-format off
         struct one_tag {};
         struct string_tag {};
         struct istring_tag {};
         // clang-format on

         rules_t::reverse_iterator find_rule( rules_t& r, const std::string& v, const rules_t::reverse_iterator& rbegin )
         {
            return std::find_if( rbegin, r.rend(), [ & ]( const rules_t::value_type& p ) { return TAO_PEGTL_STRCASECMP( p.c_str(), v.c_str() ) == 0; } );
         }

         rules_t::reverse_iterator find_rule( rules_t& r, const std::string& v )
         {
            return find_rule( r, v, r.rbegin() );
         }

         bool append_char( std::string& s, const char c )
         {
            if( !s.empty() ) {
               s += ", ";
            }
            s += '\'';
            if( c == '\'' || c == '\\' ) {
               s += '\\';
            }
            s += c;
            s += '\'';
            return std::isalpha( c ) != 0;
         }

         std::string remove_leading_zeroes( const std::string& v )
         {
            const auto pos = v.find_first_not_of( '0' );
            if( pos == std::string::npos ) {
               return "";
            }
            return v.substr( pos );
         }

         void shift( internal::iterator& it, int delta )
         {
            it.data += delta;
            it.byte += delta;
            it.column += delta;
         }

      }  // namespace

      namespace grammar
      {
         // ABNF grammar according to RFC 5234, updated by RFC 7405, with
         // the following differences:
         //
         // To form a C++ identifier from a rulename, all minuses are
         // replaced with underscores.
         //
         // As C++ identifiers are case-sensitive, we remember the "correct"
         // spelling from the first occurrence of a rulename, all other
         // occurrences are automatically changed to that.
         //
         // Certain rulenames are reserved as their equivalent C++ identifier is
         // reserved as a keyword, an alternative token, by the standard or
         // for other, special reasons.
         //
         // When using numerical values (num-val, repeat), the values
         // must be in the range of the corresponding C++ data type.
         //
         // Remember we are defining a PEG, not a CFG. Simply copying some
         // ABNF from somewhere might lead to surprising results as the
         // alternations are now sequential, using the sor<> rule.
         //
         // PEGs also require two extensions: The and-predicate and the
         // not-predicate. They are expressed by '&' and '!' respectively,
         // being allowed (optionally, only one of them) before the
         // repetition. You can use braces for more complex expressions.
         //
         // Finally, instead of the pre-defined CRLF sequence, we accept
         // any type of line ending as a convenience extension.

         // clang-format off
         struct CRLF : sor< abnf::CRLF, CR, LF > {};

         struct comment_cont : until< CRLF, sor< WSP, VCHAR > > {};
         struct comment : seq< one< ';' >, comment_cont > {};
         struct c_nl : sor< comment, CRLF > {};
         struct req_c_nl : c_nl {};
         struct c_wsp : sor< WSP, seq< c_nl, WSP > > {};

         struct rulename : seq< ALPHA, star< ranges< 'a', 'z', 'A', 'Z', '0', '9', '-' > > > {};

         struct quoted_string_cont : until< DQUOTE, print > {};
         struct quoted_string : seq< DQUOTE, quoted_string_cont > {};
         struct case_insensitive_string : seq< opt< istring< '%', 'i' > >, quoted_string > {};
         struct case_sensitive_string : seq< istring< '%', 's' >, quoted_string > {};
         struct char_val : sor< case_insensitive_string, case_sensitive_string > {};

         struct prose_val_cont : until< one< '>' >, print > {};
         struct prose_val : seq< one< '<' >, prose_val_cont > {};

         template< char First, typename Digit >
         struct gen_val
         {
            struct value : plus< Digit > {};
            struct range : seq< one< '-' >, value > {};
            struct next_value : seq< value > {};
            struct type : seq< istring< First >, value, sor< range, star< one< '.' >, next_value > > > {};
         };

         using hex_val = gen_val< 'x', HEXDIG >;
         using dec_val = gen_val< 'd', DIGIT >;
         using bin_val = gen_val< 'b', BIT >;

         struct num_val_choice : sor< bin_val::type, dec_val::type, hex_val::type > {};
         struct num_val : seq< one< '%' >, num_val_choice > {};

         struct alternation;
         struct option_close : one< ']' > {};
         struct option : seq< one< '[' >, pad< alternation, c_wsp >, option_close > {};
         struct group_close : one< ')' > {};
         struct group : seq< one< '(' >, pad< alternation, c_wsp >, group_close > {};
         struct element : sor< rulename, group, option, char_val, num_val, prose_val > {};

         struct repeat : sor< seq< star< DIGIT >, one< '*' >, star< DIGIT > >, plus< DIGIT > > {};
         struct repetition : seq< opt< repeat >, element > {};
         struct req_repetition : seq< repetition > {};

         struct and_predicate : seq< one< '&' >, req_repetition > {};
         struct not_predicate : seq< one< '!' >, req_repetition > {};
         struct predicate : sor< and_predicate, not_predicate, repetition > {};

         struct concatenation : list< predicate, plus< c_wsp > > {};
         struct alternation : list< concatenation, pad< one< '/' >, c_wsp > > {};

         struct defined_as_op : sor< string< '=', '/' >, one< '=' > > {};
         struct defined_as : pad< defined_as_op, c_wsp > {};
         struct rule : seq< seq< rulename, defined_as, alternation >, star< c_wsp >, req_c_nl > {};
         struct rulelist : until< eof, sor< seq< star< c_wsp >, c_nl >, rule > > {};
         // clang-format on

      }  // namespace grammar

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

      template<> inline constexpr auto error_message< abnf::grammar::comment_cont > = "unterminated comment";

      template<> inline constexpr auto error_message< abnf::grammar::quoted_string_cont > = "unterminated string (missing '\"')";
      template<> inline constexpr auto error_message< abnf::grammar::prose_val_cont > = "unterminated prose description (missing '>')";

      template<> inline constexpr auto error_message< abnf::grammar::hex_val::value > = "expected hexadecimal value";
      template<> inline constexpr auto error_message< abnf::grammar::dec_val::value > = "expected decimal value";
      template<> inline constexpr auto error_message< abnf::grammar::bin_val::value > = "expected binary value";
      template<> inline constexpr auto error_message< abnf::grammar::num_val_choice > = "expected base specifier (one of 'bBdDxX')";

      template<> inline constexpr auto error_message< abnf::grammar::option_close > = "unterminated option (missing ']')";
      template<> inline constexpr auto error_message< abnf::grammar::group_close > = "unterminated group (missing ')')";

      template<> inline constexpr auto error_message< abnf::grammar::req_repetition > = "expected element";
      template<> inline constexpr auto error_message< abnf::grammar::concatenation > = "expected element";
      template<> inline constexpr auto error_message< abnf::grammar::alternation > = "expected element";

      template<> inline constexpr auto error_message< abnf::grammar::defined_as > = "expected '=' or '=/'";
      template<> inline constexpr auto error_message< abnf::grammar::req_c_nl > = "unterminated rule";
      template<> inline constexpr auto error_message< abnf::grammar::rule > = "expected rule";
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
         : parse_tree::selector<
              Rule,
              parse_tree::store_content::on<
                 grammar::rulename,
                 grammar::prose_val,
                 grammar::hex_val::value,
                 grammar::dec_val::value,
                 grammar::bin_val::value,
                 grammar::hex_val::range,
                 grammar::dec_val::range,
                 grammar::bin_val::range,
                 grammar::hex_val::type,
                 grammar::dec_val::type,
                 grammar::bin_val::type,
                 grammar::repeat,
                 grammar::defined_as_op >,
              parse_tree::remove_content::on<
                 grammar::option,
                 grammar::and_predicate,
                 grammar::not_predicate,
                 grammar::rule >,
              parse_tree::fold_one::on<
                 grammar::alternation,
                 grammar::group,
                 grammar::repetition,
                 grammar::concatenation > >
      {};

      // Besides the above "simple" list of selected rules,
      // we also provide special handling to some nodes.
      // When they are inserted into the parse tree, the
      // transform method allows additional tree transformations
      // in order to improve the generated tree.

      template<>
      struct selector< grammar::quoted_string >
         : std::true_type
      {
         template< typename... States >
         static void transform( node_ptr& n )
         {
            shift( n->m_begin, 1 );
            shift( n->m_end, -1 );

            const auto content = n->string_view();
            for( const auto c : content ) {
               if( std::isalpha( c ) != 0 ) {
                  n->set_type< istring_tag >();
                  return;
               }
            }
            if( content.size() == 1 ) {
               n->set_type< one_tag >();
            }
            else {
               n->set_type< string_tag >();
            }
         }
      };

      template<>
      struct selector< grammar::case_sensitive_string >
         : std::true_type
      {
         template< typename... States >
         static void transform( node_ptr& n )
         {
            n = std::move( n->children.back() );
            if( n->string_view().size() == 1 ) {
               n->set_type< one_tag >();
            }
            else {
               n->set_type< string_tag >();
            }
         }
      };

      std::string to_string( const node_ptr& n );
      std::string to_string( const std::vector< node_ptr >& v );

      std::string to_string_unwrap_seq( const node_ptr& n )
      {
         if( n->is_type< grammar::group >() || n->is_type< grammar::concatenation >() ) {
            return to_string( n->children );
         }
         return to_string( n );
      }

      namespace
      {
         std::string get_rulename( const node_ptr& n )
         {
            assert( n->is_type< grammar::rulename >() );
            std::string v = n->string();
            std::replace( v.begin(), v.end(), '-', '_' );
            return v;
         }

         std::string get_rulename( const node_ptr& n, const bool print_forward_declarations )
         {
            std::string v = get_rulename( n );
            const auto it = find_rule( rules, v );
            if( it != rules.rend() ) {
               return *it;
            }
            if( keywords.count( v ) != 0 || v.find( "__" ) != std::string::npos ) {
#if defined( __cpp_exceptions )
               throw parse_error( '\'' + n->string() + "' is a reserved rulename", n->begin() );
#else
               std::cerr << '\'' + n->string() + "' is a reserved rulename" << std::endl;
               std::terminate();
#endif
            }
            if( print_forward_declarations && find_rule( rules_defined, v ) != rules_defined.rend() ) {
               std::cout << "struct " << v << ";\n";
            }
            rules.push_back( v );
            return v;
         }

         template< typename T >
         std::string gen_val( const node_ptr& n )
         {
            if( n->children.size() == 2 ) {
               if( n->children.back()->is_type< T >() ) {
                  return prefix + "range< " + to_string( n->children.front() ) + ", " + to_string( n->children.back()->children.front() ) + " >";
               }
            }
            if( n->children.size() == 1 ) {
               return prefix + "one< " + to_string( n->children ) + " >";
            }
            return prefix + "string< " + to_string( n->children ) + " >";
         }

         struct ccmp
         {
            bool operator()( const std::string& lhs, const std::string& rhs ) const noexcept
            {
               return TAO_PEGTL_STRCASECMP( lhs.c_str(), rhs.c_str() ) < 0;
            }
         };

         std::map< std::string, parse_tree::node*, ccmp > previous_rules;

      }  // namespace

      template<>
      struct selector< grammar::rule >
         : std::true_type
      {
         template< typename... States >
         static void transform( node_ptr& n )
         {
            const auto rname = get_rulename( n->children.front() );
            assert( n->children.at( 1 )->is_type< grammar::defined_as_op >() );
            const auto op = n->children.at( 1 )->string();
            // when we insert a normal rule, we need to check for duplicates
            if( op == "=" ) {
               if( !previous_rules.try_emplace( rname, n.get() ).second ) {
#if defined( __cpp_exceptions )
                  throw parse_error( "rule '" + rname + "' is already defined", n->begin() );
#else
                  std::cerr << "rule '" + rname + "' is already defined" << std::endl;
                  std::terminate();
#endif
               }
            }
            // if it is an "incremental alternation", we need to consolidate the assigned alternations
            else if( op == "=/" ) {
               const auto p = previous_rules.find( rname );
               if( p == previous_rules.end() ) {
#if defined( __cpp_exceptions )
                  throw parse_error( "incremental alternation '" + rname + "' without previous rule definition", n->begin() );
#else
                  std::cerr << "incremental alternation '" + rname + "' without previous rule definition" << std::endl;
                  std::terminate();
#endif
               }
               auto& previous = p->second->children.back();

               // if the previous rule does not assign an alternation, create an intermediate alternation and move its assignee into it.
               if( !previous->is_type< abnf::grammar::alternation >() ) {
                  auto s = std::make_unique< parse_tree::node >();
                  s->set_type< abnf::grammar::alternation >();
                  s->source = previous->source;
                  s->m_begin = previous->m_begin;
                  s->m_end = previous->m_end;
                  s->children.emplace_back( std::move( previous ) );
                  previous = std::move( s );
               }

               // append all new options to the previous rule's assignee (which always is an alternation now)
               previous->m_end = n->children.back()->m_end;

               // if the new rule itself contains an alternation, append the individual entries...
               if( n->children.back()->is_type< abnf::grammar::alternation >() ) {
                  for( auto& e : n->children.back()->children ) {
                     previous->children.emplace_back( std::move( e ) );
                  }
               }
               // ...otherwise add the node itself as another option.
               else {
                  previous->children.emplace_back( std::move( n->children.back() ) );
               }
               n.reset();
            }
            else {
#if defined( __cpp_exceptions )
               throw parse_error( "invalid operator '" + op + "', this should not happen!", n->begin() );
#else
               std::cerr << "invalid operator '" + op + "', this should not happen!" << std::endl;
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

         std::map< std::string_view, function_t > map_;

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

         nrv.add< grammar::rulename >( []( const node_ptr& n ) { return get_rulename( n, true ); } );

         nrv.add< grammar::rule >( []( const node_ptr& n ) {
            return "struct " + get_rulename( n->children.front(), false ) + " : " + to_string( n->children.back() ) + " {};";
         } );

         nrv.add< string_tag >( []( const node_ptr& n ) {
            const auto content = n->string_view();
            std::string s;
            for( const auto c : content ) {
               append_char( s, c );
            }
            return prefix + "string< " + s + " >";
         } );

         nrv.add< istring_tag >( []( const node_ptr& n ) {
            const auto content = n->string_view();
            std::string s;
            for( const auto c : content ) {
               append_char( s, c );
            }
            return prefix + "istring< " + s + " >";
         } );

         nrv.add< one_tag >( []( const node_ptr& n ) {
            const auto content = n->string_view();
            std::string s;
            for( const auto c : content ) {
               append_char( s, c );
            }
            return prefix + "one< " + s + " >";
         } );

         nrv.add< grammar::hex_val::value >( []( const node_ptr& n ) { return "0x" + n->string(); } );
         nrv.add< grammar::dec_val::value >( []( const node_ptr& n ) { return n->string(); } );
         nrv.add< grammar::bin_val::value >( []( const node_ptr& n ) {
            unsigned long long v = 0;
            const char* p = n->m_begin.data;
            // TODO: Detect overflow
            do {
               v <<= 1;
               v |= ( *p++ & 1 );
            } while( p != n->m_end.data );
            std::ostringstream oss;
            oss << v;
            return std::move( oss ).str();
         } );

         nrv.add< grammar::hex_val::type >( []( const node_ptr& n ) { return gen_val< grammar::hex_val::range >( n ); } );
         nrv.add< grammar::dec_val::type >( []( const node_ptr& n ) { return gen_val< grammar::dec_val::range >( n ); } );
         nrv.add< grammar::bin_val::type >( []( const node_ptr& n ) { return gen_val< grammar::bin_val::range >( n ); } );

         nrv.add< grammar::alternation >( []( const node_ptr& n ) { return prefix + "sor< " + to_string( n->children ) + " >"; } );
         nrv.add< grammar::option >( []( const node_ptr& n ) { return prefix + "opt< " + to_string( n->children ) + " >"; } );
         nrv.add< grammar::group >( []( const node_ptr& n ) { return prefix + "seq< " + to_string( n->children ) + " >"; } );

         nrv.add< grammar::prose_val >( []( const node_ptr& n ) { return "/* " + n->string() + " */"; } );

         nrv.add< grammar::and_predicate >( []( const node_ptr& n ) {
            assert( n->children.size() == 1 );
            return prefix + "at< " + to_string_unwrap_seq( n->children.front() ) + " >";
         } );

         nrv.add< grammar::not_predicate >( []( const node_ptr& n ) {
            assert( n->children.size() == 1 );
            return prefix + "not_at< " + to_string_unwrap_seq( n->children.front() ) + " >";
         } );

         nrv.add< grammar::concatenation >( []( const node_ptr& n ) {
            assert( !n->children.empty() );
            return prefix + "seq< " + to_string( n->children ) + " >";
         } );

         nrv.add< grammar::repetition >( []( const node_ptr& n ) -> std::string {
            assert( n->children.size() == 2 );
            const auto content = to_string_unwrap_seq( n->children.back() );
            const auto rep = n->children.front()->string();
            const auto star = rep.find( '*' );
            if( star == std::string::npos ) {
               const auto v = remove_leading_zeroes( rep );
               if( v.empty() ) {
#if defined( __cpp_exceptions )
                  throw parse_error( "repetition of zero not allowed", n->begin() );
#else
                  std::cerr << "repetition of zero not allowed" << std::endl;
                  std::terminate();
#endif
               }
               return prefix + "rep< " + v + ", " + content + " >";
            }
            const auto min = remove_leading_zeroes( rep.substr( 0, star ) );
            const auto max = remove_leading_zeroes( rep.substr( star + 1 ) );
            if( ( star != rep.size() - 1 ) && max.empty() ) {
#if defined( __cpp_exceptions )
               throw parse_error( "repetition maximum of zero not allowed", n->begin() );
#else
               std::cerr << "repetition maximum of zero not allowed" << std::endl;
               std::terminate();
#endif
            }
            if( min.empty() && max.empty() ) {
               return prefix + "star< " + content + " >";
            }
            if( !min.empty() && max.empty() ) {
               if( min == "1" ) {
                  return prefix + "plus< " + content + " >";
               }
               return prefix + "rep_min< " + min + ", " + content + " >";
            }
            if( min.empty() && !max.empty() ) {
               if( max == "1" ) {
                  return prefix + "opt< " + content + " >";
               }
               return prefix + "rep_max< " + max + ", " + content + " >";
            }
            unsigned long long min_val;
            unsigned long long max_val;
            {
               std::stringstream s;
               s.str( min );
               s >> min_val;
               s.clear();
               s.str( max );
               s >> max_val;
            }
            if( min_val > max_val ) {
#if defined( __cpp_exceptions )
               throw parse_error( "repetition minimum which is greater than the repetition maximum not allowed", n->begin() );
#else
               std::cerr << "repetition minimum which is greater than the repetition maximum not allowed" << std::endl;
               std::terminate();
#endif
            }
            if( ( min_val == 1 ) && ( max_val == 1 ) ) {
               // note: content can not be used here!
               return to_string( n->children.back() );
            }
            auto min_element = ( min_val == 1 ) ? content : ( prefix + "rep< " + min + ", " + content + " >" );
            if( min_val == max_val ) {
               return min_element;
            }
            std::ostringstream oss;
            oss << ( max_val - min_val );
            const auto max_element = prefix + ( ( max_val - min_val == 1 ) ? "opt< " : ( "rep_opt< " + std::move( oss ).str() + ", " ) ) + content + " >";
            return prefix + "seq< " + min_element + ", " + max_element + " >";
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

   }  // namespace abnf

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
      const auto root = parse_tree::parse< abnf::grammar::rulelist, abnf::selector, nothing, abnf::control >( in );

      for( const auto& rule : root->children ) {
         abnf::rules_defined.push_back( abnf::get_rulename( rule->children.front() ) );
      }

      for( const auto& rule : root->children ) {
         std::cout << abnf::to_string( rule ) << '\n';
      }
   }
   catch( const parse_error& e ) {
      const auto p = e.positions().front();
      std::cerr << e.what() << '\n'
                << in.line_at( p ) << '\n'
                << std::setw( p.column ) << '^' << '\n';
   }
#else
   if( const auto root = parse_tree::parse< abnf::grammar::rulelist, abnf::selector, nothing, abnf::control >( in ) ) {
      for( const auto& rule : root->children ) {
         abnf::rules_defined.push_back( abnf::get_rulename( rule->children.front() ) );
      }

      for( const auto& rule : root->children ) {
         std::cout << abnf::to_string( rule ) << '\n';
      }
   }
   else {
      std::cerr << "error occurred" << std::endl;
      return 1;
   }
#endif

   return 0;
}
