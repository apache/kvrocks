// Copyright (c) 2021-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#if !defined( __cpp_exceptions )
#include <iostream>
int main()
{
   std::cerr << "Exception support required, example unavailable." << std::endl;
   return 1;
}
#else

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <vector>

#include <tao/pegtl.hpp>

namespace TAO_PEGTL_NAMESPACE::expression
{
   // Expression parsing with prefix, postfix and infix operators, ternary
   // operator and a couple of other special cases supported.

   // The handling of operator precedences with left and right binding power is
   // based on https://github.com/matklad/minipratt/blob/master/src/bin/pratt.rs

   // It correctly recognises all operators with their precedence and associativity,
   // however is still very much work-in-progress regarding a lot of details...

   // TODO: Fix missing whitespace-skip before infix/postfix operators.
   // TODO: Decide whether to use must everywhere or nowhere?
   // TODO: Decide whether to suppress actions for sub-rules.
   // TODO: Finalise the event-style interface or change to fake actions or actions with ops?
   // TODO: Decide on where to use config vs. where to use grammar template parameters.
   // TODO: Choose customisation points vs. copy-n-paste customisation.
   // TODO: Constexpr-ify where possible with C++20.

   namespace internal
   {
      struct prefix_info
      {
         prefix_info( const std::string_view n, const unsigned pbp ) noexcept
            : name( n ),
              prefix_binding_power( pbp )
         {
            assert( pbp );
         }

         std::string name;

         unsigned prefix_binding_power;
      };

      struct infix_postfix_info
      {
         infix_postfix_info( const std::string_view n, const unsigned lbp, const unsigned rbp = 0 ) noexcept
            : infix_postfix_info( n, std::string_view(), lbp, rbp )
         {}

         infix_postfix_info( const std::string_view n, const std::string_view o, const unsigned lbp, const unsigned rbp = 0 ) noexcept
            : name( n ),
              other( o ),
              left_binding_power( lbp ),
              right_binding_power( rbp )
         {
            if( right_binding_power > 0 ) {
               assert( std::min( left_binding_power, right_binding_power ) & 1 );
               assert( 2 * std::min( left_binding_power, right_binding_power ) + 1 == left_binding_power + right_binding_power );
            }
            assert( left_binding_power > 0 );
         }

         [[nodiscard]] bool is_infix() const noexcept
         {
            return right_binding_power != 0;
         }

         [[nodiscard]] bool is_postfix() const noexcept
         {
            return right_binding_power == 0;
         }

         std::string name;
         std::string other;  // Used for the ':' of the ternary operator etc.

         unsigned left_binding_power;
         unsigned right_binding_power;
      };

      template< typename ParseInput >
      [[nodiscard]] bool match_string_view( ParseInput& in, const std::string_view sv )
      {
         if( in.size( sv.size() ) >= sv.size() ) {
            if( std::memcmp( in.current(), sv.data(), sv.size() ) == 0 ) {
               in.bump( sv.size() );
               return true;
            }
         }
         return false;
      }

      template< typename ParseInput, typename OperatorInfo >
      [[nodiscard]] const OperatorInfo* match_prefix( ParseInput& in, const std::size_t max_length, const std::vector< OperatorInfo >& ops )
      {
         const std::size_t max = std::min( max_length, in.size( max_length ) );
         for( std::string op( in.current(), max ); !op.empty(); op.pop_back() ) {
            if( const auto i = std::find_if( ops.begin(), ops.end(), [ = ]( const OperatorInfo& info ) { return info.name == op; } ); i != ops.end() ) {
               in.bump( op.size() );
               return &*i;
            }
         }
         return nullptr;
      }

      template< typename ParseInput, typename OperatorInfo >
      [[nodiscard]] const OperatorInfo* match_infix_postfix( ParseInput& in, const std::size_t max_length, const std::vector< OperatorInfo >& ops, const unsigned min_precedence )
      {
         const std::size_t max = std::min( max_length, in.size( max_length ) );
         for( std::string op( in.current(), max ); !op.empty(); op.pop_back() ) {
            if( const auto i = std::find_if( ops.begin(), ops.end(), [ = ]( const OperatorInfo& info ) { return info.name == op; } ); ( i != ops.end() ) && ( i->left_binding_power >= min_precedence ) ) {
               in.bump( op.size() );
               return &*i;
            }
         }
         return nullptr;
      }

      template< typename T >
      [[nodiscard]] std::vector< T > sorted_operator_vector( const std::initializer_list< T >& t )
      {
         std::vector< T > v{ t };
         const auto less = []( const auto& l, const auto& r ) { return l.name < r.name; };
         std::sort( v.begin(), v.end(), less );
         return v;
      }

      struct operator_maps
      {
         operator_maps()
            : prefix( sorted_operator_vector(
               { prefix_info( "!", 80 ),
                 prefix_info( "+", 80 ),
                 prefix_info( "-", 80 ),
                 prefix_info( "~", 80 ),
                 prefix_info( "*", 80 ),
                 prefix_info( "&", 80 ),
                 prefix_info( "++", 80 ),
                 prefix_info( "--", 80 ) } ) ),
              infix_postfix( sorted_operator_vector(
                 { infix_postfix_info( "::", 99, 100 ),  // Special: Followed by identifier (or template-space-identifer, which we don't support yet).
                   infix_postfix_info( ".*", 37, 38 ),
                   infix_postfix_info( "->*", 37, 38 ),
                   infix_postfix_info( "*", 35, 36 ),
                   infix_postfix_info( "/", 35, 36 ),
                   infix_postfix_info( "%", 35, 36 ),
                   infix_postfix_info( "+", 33, 34 ),
                   infix_postfix_info( "-", 33, 34 ),
                   infix_postfix_info( "<<", 31, 32 ),
                   infix_postfix_info( ">>", 31, 32 ),
                   infix_postfix_info( "<=>", 29, 30 ),
                   infix_postfix_info( "<", 27, 28 ),
                   infix_postfix_info( "<=", 27, 28 ),
                   infix_postfix_info( ">", 27, 28 ),
                   infix_postfix_info( ">=", 27, 28 ),
                   infix_postfix_info( "==", 25, 26 ),
                   infix_postfix_info( "!=", 25, 26 ),
                   infix_postfix_info( "&", 23, 24 ),
                   infix_postfix_info( "^", 21, 22 ),
                   infix_postfix_info( "|", 19, 20 ),
                   infix_postfix_info( "&&", 17, 18 ),
                   infix_postfix_info( "||", 15, 16 ),
                   infix_postfix_info( "?", ":", 14, 13 ),  // Special: Ternary operator.
                   infix_postfix_info( "=", 12, 11 ),
                   infix_postfix_info( "+=", 12, 11 ),
                   infix_postfix_info( "-=", 12, 11 ),
                   infix_postfix_info( "*=", 12, 11 ),
                   infix_postfix_info( "/=", 12, 11 ),
                   infix_postfix_info( "%=", 12, 11 ),
                   infix_postfix_info( "<<=", 12, 11 ),
                   infix_postfix_info( ">>=", 12, 11 ),
                   infix_postfix_info( "&=", 12, 11 ),
                   infix_postfix_info( "^=", 12, 11 ),
                   infix_postfix_info( "|=", 12, 11 ),
                   // infix_postfix_info( ",", 9, 10 ),  // TODO: Enable, but forbid in function argument list.
                   infix_postfix_info( "[", "]", 90 ),  // Special: Argument list.
                   infix_postfix_info( "(", ")", 90 ),  // Special: Argument list.
                   infix_postfix_info( ".", 90 ),       // Special: Followed by identifier.
                   infix_postfix_info( "->", 90 ),      // Special: Followed by identifier.
                   infix_postfix_info( "++", 90 ),
                   infix_postfix_info( "--", 90 ) } ) ),
              max_prefix_length( std::max_element( prefix.begin(), prefix.end(), []( const auto& l, const auto& r ) { return l.name.size() < r.name.size(); } )->name.size() ),
              max_infix_postfix_length( std::max_element( infix_postfix.begin(), infix_postfix.end(), []( const auto& l, const auto& r ) { return l.name.size() < r.name.size(); } )->name.size() )
         {
            // These are C++20 operators with the correct associativity and relative precedence, however some are still missing:
            // TODO: Compound literal (C99), _Alignof (C11), Functional cast, sizeof, co_await, co_yield, throw, new, new[], delete, delete[], C-style casts.
         }

         const std::vector< prefix_info > prefix;
         const std::vector< infix_postfix_info > infix_postfix;

         const std::size_t max_prefix_length;
         const std::size_t max_infix_postfix_length;
      };

      struct string_view_rule
      {
         template< apply_mode A,
                   rewind_mode M,
                   template< typename... >
                   class Action,
                   template< typename... >
                   class Control,
                   typename ParseInput >
         [[nodiscard]] static bool match( ParseInput& in, const std::string_view sv ) noexcept( noexcept( match_string_view( in, sv ) ) )
         {
            return match_string_view( in, sv );
         }
      };

      struct comment
         : seq< one< '#' >, until< eolf > >
      {};

      struct ignored
         : sor< space, comment >
      {};

      template< typename Literal, typename Identifier >
      struct expression;

      template< typename Literal, typename Identifier >
      struct bracket_expression
      {
         template< apply_mode A,
                   rewind_mode M,
                   template< typename... >
                   class Action,
                   template< typename... >
                   class Control,
                   typename ParseInput,
                   typename Result,
                   typename Config >
         [[nodiscard]] static bool match( ParseInput& in, Result& res, const Config& cfg, const unsigned /*unused*/ )
         {
            return Control< if_must< one< '(' >, star< ignored >, expression< Literal, Identifier >, star< ignored >, one< ')' > > >::template match< A, M, Action, Control >( in, res, cfg, 0 );
         }
      };

      template< typename Literal, typename Identifier >
      struct prefix_expression
      {
         template< apply_mode A,
                   rewind_mode M,
                   template< typename... >
                   class Action,
                   template< typename... >
                   class Control,
                   typename ParseInput,
                   typename Result,
                   typename Config >
         [[nodiscard]] static bool match( ParseInput& in, Result& res, const Config& cfg, const unsigned /*unused*/ )
         {
            if( const auto* info = match_prefix( in, cfg.max_prefix_length, cfg.prefix ) ) {
               (void)Control< must< star< ignored >, expression< Literal, Identifier > > >::template match< A, M, Action, Control >( in, res, cfg, info->prefix_binding_power );
               if constexpr( A == apply_mode::action ) {
                  res.prefix( info->name );
               }
               return true;
            }
            return false;
         }
      };

      template< typename Literal, typename Identifier >
      struct infix_postfix_expression
      {
         template< apply_mode A,
                   rewind_mode M,
                   template< typename... >
                   class Action,
                   template< typename... >
                   class Control,
                   typename ParseInput,
                   typename Result,
                   typename Config >
         [[nodiscard]] static bool match( ParseInput& in, Result& res, const Config& cfg, const unsigned min )
         {
            if( const auto* info = match_infix_postfix( in, cfg.max_infix_postfix_length, cfg.infix_postfix, min ) ) {
               if( info->name == "?" ) {
                  (void)Control< must< star< ignored >, expression< Literal, Identifier > > >::template match< A, M, Action, Control >( in, res, cfg, 0 );
                  (void)Control< must< star< ignored >, string_view_rule > >::template match< A, M, Action, Control >( in, info->other );
                  (void)Control< must< star< ignored >, expression< Literal, Identifier > > >::template match< A, M, Action, Control >( in, res, cfg, info->right_binding_power );
                  if constexpr( A == apply_mode::action ) {
                     res.ternary( info->name, info->other );
                  }
                  return true;
               }
               if( ( info->name == "." ) || ( info->name == "::" ) || ( info->name == "->" ) ) {
                  (void)Control< must< star< ignored >, Identifier > >::template match< A, M, Action, Control >( in, res, cfg, 0 );
                  if constexpr( A == apply_mode::action ) {
                     res.infix( info->name );
                  }
                  return true;
               }
               if( ( info->name == "(" ) || ( info->name == "[" ) ) {
                  const std::size_t size = res.string_stack.size();  // TODO: Determine number of arguments without relying on res!!!
                  (void)Control< must< star< ignored >, opt< list_must< expression< Literal, Identifier >, one< ',' >, ignored > > > >::template match< A, M, Action, Control >( in, res, cfg, 0 );
                  (void)Control< must< star< ignored >, string_view_rule > >::template match< A, M, Action, Control >( in, info->other );
                  if constexpr( A == apply_mode::action ) {
                     res.call( info->name, info->other, res.string_stack.size() - size );
                  }
                  return true;
               }
               if( info->is_infix() ) {
                  (void)Control< must< star< ignored >, expression< Literal, Identifier > > >::template match< A, M, Action, Control >( in, res, cfg, info->right_binding_power );
                  if constexpr( A == apply_mode::action ) {
                     res.infix( info->name );
                  }
                  return true;
               }
               if( info->is_postfix() ) {
                  if constexpr( A == apply_mode::action ) {
                     res.postfix( info->name );
                  }
                  return true;
               }
            }
            return false;
         }
      };

      template< typename Literal, typename Identifier >
      struct first_expression
         : sor< Literal, Identifier, bracket_expression< Literal, Identifier >, prefix_expression< Literal, Identifier > >
      {};

      template< typename Literal, typename Identifier >
      struct expression
         : seq< first_expression< Literal, Identifier >, star< infix_postfix_expression< Literal, Identifier > > >
      {};

   }  // namespace internal

   template< typename Literal, typename Identifier >
   struct grammar
   {
      using rule_t = grammar;
      using subs_t = type_list< internal::expression< Literal, Identifier > >;

      template< apply_mode A,
                rewind_mode M,
                template< typename... >
                class Action,
                template< typename... >
                class Control,
                typename ParseInput,
                typename Result >
      [[nodiscard]] static bool match( ParseInput& in, Result& res )
      {
         const internal::operator_maps cfg;
         return match< A, M, Action, Control >( in, res, cfg );
      }

      template< apply_mode A,
                rewind_mode M,
                template< typename... >
                class Action,
                template< typename... >
                class Control,
                typename ParseInput,
                typename Result,
                typename Config >
      [[nodiscard]] static bool match( ParseInput& in, Result& res, const Config& cfg )
      {
         return Control< internal::expression< Literal, Identifier > >::template match< A, M, Action, Control >( in, res, cfg, 0 );
      }
   };

}  // namespace TAO_PEGTL_NAMESPACE::expression

namespace application
{
   namespace pegtl = TAO_PEGTL_NAMESPACE;

   [[nodiscard]] inline std::string operator+( const char* l, const std::string_view r )
   {
      return std::string( l ) + " '" + std::string( r ) + "'";
   }

   struct result
   {
      void infix( const std::string_view op )
      {
         assert( string_stack.size() >= 2 );

         std::string tmp = "( " + string_stack.at( string_stack.size() - 2 ) + " " + std::string( op ) + " " + string_stack.at( string_stack.size() - 1 ) + " )";
         string_stack.pop_back();
         string_stack.back() = std::move( tmp );
      }

      void prefix( const std::string_view op )
      {
         assert( string_stack.size() >= 1 );  // NOLINT(readability-container-size-empty)

         std::string tmp = std::string( op ) + "( " + string_stack.at( string_stack.size() - 1 ) + " )";
         string_stack.back() = std::move( tmp );
      }

      void postfix( const std::string_view op )
      {
         assert( string_stack.size() >= 1 );  // NOLINT(readability-container-size-empty)

         std::string tmp = "( " + string_stack.at( string_stack.size() - 1 ) + " )" + std::string( op );
         string_stack.back() = std::move( tmp );
      }

      void ternary( const std::string_view op, const std::string_view o2 )
      {
         assert( string_stack.size() >= 2 );

         std::string tmp = "( " + string_stack.at( string_stack.size() - 3 ) + " " + std::string( op ) + " " + string_stack.at( string_stack.size() - 2 ) + " " + std::string( o2 ) + " " + string_stack.at( string_stack.size() - 1 ) + " )";
         string_stack.pop_back();
         string_stack.pop_back();
         string_stack.back() = std::move( tmp );
      }

      void call( const std::string_view op, const std::string_view o2, const std::size_t args )
      {
         assert( string_stack.size() > args );

         std::string tmp = *( string_stack.end() - args - 1 ) + std::string( op ) + " ";
         for( std::size_t i = 0; i < args; ++i ) {
            if( i > 0 ) {
               tmp += ", ";
            }
            tmp += *( string_stack.end() - args + i );
         }
         tmp += " " + std::string( o2 );
         string_stack.resize( string_stack.size() - args );
         string_stack.back() = std::move( tmp );
      }

      void number( const std::int64_t l )
      {
         string_stack.emplace_back( std::to_string( l ) );
      }

      void identifier( const std::string& id )
      {
         string_stack.emplace_back( id );
      }

      std::vector< std::string > string_stack;
   };

   struct literal
      : pegtl::plus< pegtl::digit >
   {};

   struct grammar
      : pegtl::must< pegtl::expression::grammar< literal, pegtl::identifier >, pegtl::eof >
   {};

   template< typename Rule >
   struct action
      : pegtl::nothing< Rule >
   {};

   template<>
   struct action< literal >
   {
      template< typename Input, typename... States >
      static void apply( const Input& in, result& res, States&&... /*unused*/ )
      {
         res.number( std::stoll( in.string() ) );
      }
   };

   template<>
   struct action< pegtl::identifier >
   {
      template< typename Input, typename... States >
      static void apply( const Input& in, result& res, States&&... /*unused*/ )
      {
         res.identifier( in.string() );
      }
   };

}  // namespace application

int main( int argc, char** argv )
{
   // if( TAO_PEGTL_NAMESPACE::analyze< application::grammar >() != 0 ) {
   //    return 1;
   // }
   for( int i = 1; i < argc; ++i ) {
      TAO_PEGTL_NAMESPACE::argv_input in( argv, i );
      try {
         application::result res;
         TAO_PEGTL_NAMESPACE::parse< application::grammar, application::action >( in, res );
         std::cout << "Input: " << argv[ i ] << std::endl;
         assert( res.string_stack.size() == 1 );
         std::cout << "Result: " << res.string_stack.at( 0 ) << std::endl;
      }
      catch( const TAO_PEGTL_NAMESPACE::parse_error& e ) {
         const auto p = e.positions().front();
         std::cerr << e.what() << '\n'
                   << in.line_at( p ) << '\n'
                   << std::setw( p.column ) << '^' << '\n';
      }
   }
   return 0;
}

#endif
