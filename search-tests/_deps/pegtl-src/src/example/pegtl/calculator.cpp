// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include <cassert>
#include <functional>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

#include <tao/pegtl.hpp>

// Include the analyze function that checks
// a grammar for possible infinite cycles.

#include <tao/pegtl/contrib/analyze.hpp>

namespace pegtl = TAO_PEGTL_NAMESPACE;

namespace calculator
{
   // This enum is used for the order in which the operators are
   // evaluated, i.e. the priority of the operators; a higher
   // number indicates a lower priority.

   enum class order : int
   {
   };

   // For each binary operator known to the calculator we need an
   // instance of the following data structure with the priority,
   // and a function that performs the calculation. All operators
   // are left-associative.

   struct op
   {
      order p;
      std::function< long( long, long ) > f;
   };

   // Class that takes care of an operand and an operator stack for
   // shift-reduce style handling of operator priority; in a
   // reduce-step it calls on the functions contained in the op
   // instances to perform the calculation.

   struct stack
   {
      void push( const op& b )
      {
         while( ( !m_o.empty() ) && ( m_o.back().p <= b.p ) ) {
            reduce();
         }
         m_o.push_back( b );
      }

      void push( const long l )
      {
         m_l.push_back( l );
      }

      long finish()
      {
         while( !m_o.empty() ) {
            reduce();
         }
         assert( m_l.size() == 1 );
         const auto r = m_l.back();
         m_l.clear();
         return r;
      }

   private:
      std::vector< op > m_o;
      std::vector< long > m_l;

      void reduce()
      {
         assert( !m_o.empty() );
         assert( m_l.size() > 1 );

         const auto r = m_l.back();
         m_l.pop_back();
         const auto l = m_l.back();
         m_l.pop_back();
         const auto o = m_o.back();
         m_o.pop_back();
         m_l.push_back( o.f( l, r ) );
      }
   };

   // Additional layer, a "stack of stacks", to clearly show how bracketed
   // sub-expressions can be easily processed by giving them a stack of
   // their own. Once a bracketed sub-expression has finished evaluation on
   // its stack, the result is pushed onto the next higher stack, and the
   // sub-expression's temporary stack is discarded. The top-level calculation
   // is handled just like a bracketed sub-expression, on the first stack pushed
   // by the constructor.

   struct stacks
   {
      stacks()
      {
         open();
      }

      void open()
      {
         m_v.emplace_back();
      }

      template< typename T >
      void push( const T& t )
      {
         assert( !m_v.empty() );
         m_v.back().push( t );
      }

      void close()
      {
         assert( m_v.size() > 1 );
         const auto r = m_v.back().finish();
         m_v.pop_back();
         m_v.back().push( r );
      }

      long finish()
      {
         assert( m_v.size() == 1 );
         return m_v.back().finish();
      }

   private:
      std::vector< stack > m_v;
   };

   // A wrapper around the data structures that contain the binary
   // operators for the calculator.

   struct operators
   {
      operators()
      {
         // By default we initialise with all binary operators from the C language that can be
         // used on integers, all with their usual priority.

         insert( "*", order( 5 ), []( const long l, const long r ) { return l * r; } );
         insert( "/", order( 5 ), []( const long l, const long r ) { return l / r; } );
         insert( "%", order( 5 ), []( const long l, const long r ) { return l % r; } );
         insert( "+", order( 6 ), []( const long l, const long r ) { return l + r; } );
         insert( "-", order( 6 ), []( const long l, const long r ) { return l - r; } );
         insert( "<<", order( 7 ), []( const long l, const long r ) { return l << r; } );
         insert( ">>", order( 7 ), []( const long l, const long r ) { return l >> r; } );
         insert( "<", order( 8 ), []( const long l, const long r ) { return l < r; } );
         insert( ">", order( 8 ), []( const long l, const long r ) { return l > r; } );
         insert( "<=", order( 8 ), []( const long l, const long r ) { return l <= r; } );
         insert( ">=", order( 8 ), []( const long l, const long r ) { return l >= r; } );
         insert( "==", order( 9 ), []( const long l, const long r ) { return l == r; } );
         insert( "!=", order( 9 ), []( const long l, const long r ) { return l != r; } );
         insert( "&", order( 10 ), []( const long l, const long r ) { return l & r; } );
         insert( "^", order( 11 ), []( const long l, const long r ) { return l ^ r; } );
         insert( "|", order( 12 ), []( const long l, const long r ) { return l | r; } );
         insert( "&&", order( 13 ), []( const long l, const long r ) { return ( ( l != 0 ) && ( r != 0 ) ) ? 1 : 0; } );
         insert( "||", order( 14 ), []( const long l, const long r ) { return ( ( l != 0 ) || ( r != 0 ) ) ? 1 : 0; } );
      }

      // Arbitrary user-defined operators can be added at runtime.

      void insert( const std::string& name, const order p, const std::function< long( long, long ) >& f )
      {
         assert( !name.empty() );
         m_ops.try_emplace( name, op{ p, f } );
      }

      [[nodiscard]] const std::map< std::string, op >& ops() const noexcept
      {
         return m_ops;
      }

   private:
      std::map< std::string, op > m_ops;
   };

   // Here the actual grammar starts.

   using namespace tao::pegtl;

   // Comments are introduced by a '#' and proceed to the end-of-line/file.

   struct comment
      : seq< one< '#' >, until< eolf > >
   {};

   // The calculator ignores all spaces and comments; space is a pegtl rule
   // that matches the usual ascii characters ' ', '\t', '\n' etc. In other
   // words, everything that is space or a comment is ignored.

   struct ignored
      : sor< space, comment >
   {};

   // Since the binary operators are taken from a runtime data structure
   // (rather than hard-coding them into the grammar), we need a custom
   // rule that attempts to match the input against the current map of
   // operators.

   struct infix
   {
      using rule_t = ascii::any::rule_t;

      template< apply_mode,
                rewind_mode,
                template< typename... >
                class Action,
                template< typename... >
                class Control,
                typename ParseInput,
                typename... States >
      static bool match( ParseInput& in, const operators& b, stacks& s, States&&... /*unused*/ )
      {
         // Look for the longest match of the input against the operators in the operator map.

         return match( in, b, s, std::string() );
      }

   private:
      template< typename ParseInput >
      static bool match( ParseInput& in, const operators& b, stacks& s, std::string t )
      {
         if( in.size( t.size() + 1 ) > t.size() ) {
            t += in.peek_char( t.size() );
            const auto i = b.ops().lower_bound( t );
            if( i != b.ops().end() ) {
               if( match( in, b, s, t ) ) {
                  return true;
               }
               if( i->first == t ) {
                  // While we are at it, this rule also performs the task of what would
                  // usually be an associated action: To push the matched operator onto
                  // the operator stack.
                  s.push( i->second );
                  in.bump( t.size() );
                  return true;
               }
            }
         }
         return false;
      }
   };

   // A number is a non-empty sequence of digits preceded by an optional sign.

   struct number
      : seq< opt< one< '+', '-' > >, plus< digit > >
   {};

   struct expression;

   // A bracketed expression is introduced by a '(' and, in this grammar, must
   // proceed with an expression and a ')'.

   struct bracket
      : seq< one< '(' >, expression, one< ')' > >
   {};

   // An atomic expression, i.e. one without operators, is either a number or
   // a bracketed expression.

   struct atomic
      : sor< number, bracket >
   {};

   // An expression is a non-empty list of atomic expressions where each pair
   // of atomic expressions is separated by an infix operator and we allow
   // the rule ignored as padding (before and after every single expression).

   struct expression
      : list< atomic, infix, ignored >
   {};

   // The top-level grammar allows one expression and then expects eof.

   struct grammar
      : seq< expression, eof >
   {};

   // After the grammar we proceed with the additional actions that are
   // required to let our calculator actually do something.

   // The base-case of the class template for the actions, does nothing.

   template< typename Rule >
   struct action
   {};

   // This action will be called when the number rule matches; it converts the
   // matched portion of the input to a long and pushes it onto the operand
   // stack.

   template<>
   struct action< number >
   {
      template< typename ActionInput >
      static void apply( const ActionInput& in, const operators& /*unused*/, stacks& s )
      {
         std::stringstream ss( in.string() );
         long v;
         ss >> v;
         s.push( v );
      }
   };

   // The actions for the brackets call functions that create, and collect
   // a temporary additional stack for evaluating the bracketed expression.

   template<>
   struct action< one< '(' > >
   {
      static void apply0( const operators& /*unused*/, stacks& s )
      {
         s.open();
      }
   };

   template<>
   struct action< one< ')' > >
   {
      static void apply0( const operators& /*unused*/, stacks& s )
      {
         s.close();
      }
   };

}  // namespace calculator

int main( int argc, char** argv )  // NOLINT(bugprone-exception-escape)
{
   // Check the grammar for some possible issues.

   if( pegtl::analyze< calculator::grammar >() != 0 ) {
      return 1;
   }

   // The objects required as state by the actions.

   calculator::stacks s;
   calculator::operators b;

   for( int i = 1; i < argc; ++i ) {
      // Parse and process the command-line arguments as calculator expressions...
      pegtl::argv_input in( argv, i );
      if( pegtl::parse< calculator::grammar, calculator::action >( in, b, s ) ) {
         // ...and print the respective results to std::cout.
         std::cout << s.finish() << std::endl;
      }
      else {
         std::cerr << "parse error for: " << argv[ i ] << std::endl;
      }
   }
   return 0;
}
