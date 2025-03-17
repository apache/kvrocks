// Copyright (c) 2020-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

#include <tao/pegtl/contrib/predicates.hpp>

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< predicates_or< one< ' ' > > >( __LINE__, __FILE__, true, false );
      verify_analyze< predicates_or< one< ' ' >, range< 'a', 'z' > > >( __LINE__, __FILE__, true, false );

      verify_rule< predicates_or< one< 'a' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );

      for( char i = 1; i < 'a'; ++i ) {
         char t[] = { i, 0 };
         verify_rule< predicates_or< one< 'a' > > >( __LINE__, __FILE__, std::string( t ), result_type::local_failure, 1 );
      }
      for( char i = 'b'; i < 127; ++i ) {
         char t[] = { i, 0 };
         verify_rule< predicates_or< one< 'a' > > >( __LINE__, __FILE__, std::string( t ), result_type::local_failure, 1 );
      }

      verify_rule< predicates_or< one< 'a', 'b' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< predicates_or< one< 'a', 'b' > > >( __LINE__, __FILE__, "b", result_type::success, 0 );

      for( char i = 1; i < 'a'; ++i ) {
         char t[] = { i, 0 };
         verify_rule< predicates_or< one< 'a', 'b' > > >( __LINE__, __FILE__, std::string( t ), result_type::local_failure, 1 );
      }
      for( char i = 'c'; i < 127; ++i ) {
         char t[] = { i, 0 };
         verify_rule< predicates_or< one< 'a', 'b' > > >( __LINE__, __FILE__, std::string( t ), result_type::local_failure, 1 );
      }

      verify_rule< predicates_or< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< predicates_or< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, "b", result_type::success, 0 );

      for( char i = 1; i < 'a'; ++i ) {
         char t[] = { i, 0 };
         verify_rule< predicates_or< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, std::string( t ), result_type::local_failure, 1 );
      }
      for( char i = 'c'; i < 127; ++i ) {
         char t[] = { i, 0 };
         verify_rule< predicates_or< one< 'a' >, one< 'b' > > >( __LINE__, __FILE__, std::string( t ), result_type::local_failure, 1 );
      }

      verify_rule< predicates_or< range< 'a', 'b' > > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< predicates_or< range< 'a', 'b' > > >( __LINE__, __FILE__, "b", result_type::success, 0 );

      for( char i = 1; i < 'a'; ++i ) {
         char t[] = { i, 0 };
         verify_rule< predicates_or< range< 'a', 'b' > > >( __LINE__, __FILE__, std::string( t ), result_type::local_failure, 1 );
      }
      for( char i = 'c'; i < 127; ++i ) {
         char t[] = { i, 0 };
         verify_rule< predicates_or< range< 'a', 'b' > > >( __LINE__, __FILE__, std::string( t ), result_type::local_failure, 1 );
      }

      using rule_t = predicates_or< one< 'f' >, range< 'b', 'd' >, range< 'm', 'n' >, one< 'x', 'y' > >;

      verify_rule< rule_t >( __LINE__, __FILE__, "b", result_type::success, 0 );
      verify_rule< rule_t >( __LINE__, __FILE__, "c", result_type::success, 0 );
      verify_rule< rule_t >( __LINE__, __FILE__, "d", result_type::success, 0 );
      verify_rule< rule_t >( __LINE__, __FILE__, "m", result_type::success, 0 );
      verify_rule< rule_t >( __LINE__, __FILE__, "n", result_type::success, 0 );
      verify_rule< rule_t >( __LINE__, __FILE__, "x", result_type::success, 0 );
      verify_rule< rule_t >( __LINE__, __FILE__, "y", result_type::success, 0 );
      verify_rule< rule_t >( __LINE__, __FILE__, "f", result_type::success, 0 );

      verify_rule< rule_t >( __LINE__, __FILE__, "a", result_type::local_failure, 1 );
      verify_rule< rule_t >( __LINE__, __FILE__, "e", result_type::local_failure, 1 );
      verify_rule< rule_t >( __LINE__, __FILE__, "g", result_type::local_failure, 1 );
      verify_rule< rule_t >( __LINE__, __FILE__, "l", result_type::local_failure, 1 );
      verify_rule< rule_t >( __LINE__, __FILE__, "w", result_type::local_failure, 1 );
      verify_rule< rule_t >( __LINE__, __FILE__, "z", result_type::local_failure, 1 );
      verify_rule< rule_t >( __LINE__, __FILE__, "k", result_type::local_failure, 1 );
      verify_rule< rule_t >( __LINE__, __FILE__, "r", result_type::local_failure, 1 );

      using pred_t = predicates_or< one< 'a' >, predicates_or< one< 'b' > > >;

      verify_rule< predicates_or< pred_t > >( __LINE__, __FILE__, "a", result_type::success, 0 );
      verify_rule< predicates_or< pred_t > >( __LINE__, __FILE__, "b", result_type::success, 0 );

      for( char i = 1; i < 'a'; ++i ) {
         char t[] = { i, 0 };
         verify_rule< predicates_or< pred_t > >( __LINE__, __FILE__, std::string( t ), result_type::local_failure, 1 );
      }
      for( char i = 'c'; i < 127; ++i ) {
         char t[] = { i, 0 };
         verify_rule< predicates_or< pred_t > >( __LINE__, __FILE__, std::string( t ), result_type::local_failure, 1 );
      }
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
