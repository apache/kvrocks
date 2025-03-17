// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"
#include "verify_rule.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   void unit_test()
   {
      verify_analyze< shebang >( __LINE__, __FILE__, true, false );

      verify_rule< shebang >( __LINE__, __FILE__, "", result_type::local_failure, 0 );
      verify_rule< shebang >( __LINE__, __FILE__, "#", result_type::local_failure, 1 );
      verify_rule< shebang >( __LINE__, __FILE__, "!", result_type::local_failure, 1 );
      verify_rule< shebang >( __LINE__, __FILE__, "!#", result_type::local_failure, 2 );
      verify_rule< shebang >( __LINE__, __FILE__, "#  ", result_type::local_failure, 3 );
      verify_rule< shebang >( __LINE__, __FILE__, "!  ", result_type::local_failure, 3 );
      verify_rule< shebang >( __LINE__, __FILE__, "## ", result_type::local_failure, 3 );
      verify_rule< shebang >( __LINE__, __FILE__, "!! ", result_type::local_failure, 3 );
      verify_rule< shebang >( __LINE__, __FILE__, "#!", result_type::success, 0 );
      verify_rule< shebang >( __LINE__, __FILE__, "#! ", result_type::success, 0 );
      verify_rule< shebang >( __LINE__, __FILE__, "#!/bin/bash", result_type::success, 0 );
      verify_rule< shebang >( __LINE__, __FILE__, "#!/bin/bash\n", result_type::success, 0 );
      verify_rule< shebang >( __LINE__, __FILE__, "#!/bin/bash\n#!/b", result_type::success, 4 );
      verify_rule< shebang >( __LINE__, __FILE__, "#!\n", result_type::success, 0 );
      verify_rule< shebang >( __LINE__, __FILE__, "#!\n ", result_type::success, 1 );
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
