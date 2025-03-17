# Getting Started

Since the PEGTL is a parser library, here is an "inverse hello world" example that parses,
rather than prints, the string `Hello, foo!` for any sequence of alphabetic ASCII characters `foo`.

```c++
#include <string>
#include <iostream>

#include <tao/pegtl.hpp>

namespace pegtl = tao::pegtl;

namespace hello
{
   // Parsing rule that matches a literal "Hello, ".

   struct prefix
      : pegtl::string< 'H', 'e', 'l', 'l', 'o', ',', ' ' >
   {};

   // Parsing rule that matches a non-empty sequence of
   // alphabetic ascii-characters with greedy-matching.

   struct name
      : pegtl::plus< pegtl::alpha >
   {};

   // Parsing rule that matches a sequence of the 'prefix'
   // rule, the 'name' rule, a literal "!", and 'eof'
   // (end-of-file/input), and that throws an exception
   // on failure.

   struct grammar
      : pegtl::must< prefix, name, pegtl::one< '!' >, pegtl::eof >
   {};

   // Class template for user-defined actions that does
   // nothing by default.

   template< typename Rule >
   struct action
   {};

   // Specialisation of the user-defined action to do
   // something when the 'name' rule succeeds; is called
   // with the portion of the input that matched the rule.

   template<>
   struct action< name >
   {
      template< typename ParseInput >
      static void apply( const ParseInput& in, std::string& v )
      {
         v = in.string();
      }
   };

}  // namespace hello

int main( int argc, char* argv[] )
{
   if( argc != 2 ) return 1;

   // Start a parsing run of argv[1] with the string
   // variable 'name' as additional argument to the
   // action; then print what the action put there.

   std::string name;

   pegtl::argv_input in( argv, 1 );
   pegtl::parse< hello::grammar, hello::action >( in, name );

   std::cout << "Good bye, " << name << "!" << std::endl;
   return 0;
}
```

Assuming you are in the main directory of the PEGTL, the above source can be
found in the `src/example/pegtl/` directory. Compile the program with something like

```sh
$ g++ --std=c++17 -Iinclude src/example/pegtl/hello_world.cpp -o hello_world
```

and then invoke it as follows:

```sh
$ ./hello_world 'Hello, world!'
Good bye, world!
$ ./hello_world 'Hello, Colin!'
Good bye, Colin!
$ ./hello_world 'Howdy, Paula!'
terminate called after throwing an instance of 'tao::pegtl::parse_error'
  what():  argv[1]:1:0(0): parse error matching hello::prefix
Aborted (core dumped)
```

The PEGTL provides multiple facilities that help to get started and develop your grammar.
In the following paragraphs we will show several small programs to showcase the capabilities of the PEGTL.

Note, however, that all examples shown in this document will lack proper error handling.
Frequently an application will include `try-catch` blocks to handle the exceptions.
The correct way of handling errors is shown at the last paragraph of this page.

## Parsing Expression Grammars

The PEGTL creates parsers according to a [Parsing Expression Grammar](http://en.wikipedia.org/wiki/Parsing_expression_grammar) (PEG).
The table below shows how the PEG combinators map to PEGTL [rule classes](Rule-Reference.md#combinators) (strictly speaking: class templates).
Beyond these standard combinators the PEGTL contains a [large number of additional combinators](Rule-Reference.md) as well as the possibility of [creating custom rules](Rules-and-Grammars.md#creating-new-rules).

| PEG                               | `tao::pegtl::`                                                                                            |
| ---                               | ---                                                                                                       |
| &*e*                              | [`at< R... >`](Rule-Reference.md#at-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>         |
| !*e*                              | [`not_at< R... >`](Rule-Reference.md#not_at-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup> |
| *e*?                              | [`opt< R... >`](Rule-Reference.md#opt-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>       |
| *e*+                              | [`plus< R... >`](Rule-Reference.md#plus-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>     |
| *e*<sub>1</sub>*e*<sub>2</sub>    | [`seq< R... >`](Rule-Reference.md#seq-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>       |
| *e*<sub>1</sub> / *e*<sub>2</sub> | [`sor< R... >`](Rule-Reference.md#sor-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>       |
| *e**                              | [`star< R... >`](Rule-Reference.md#star-r-) <sup>[(combinators)](Rule-Reference.md#combinators)</sup>     |

The PEGTL also contains a [large number of atomic rules](Rule-Reference.md) for matching ASCII and Unicode characters, strings, ranges and similar, beginning-of-file or end-of-line and similar, and more...

## Grammar Analysis

Every grammar must be free of cycles that make no progress, i.e. it must not contain unbounded recursive or iterative rules that do not consume any input, as such grammar might enter an infinite loop.
One common pattern for these kinds of problematic grammars is the so-called [left recursion](https://en.wikipedia.org/wiki/Left_recursion) that, while not a problem for less deterministic formalisms like CFGs, must be avoided with PEGs in order to prevent aforementioned infinite loops.

The PEGTL provides a [grammar analysis](Grammar-Analysis.md) which analyses a grammar for cycles that make no progress.
While it could be implemented with compile-time meta-programming, to prevent the compiler from exploding the analysis is done at run-time.
It is best practice to create a separate dedicated program that does nothing else but run the grammar analysis, thus keeping this development and debug aid out of the main application.

```c++
#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/analyze.hpp>

// This example uses the included JSON grammar
#include <tao/pegtl/contrib/json.hpp>

namespace pegtl = tao::pegtl;

using grammar = pegtl::must< pegtl::json::text, pegtl::eof >;

int main()
{
   if( pegtl::analyze< grammar >() != 0 ) {
      std::cerr << "cycles without progress detected!\n";
      return 1;
   }

   return 0;
}
```
For more information see [Grammar Analysis](Grammar-Analysis.md).

## Tracer

A fundamental tool used when developing a grammar is a tracer that prints every step of a parsing run, thereby showing exactly which rule was attempted to match where, and what the result was.
The PEGTL provides a tracer that will print to `stderr`, and of course allows users to write their own tracers with custom output formats.

```c++
#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/trace.hpp>

// This example uses the included JSON grammar
#include <tao/pegtl/contrib/json.hpp>

namespace pegtl = tao::pegtl;

using grammar = pegtl::must< pegtl::json::text, pegtl::eof >;

int main( int argc, char** argv )
{
   if( argc != 2 ) return 1;

   pegtl::argv_input in( argv, 1 );
   pegtl::standard_trace< grammar >( in );

   return 0;
}
```

In the above each command line parameter is parsed as a JSON string and a trace is given to understand how the grammar matches the input.

For more information see `tao/pegtl/contrib/trace.hpp`.

## Parse Tree / AST

When developing parsers, a common goal after creating the grammar is to generate a [parse tree](https://en.wikipedia.org/wiki/Parse_tree) or an [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree).

The PEGTL provides a [Parse Tree](Parse-Tree.md) builder that can filter and/or transform tree nodes on-the-fly.
Additionally, a helper is provided to print out the resulting data structure in [DOT](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) format, suitable for creating a graphical representation of the parse tree.

The following example uses a selector to choose which rules generate parse tree nodes, as the graphical representation will usually be too large and confusing when not using a filter and generating nodes for *all* rules.

```c++
#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/parse_tree.hpp>
#include <tao/pegtl/contrib/parse_tree_to_dot.hpp>

// This example uses the included JSON grammar
#include <tao/pegtl/contrib/json.hpp>

namespace pegtl = tao::pegtl;

using grammar = pegtl::must< pegtl::json::text, pegtl::eof >;

template< typename Rule >
using selector = pegtl::parse_tree::selector<
   Rule,
   pegtl::parse_tree::store_content::on<
      pegtl::json::null,
      pegtl::json::true_,
      pegtl::json::false_,
      pegtl::json::number,
      pegtl::json::string,
      pegtl::json::key,
      pegtl::json::array,
      pegtl::json::object,
      pegtl::json::member > >;

int main( int argc, char** argv )
{
   if( argc != 2 ) return 1;

   pegtl::argv_input in( argv, 1 );
   const auto root = parse_tree::parse< grammar, selector >( in );
   if( root ) {
      parse_tree::print_dot( std::cout, *root );
   }

   return 0;
}
```

Running the above program with some example input:

```sh
$ build/src/example/pegtl/json_parse_tree '{"foo":[true,{}],"bar":[42,null]}' | dot -Tsvg -o json_parse_tree.svg
```

The above will generate an SVG file with a graphical representation of the parse tree.

![JSON Parse Tree](Json-Parse-Tree.svg)

For more information see [Parse Tree](Parse-Tree.md).

## Error Handling

Although the PEGTL could be used without exceptions, most programs will use input classes, grammars and/or actions that can throw exceptions.
Typically, the following pattern helps to print the exceptions in a human friendly way:

```c++
   // The surrounding try/catch for normal exceptions.
   // These might occur if a file can not be opened, etc.
   try {
      tao::pegtl::file_input in( filename );

      // The inner try/catch block, see below...
      try {

         // The actual parser, tracer, parse tree, ...
         pegtl::parse< grammar >( in );

      }
      catch( const pegtl::parse_error& e ) {

         // This catch block needs access to the input
         const auto p = e.positions().front();
         std::cerr << e.what() << '\n'
                   << in.line_at( p ) << '\n'
                   << std::setw( p.column ) << '^' << std::endl;

      }
   }
   catch( const std::exception& e ) {

      // Generic catch block for other exceptions
      std::cerr << e.what() << std::endl;

   }
```

For more information see [Errors and Exceptions](Errors-and-Exceptions.md).

Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
