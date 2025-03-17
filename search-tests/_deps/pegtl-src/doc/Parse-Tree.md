# Parse Tree

The PEGTL provides facilities for building a [parse tree](https://en.wikipedia.org/wiki/Parse_tree) (or [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree)).

It provides the basic infrastructure to build a parse tree that

* by default is *complete*, with one tree node for every successfully matched rule,
  * with an optional *selection* to only generate nodes for a chosen subset of rules;
* by default uses the included tree node class that stores all pertinent information,
  * but can also be used with a custom tree node class that adheres to certain rules;
* and supports on-the-fly tree transformations; some of the more common ones are included.

## Content

* [Full Parse Tree](#full-parse-tree)
* [Partial Parse Tree](#partial-parse-tree)
* [Transforming Nodes](#transforming-nodes)
* [Transformers](#transformers)
* [`tao::pegtl::parse_tree::node`](#taopegtlparse_treenode)
* [Custom Node Class](#custom-node-class)
* [Requirements](#requirements)

## Full Parse Tree

To obtain a (complete) parse tree, simply call `tao::pegtl::parse_tree::parse()` with a grammar and an input.

```c++
#include <tao/pegtl/contrib/parse_tree.hpp>

auto root = tao::pegtl::parse_tree::parse< my_grammar >( in );
```

The result is a `std::unique_ptr< tao::pegtl::parse_tree::node >`.
The pointer is empty when the input did not match the grammar, otherwise it contains the root node of the resulting parse tree.

The tree nodes have a `type` member that contains the name of the grammar rule of which it represents a successful match, `begin()` and `end()` member functions to access the position of the matched portion of the input, `string()` and `string_view()` to actually access said matched input, and a vector called `children` with unique pointers to the child nodes.

Note that the included tree node class **points** to the matched data, rather than copying it into the node, wherefore the input **must** still be "alive" when accessing the matched data!

A more complete description of the included tree node class can be found below.

## Partial Parse Tree

Usually only a subset of grammar rules should generate parse tree nodes.
In order to select the grammar rules for which a successful match is to generate a parse tree node, a second template parameter can be passed to `parse_tree::parse()`.
This parameter is a class template that is called a *selector*.
For each rule `Rule`, the boolean value `selector< Rule >::value` determines whether a parse tree node is generated.

```c++
template< typename Rule > struct my_selector : std::false_type {};
template<> struct my_selector< my_rule_1 > : std::true_type {};
template<> struct my_selector< my_rule_2 > : std::true_type {};
template<> struct my_selector< my_rule_3 > : std::true_type {};

// ...

auto root = tao::pegtl::parse_tree::parse< my_grammar, my_selector >( in );
```

Note that the example uses a white-list style; the default is `std::false_type` and only rules listed with a specialisation deriving from `std::true_type` will generate nodes.
The opposite, a black-list style, is of course possible, too.

The PEGTL includes a selector class and additional utility classes to allow for a less verbose specification of a selector.
The following definition of `my_selector` will behave just like the one above.

```c++
template< typename Rule >
using my_selector = tao::pegtl::parse_tree::selector< Rule,
   tao::pegtl::parse_tree::store_content::on<
      my_rule_1,
      my_rule_2,
      my_rule_3 > >;

// ...

auto root = tao::pegtl::parse_tree::parse< my_grammar, my_selector >( in );
```

Note that `store_content` further specifies that the information about the matched portion of the input be stored in the generated nodes; other possibilities are discussed below.

## Transforming Nodes

A parse tree, full or partial, can still be too closely related to the structure of the grammar.
In order to simplify the tree (or otherwise improve its structure), an optional `transform()` static member function can be added to each specialization of the selector class template (that generates a node).
This function gets passed a reference to the current node, which also gives access to the children, but not to the parent:

```c++
template<> struct my_selector< my_rule_2 > : std::true_type
{
   static void transform( std::unique_ptr< node >& n )
   {
      // modify n...
   }
};
```

`transform` can modify `n` in any way you like, the [`parse_tree.cpp`](https://github.com/taocpp/PEGTL/blob/main/src/example/pegtl/parse_tree.cpp)-example shows two techniques for marking nodes as "content-less", and for transforming the parse tree into an AST.

It is also possible to call `n.reset()`, or otherwise set `n` to an empty pointer, which effectively removes `n` (and all of its child nodes) from the parse tree.

## Transformer

As shown above, the selector class template allows to specify which nodes should be stored. Several additional helper classes are predefined that have common `transform` methods. The selector allows to add multiple sections with different helpers like this:

```c++
template< typename Rule >
using my_selector = tao::pegtl::parse_tree::selector< Rule,
   tao::pegtl::parse_tree::store_content::on<
      my_rule_1,
      my_rule_2,
      my_rule_3 >,
   tao::pegtl::parse_tree::remove_content::on<
      my_rule_4,
      my_rule_5 >,
   tao::pegtl::parse_tree::apply< my_helper >::on<
      my_rule_7,
      my_rule_8 > >;
```

Note that each rule may only be used in one section, it is an error to add a rule to multiple sections.

`store_content` and `remove_content` are predefined by the library, whereas `my_helper` can be defined by yourself.

###### `tao::pegtl::parse_tree::store_content`

This stores the node, including pointing to the content it matched on.

###### `tao::pegtl::parse_tree::remove_content`

This stores the node, but calls the node's `remove_content` member function.

###### `tao::pegtl::parse_tree::fold_one`

This stores the node, but when a node has exactly one child, the node replaces itself with this child, otherwise removes its own content (not children).

###### `tao::pegtl::parse_tree::discard_empty`

This stores the node, except for when the node does *not* have any children, in which case it removes itself, otherwise removes its own content (not children).

### Example

An example of using some of the transformers can be found in `src/example/pegtl/abnf2pegtl.cpp`.

## `tao::pegtl::parse_tree::node`

This is the interface of the node class used by `tao::pegtl::parse_tree::parse` when no custom node class is specified.

```c++
template< typename T, typename Source = std::string_view >
struct basic_node
{
   using node_t = T;
   using children_t = std::vector< std::unique_ptr< node_t > >;

   children_t children;
   std::string_view type;
   Source source;

   bool is_root() const noexcept;

   template< typename U >
   bool is_type() const noexcept;

   template< typename U >
   void set_type() noexcept;

   // precondition from here on: !is_root()

   position begin() const;
   position end() const;

   bool has_content() const noexcept;
   std::string_view string_view() const noexcept;  // precondition: has_content()
   std::string string() const;  // precondition: has_content()

   template< tracking_mode P = tracking_mode::eager, typename Eol = eol::lf_crlf >
   memory_input< P, Eol > as_memory_input() const;

   // useful for transform:
   void remove_content();
};

struct node : basic_node< node > {};
```

The name is the demangled name of the rule. By default, all nodes (except the root node) can provide the content that matched, i.e. the part of the input that the rule the node was created for matched. It is only necessary to check `has_content()` when `remove_content()` was used by a transform function (either directly or indirectly via one of the convenience helpers), otherwise all nodes except for the root will always "have content".

See [`parse_tree.cpp`](https://github.com/taocpp/PEGTL/blob/main/src/example/pegtl/parse_tree.cpp) for more information on how to output (or otherwise use) the nodes.

## Custom Node Class

For more control over how data is stored/handled in the nodes, a custom node class can be utilised.
The type of node is passed as an additional template parameter to the parse tree `parse()` function.

```c++
auto r1 = tao::pegtl::parse_tree::parse< my_grammar, my_node >( in );
auto r2 = tao::pegtl::parse_tree::parse< my_grammar, my_node, my_selector >( in );
```

Note that `my_node` is a class, while `my_selector` is a class template. A custom node class must implement the following interface.

```c++
struct my_node
{
   // Must be default constructible
   my_node() = default;

   // Nodes are always owned/handled by a std::unique_ptr
   // and never copied or assigned...
   my_node( const my_node& ) = delete;
   my_node( my_node&& ) = delete;
   my_node& operator=( const my_node& ) = delete;
   my_node& operator=( my_node&& ) = delete;

   // Must be destructible
   ~my_node() = default;

   // All non-root nodes receive a call to start() when
   // a match is attempted for Rule in a parsing run...
   template< typename Rule, typename ParseInput, typename... States >
   void start( const ParseInput& in, States&&... st );

   // ...and later a call to success() when the match succeeded...
   template< typename Rule, typename ParseInput, typename... States >
   void success( const ParseInput& in, States&&... st );

   // ...or to failure() when a (local) failure was encountered.
   template< typename Rule, typename ParseInput, typename... States >
   void failure( const ParseInput& in, States&&... st );

   // After a call to success(), and the (optional) call to the selector's
   // transform() did not discard a node, it is passed to its parent node
   // with a call to the parent node's emplace_back() member function.
   template< typename... States >
   void emplace_back( std::unique_ptr< node_t > child, States&&... st );
};
```

## Requirements

The parse tree uses a rule's meta data supplied by [`subs_t`](Meta-Data-and-Visit.md#sub-rules) for internal optimizations.

Copyright (c) 2018-2022 Dr. Colin Hirsch and Daniel Frey
