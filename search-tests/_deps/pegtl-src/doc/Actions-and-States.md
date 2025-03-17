# Actions and States

In its most simple form, a parsing run only returns whether (a portion of) the input matches the grammar.
To actually do something useful during a parsing run it is necessary to attach (user-defined) *actions* to one or more grammar rules.

Actions are essentially functions that are called during the parsing run whenever the rule they are attached to successfully matched.
When an action is *applied*, the corresponding function receives the *states*, an arbitrary list of (user-defined) objects, as arguments.

## Contents

* [Overview](#overview)
* [Example](#example)
* [States](#states)
* [Apply](#apply)
* [Apply0](#apply0)
* [Inheriting](#inheriting)
* [Specialising](#specialising)
* [Changing Actions](#changing-actions)
  * [Via Rules](#via-rules)
  * [Via Actions](#via-actions)
* [Changing States](#changing-states)
  * [Via Rules](#via-rules-1)
  * [Via Actions](#via-actions-1)
* [Changing Actions and States](#changing-actions-and-states)
* [Match](#match)
* [Nothing](#nothing)
* [Backtracking](#backtracking)
* [Troubleshooting](#troubleshooting)
  * [Boolean Return](#boolean-return)
  * [State Mismatch](#state-mismatch)
* [Legacy Actions](#legacy-actions)

## Overview

Actions are implemented as static member functions called `apply()` or `apply0()` of specialisations of custom class templates (which is not quite as difficult as it sounds).

States are additional function arguments to `tao::pegtl::parse()` that are forwarded to all actions.

To use actions during a parsing run they first need to be implemented.

* Define a custom action class template.
* Specialise the action class template for every rule for which a function is to be called and
  * either implement an `apply()` or `apply0()` static member function,
  * or derive from a class that implements the desired function.

The very first step, defining a custom action class template, usually looks like this.

```c++
template< typename Rule >
struct my_action
   : tao::pegtl::nothing< Rule > {};
```

Instantiations of the primary template for `my_action< Rule >` inherit from `tao::pegtl::nothing< Rule >` to indicate that, by default, neither `my_action< Rule >::apply()` nor `my_action< Rule >::apply0()` are to be called when `Rule` is successfully matched during a parsing run, or, in short, that no action is to be applied to `Rule`.

You then specialise the action class template for those rules that you *do* want to call an action on.
An example for a simple action for a specific state might look like this.

```c++
template<>
struct my_action< my_rule >
{
   template< typename ActionInput >
   static void apply( const ActionInput& in, my_state& s )
   {
      // ... implement
   }
};
```

Then the parsing run needs to be set up with the actions and any required states.
For this, the initial action can be passed as the second template parameter and the initial states can be passed as additional arguments to `tao::pegtl::parse()`.

In order to manage the complexity in larger parsers and/or compose multiple grammars that each bring their own actions which in turn expect certain states, it can be useful to [change the actions](#changing-actions) and/or [change the states](#changing-states) within a parsing run.

## Example

Here is a very short example that shows the basic way to put together a parsing run with actions and states.

```c++
// Define a simple grammar consisting of a single rule.
struct my_grammar
   : tao::pegtl::star< tao::pegtl::any > {};

// Primary action class template.
template< typename Rule >
struct my_action
   : tao::pegtl::nothing< Rule > {};

// Specialise the action class template.
template<>
struct my_action< tao::pegtl::any >
{
   // Implement an apply() function that will be called by
   // the PEGTL every time tao::pegtl::any matches during
   // the parsing run.
   template< typename ActionInput >
   static void apply( const ActionInput& in, std::string& out )
   {
      // Get the portion of the original input that the
      // rule matched this time as string and append it
      // to the result string.
      out += in.string();
   }
};

template< typename ParseInput >
std::string as_string( ParseInput& in )
{
   // Set up the states, here a single std::string as that is
   // what our action requires as additional function argument.
   std::string out;

   // Start the parsing run with our grammar, action and state.
   tao::pegtl::parse< my_grammar, my_action >( in, out );

   // Do something with the result.
   return out;
}
```

All together the `as_string()` function is a convoluted way of turning an [input](Inputs-and-Parsing.md) into a `std::string` byte-by-byte.

In the following we will take a more in-depth look at states and `apply()` and `apply0()` before diving into more advanced subjects.

## States

There is not much more to say about the states other than what has already been mentioned, namely that they are a list (colloquial list, not `std::list`) of objects that are

* passed by the user as additional arguments to [`tao::pegtl::parse()`](Inputs-and-Parsing.md#parse-function), and then

* passed by the PEGTL as additional arguments to all actions' `apply()` or `apply0()` static member functions.

The additional arguments to `apply()` and `apply0()` can be chosen freely, however all actions must accept the same list of states since they are all called with the same arguments by default.

States are not forwarded with "perfect forwarding" since r-value references don't make much sense when they will be used as action arguments many times.
The `parse()` function still uses universal references to bind to the state arguments in order to allow temporary objects.

## Apply

As seen above, the actual functions that are called when an action is applied are static member functions named `apply()` of the specialisations of the action class template.

```c++
template<>
struct my_action< my_rule >
{
   template< typename ActionInput >
   static void apply( const ActionInput& in, /* all the states */ )
   {
      // Called whenever matching my_rule during a parsing run
      // succeeds (and actions are not disabled). The argument
      // named 'in' represents the matched part of the input.
      // Can also return bool instead of void.
   }
}
```

The first argument is not the input used in the parsing run, but rather a separate object of distinct type that represents the portion of the input that the rule to which the action is attached just matched. The remaining arguments to `apply()` are the current state arguments.

The exact type of the input class passed to `apply()` is not specified.
It is best practice to "template over" the type of the input as shown above.

Actions can then assume that the input provides (at least) the following interface.
The `Input` template parameter is set to the class of the input used as input in the parsing run at the point where the action is applied.

For illustrative purposes, we will assume that the input passed to `apply()` is of type `action_input`.
Any resemblance to real classes is not a coincidence, see `include/tao/pegtl/internal/action_input.hpp`.

```c++
template< typename ParseInput >
class action_input
{
public:
   using input_t = ParseInput;
   using iterator_t = typename ParseInput::iterator_t;

   bool empty() const noexcept;
   std::size_t size() const noexcept;

   const char* begin() const noexcept;  // Non-owning pointer!
   const char* end() const noexcept;  // Non-owning pointer!

   std::string string() const; // std::string( begin(), end() )
   std::string_view string_view() const noexcept;  // std::string_view( begin(), size() )

   char peek_char( const std::size_t offset = 0 ) const noexcept;  // begin()[ offset ]
   std::uint8_t peek_uint8( const std::size_t offset = 0 ) const noexcept;  // similar

   pegtl::position position() const noexcept;  // Not efficient with tracking_mode::lazy.

   const ParseInput& input() const noexcept;
   const iterator_t& iterator() const noexcept;
};
```

Note that `input()` returns the input from the parsing run which will be at the position after what has just been parsed, i.e. for an action input `ai` the assertion `ai.end() == ai.input().current()` will always hold true.
Conversely `iterator()` returns a pointer or iterator to the beginning of the action input's data, i.e. where the successful match attempt to the rule the action called with the action input is attached to started.

More importantly the `action_input` does **not** own the data it points to, it belongs to the original input used in the parsing run.
Therefore **the validity of the pointed-to data might not extend (much) beyond the call to `apply()`**!

When the original input has tracking mode `eager`, the `iterator_t` returned by `action_input::iterator()` will contain the `byte`, `line` and `column` counters corresponding to the beginning of the matched input represented by the `action_input`.

When the original input has tracking mode `lazy`, then `action_input::position()` is not efficient because it calculates the line number etc. by scanning the complete original input from the beginning

Actions often need to store and/or reference portions of the input for after the parsing run, for example when an abstract syntax tree is generated.
Some of the syntax tree nodes will contain portions of the input, for example for a variable name in a script language that needs to be stored in the syntax tree just as it occurs in the input data.

The **default safe choice** is to copy the matched portions of the input data that are passed to an action by storing a deep copy of the data as `std::string`, as obtained by the input class' `string()` member function, in the data structures built while parsing.

When the return type of an action, i.e. its `apply()`, is `bool`, it can retro-actively let the library consider the attempt to match the rule to which the action is attached a (local) failure.
For the overall parsing run, there is no difference between a rule returning `false` and an attached action returning `false`, however the action is only called when the rule returned `true`.
When an action returns `false`, the library rewinds the input to where it was when the rule to which the action was attached started its successful match.
This is unlike `match()` static member functions that have to rewind the input themselves.

## Apply0

In cases where the matched part of the input is not required, an action can implement a static member function called `apply0()` instead of `apply()`.
What changes is that `apply0()` will be called without an input as first argument, i.e. only with all the states.

```c++
template<>
struct my_action< my_rule >
{
   static void apply0( /* all the states */ )
   {
      // Called whenever matching my_rule during a parsing run
      // succeeds (and actions are not disabled). Can also return
      // bool instead of void.
   }
};
```

Using `apply0()` is never necessary, it is "only" an optimisation with minor benefits at compile time, and potentially more noteworthy benefits at run time.
We recommend implementing `apply0()` over `apply()` whenever both are viable.

Though an infrequently used feature, `apply0()` can also return `bool` instead of `void`, just like `apply()` and with the same implications.

## Inheriting

We will use an example to show how to use existing actions via inheritance.
The grammar for this example consists of a couple of simple rules.

```c++
struct plain
   : tao::pegtl::utf8::range< 0x20, 0x10FFFF > {};

struct escaped
   : tao::pegtl::one< '\'', '"', '?', '\\', 'a', 'b', 'f', 'n', 'r', 't', 'v' > {};

struct character
   : tao::pegtl::if_must_else< tao::pegtl::one< '\\' >, escaped, plain > {};

struct text
   : tao::pegtl::must< tao::pegtl::star< character >, tao::pegtl::eof > {};
```

Our goal is for a parsing run with the `text` rule to produce a copy of the input where the backslash escape sequences are replaced by the character they represent.
When the `plain` rule matches, the bytes of the matched UTF-8-encoded code-point can be appended to the result.
When the `escaped` rule matches, the bytes corresponding to the character represented by the escape sequence must be appended to the result.
This can be achieved with appropriate specialisations of `my_action` using some [contrib](Contrib-and-Examples.md#contrib) classes from `tao/pegtl/contrib/unescape.hpp`.

```c++
template<>
struct my_action< plain >
   : tao::pegtl::append_all {};

template<>
struct my_action< escaped >
   : tao::pegtl::unescape_c< escaped, '\'', '"', '?', '\\', '\a', '\b', '\f', '\n', '\r', '\t', '\v' > {};
```

For step three the [input for the parsing run](Inputs-and-Parsing.md) is set up as usual.
In addition, the actions are passed as second template parameter, and a `std::string` as second argument to `parse()`.
Here `unescaped` is the state that is required by the `append_all` and `unescape_c` actions; all additional arguments passed to `parse()` are forwarded to all actions.

```c++
std::string unescape( const std::string& escaped )
{
   std::string unescaped;
   tao::pegtl::memory_input in( result, __FUNCTION__ );
   tao::pegtl::parse< text, my_action >( in, unescaped );
   return unescaped;
}
```

At the end of the parsing run, the complete unescaped string can be found in the aptly named variable `unescaped`.

A more complete example of how to unescape strings can be found in `src/example/pegtl/unescape.cpp`.

## Specialising

The rule class for which an action class template is specialised *must* exactly match the definition of the rule in the grammar.
For example consider the following rule.

```c++
struct foo
   : tao::pegtl::plus< tao::pegtl::alpha > {};
```

Now an action class template can be specialised for `foo`, or for `tao::pegtl::alpha`, but *not* for `tao::pegtl::plus< tao::pegtl::alpha >`.

This because base classes are not taken into consideration by the C++ language when choosing a specialisation, which might be surprising when being used to pointer arguments to functions where conversions from pointer-to-derived to pointer-to-base are performed implicitly and silently.

So although the function called by the library to match `foo` is the inherited `tao::pegtl::plus< tao::pegtl::alpha >::match()`, the rule class is `foo` and the function known as `foo::match()`, wherefore an action needs to be specialised for `foo` instead of `tao::pegtl::plus< tao::pegtl::alpha >`.

While it is possible to specialise the action class template for `tao::pegtl::alpha`, it might not be a good idea since the action would be applied for *all* occurrences of `tao::pegtl::alpha` in the grammar.
To circumvent this issue a new name can be given to the `tao::pegtl::alpha`, a name that will not be "randomly" used in other places of the grammar.

```c++
struct bar
   : tao::pegtl::alpha {};

struct foo
   : tao::pegtl::plus< bar > {};
```

Now an action class template can be specialised for `foo` and `bar`, but again *not* for `tao::pegtl::plus< bar >` or `tao::pegtl::alpha`.

More precisely, it could be specialised for the latter two rules, but wouldn't ever be called unless these rules were used elsewhere in the grammar, a different kettle of fish.

Note that this is also the reason why you should **not** use type aliases instead of inheritance when defining your grammars.

## Changing Actions

The action class template can be changed, and actions enabled or disabled, in ways beyond supplying, or not, an action to `tao::pegtl::parse()` at the start of a parsing run.

### Via Rules

The [`tao::pegtl::enable<>`](Rule-Reference.md#enable-r-) and [`tao::pegtl::disable<>`](Rule-Reference.md#disable-r-) rules behave just like [`seq<>`](Rule-Reference.md#seq-r-) but, without touching the current action, enable or disable calling of actions within their sub-rules, respectively.

The [`tao::pegtl::action<>`](Rule-Reference.md#action-a-r-) rule also behaves similarly to [`seq<>`](Rule-Reference.md#seq-r-) but takes an action class template as first template parameter and, without enabling or disabling actions, uses its first template parameter as action for the sub-rules.

The following two lines effectively do the same thing, namely parse with `my_grammar` as top-level parsing rule without invoking actions (unless actions are enabled again somewhere else).

```c++
tao::pegtl::parse< my_grammar >( ... );
tao::pegtl::parse< tao::pegtl::disable< my_grammar >, my_action >( ... );
```

Similarly the following two lines both start parsing `my_grammar` with `my_action` (again only unless something changes somewhere else).

```c++
tao::pegtl::parse< my_grammar, my_action >( ... );
tao::pegtl::parse< tao::pegtl::action< my_action, my_grammar > >( ... );
```

User-defined parsing rules can use `action<>`, `enable<>` and `disable<>` just like any other combinator rules.
For example to disable actions in LISP-style comments the following rule could be used as per `src/example/pegtl/s_expression.cpp`.

```c++
struct comment
   : tao::pegtl::seq< tao::pegtl::one< '#' >, tao::pegtl::disable< cons_list > > {};
```

The ability to change the actions during a parsing run also allows using the same rules multiple times with different action class templates within a grammar.

### Via Actions

The action classes `tao::pegtl::disable_action` and `tao::pegtl::enable_action` can be used to disable and enable actions, respectively, for any rule (and its sub-rules).
For example actions can be disabled for `my_rule` in a parsing run using `my_action` as follows.

```c++
template<>
struct my_action< my_rule >
   : tao::pegtl::disable_action {};

tao::pegtl::parse< my_grammar, my_action >( ... );
```

Conversely `tao::pegtl::change_action<>` takes a new action class template as only template parameter and changes the current action in a parsing run to its template parameter.

Note that parsing proceeds with the rule to which the action changing action is attached to "as if" the new action had been the current action all along.
The new action can even perform an action change *on the same rule*, however care should be taken to not introduce infinite cycles of changes.

## Changing States

The states, too, can be changed in ways beyond supplying them, or not, to `tao::pegtl::parse()` at the start of a parsing run.

### Via Rules

The [`state` rule](Rule-Reference.md#state-s-r-) behaves similarly to [`seq`](Rule-Reference.md#seq-r-) but uses the first template parameter as type of a new object.
This new object is used replaces the current state(s) for the remainder of the implicit [`seq`](Rule-Reference.md#seq-r-).

The new object is constructed with a const-reference to the current input of the parsing run, and all previous states, if any, as arguments.
If no such constructor exists, the new object is default constructed.
If the implicit [`seq`](Rule-Reference.md#seq-r-) of the sub-rules succeeds, then, by default, a member function named `success()` is called on this "new" object, receiving the same arguments as the constructor.
At this point the input will be advanced by whatever the sub-rules have consumed in the meantime.

Please consult `include/tao/pegtl/internal/state.hpp` to see how the default behaviour on success can be changed by overriding `tao::pegtl::state<>::success()` in a derived class when using that class instead.

Embedding a state change into the grammar with [`state<>`](Rule-Reference.md#state-s-r-) is only recommended when some state is used by custom parsing rules.

### Via Actions

The actions `tao::pegtl::change_state<>` and `tao::pegtl::change_states<>` can be used to change from the current to a new set of states while parsing the rules they are attached to.

The differences are summarised in this table; note that `change_state` is more similar to the legacy `change_state` control class as included with the 2.z versions of the PEGTL.

| Feature | `change_state` | `change_states` |
| --- | --- | --- |
| Number of new states | one | any |
| Construction of new states | optionally with input and old states | default |
| Success function on action | if not on new state | required |

With `change_state` only a single new state type can be given as template parameter, and only a single new state will be created.
The constructor of the new state receives the same arguments as per `tao::pegtl::state<>`, the current input from the parsing run and all previous states.

A `success()` static member function is supplied that calls the `success()` member function on the new state, again with the current input from the parsing run and all previous states.
The supplied `success()` can of course be overridden in a derived class.

With `change_states`, being a variadic template, any number of new state types can be given and an appropriate set of new states will be created (nearly) simultaneously.
All new states are default-constructed, if something else is required the reader is encouraged to copy and modify the implementation of `change_states` in their project.

The user *must* implement a custom `success()` static member function that takes the current input from the parsing run, the new states, and the old states as arguments.

Note that, *unlike* the `tao::pegtl::state<>` combinator, the success functions are *only called when actions are currently enabled*!

Using the changing actions is again done via inheritance as shown in the following example for `change_states`.

```c++
template<>
struct my_action< my_rule >
   : tao::pegtl::change_states< new_state_1, new_state_2 >
{
   template< typename ParseInput >
   static void success( const ParseInput&, new_state_1&, new_state_2&, /* the previous states*/ )
   {
      // Do whatever with both the new and the old states...
   }
};
```

For a more complete example of how to build a generic JSON data structure with `change_state` and friends see `src/example/pegtl/json_build.cpp`.

## Changing Actions and States

The actions `change_action_and_state<>` and `change_action_and_states<>` combine `change_action` with one of the `change_state<>` or `change_states<>` actions, respectively.
For `change_action_and_state<>` and `change_action_and_states<>` the new action class template is passed as first template parameter as for `change_action`, followed by the new state(s) as given to `change_state<>` and `change_states<>`.

Note that `change_action_and_state<>` and `change_action_and_states<>` behave like `change_action<>` in that they proceed to match the rule to which the changing action is attached to "as if" the new action had been the current action all along.

## Match

Besides `apply()` and `apply0()`, an action class specialization can also have a `match()` static member function.
The default control class template `normal` will detect the presence of a suitable `match()` function and call this function instead of `tao::pegtl::match()`.

```c++
template<>
struct my_action< my_rule >
{
   template< typename Rule,
             apply_mode A,
             rewind_mode M,
             template< typename... > class Action,
             template< typename... > class Control,
             typename ParseInput,
             typename... States >
   static bool match( ParseInput& in, States&&... st )
   {
      // Call the function that would have been called otherwise,
      // in this case without changing anything...
      return tao::pegtl::match< Rule, A, M, Action, Control >( in, st... );
   }
}
```

Implementing a custom `match()` for an action is considered a rather advanced feature that is not used directly very often.
All "changing" action classes mentioned in this document are implemented as actions with `match()`.
Their implementations can be found in `<tao/pegtl/change_*.hpp>` and should be studied before implementing a custom action with `match()`.

## Nothing

Letting the primary template of an action class template derive from `tao::pegtl::nothing` is recommended, but not necessary.

When using `nothing`, some assertions are enabled that are usually very helpful while developing a parser.

When not using `nothing`, simply by never mentioning it (as base class), these assertions are disabled and it is possible for an action's `apply()` or `apply0()` implementation to be silently ignored.

In the following let `a` be an action template class, i.e. the instantiation of an action class template `action` for some rule `r`, or `using a = action< r >` for short.

We say that `apply()` is *callable* when it is the name of a static member function of `a` that returns either `void` or `bool` and can be called with an input and the current states.

We say that `apply0()` is *callable* when it is the name of a static member function of `a` that returns either `void` or `bool` and can be called with the current states.

The following assertions are always enabled.

* There must be at most one callable `apply` or `apply0()` in `a`.
* If `nothing< r >` is an accessible base class of `a` then `a` must not have a callable `apply()`.
* If `nothing< r >` is an accessible base class of `a` then `a` must not have a callable `apply0()`.
* If `require_apply` is an accessible base class of `a` then it must have a callable `apply()`.
* If `require_apply0` is an accessible base class of `a` then it must have a callable `apply0()`.

The classes `require_apply` and `require_apply0` are also explained in [the State Mismatch section](#state-mismatch).

The following assertion is only enabled when `std::is_base_of_v< tao::pegtl::nothing< void >, action< void > >` is `true`.

* Either `nothing` must be an accessible base class of `a`, or
* `maybe_nothing` must be an accessible base class of `a`, or
* `a` must have a callable `apply()` or `apply0()`.

The class `tao::pegtl::maybe_nothing` is an accessible base class of all the changing actions explained above.
This make is possible, but not necessary, to implement `apply()` or `apply0()` for actions derived from them.

Note that `maybe_nothing` can be combined, through multiple inheritance, with one of `nothing< r >`, `require_apply` or `require_apply0`.

For example when a class `b` is derived from `change_state`, it also gains that class' `maybe_nothing` as accessible base class.
At this point `b` is allowed to either have or not have an `apply()` or `apply0()`.
By letting `b` also derive from one of the three mentioned classes, the `maybe_nothing` will be ignored and `b` will be checked to have or not have the functions as dictated by the respective additional base class.

## Backtracking

Sometimes there can be *backtracking* during a parsing run which can lead to Actions being called in places where their effects are undesired.
While it might be intuitively clear what backtracking is, for the purpose of the following discussion we give a slightly more formal definition.

We speak of *backtracking* across a rule `S` when there is a rule `R` of which `S` is a (direct or indirect) sub-rule and during a parsing run
1. `R` returns local failure after
2. `S` succeeded and its success is a requirement for the success of `R` and
3. it is "still possible" for the top-level grammar rule of the parsing run to succeed.

In this case the input will have been rewound to the point at which `R` was attempted to match and all effects of `S` on the Input will have been undone, however, and this is the subject of this section, any action attached to `S` will have been already performed without there being an automatic "undo".

#### The AAC-Problem

In some cases it is easy to rewrite the grammar in a way that prevents backtracking.
This simultaneously removes the issue of having to undo actions and improves parsing performance.

The prototypical case for which such a rewrite can be done is `R = sor< seq< A, B >, seq< A, C > >` where `A`, `B` and `C` are arbitrary rules.

If during a parsing run there are actions attached to `A` and `C`, and the input matches `seq< A, C >` but not `seq< A, B >`, then the action for `A` will be called *twice* before the action for `C`, which gives this problem its "AAC" name, given that what happens is:

* Begin `sor< seq< A, B >, seq< A, C > >`
* Begin `seq< A, B >`
* Begin `A`
* Success `A` with action called
* Begin `B`
* Failure `B`
* Failure `seq< A, B >`
* Begin `seq< A, C >`
* Begin `A` at the same position as the begin `A` above
* Success `A` with action called again on the same input
* Begin `C`
* Success `C`
* Success `seq< A, C >`
* Success `sor< seq< A, B >, seq< A, C > >`

#### Rewriting

In practice the structure of the rule might be more complicated than the pure AAC-problem which will make it harder to recognise the pattern.
One solution is to rewrite `R` as `R' = seq< A, sor< B, C > >` where of course any action for `A` will be called at most once for every successful match of `R'`.

#### Manual Undo 

Another solution is to undo the effects of the Action attached to `A` in case the encompassing `seq< A, B >` (or `seq< A, C >`) fail.

The advantage of this approach is that the implementation of the Action for `A` can pretend that is only called when really needed.
The disadvantage is that there is no function on the Action that is called in the case of failure which requires the user to either write a custom `match()` function in the Action for `seq< A, B >` or to implement the `failure()` function in a custom [Control class](Control-and-Debug.md).

#### Manual Commit

A further solution is to let the Action for `A` perform its job "to the side", and only "commit" the effects to the target data structure in the Action for `seq< A, B >`.

For example if the Action attached to `A` takes the matched portion of the Input as `std::string` and appends it to `std::vector< std::string >` one could change said Action for `A` to only fill some temporary string in one of the States, and create an Action for `seq< A, B >` that, after it is called on success of that rule, appends the aforementioned temporary string to the target vector.

#### Looking Ahead

When everything else fails and a quick-and-dirty solution to Actions being called too often in the presence of backtracking is required and/or performance is not of prime importance it is relatively easy to solve the problem by employing the infinite look-ahead capability of PEGs.

When backtracking across `S` is a problem because an Action attached to `S` can be called when `S` succeeds even though there is a higher-up rule `R` that can still fail then simply replace `R` with `seq< at< R >, R >` in the grammar.

Remembering that `at` disables all Actions explains how this solves the problem; we first verify without Actions that `R` will indeed match at this point and only then match `R` again with Actions enabled.

## Troubleshooting

The following lists a couple of frequently encountered Action-related errors and how to fix them.

### Boolean Return

Actions returning `bool` are an advanced use case that should be used with caution.
They prevent some internal optimisations, in particular when used with `apply0()`.
They can also have weird effects on the semantics of a parsing run, for example `at< rule >` can succeed for the same input for which `rule` fails when there is a `bool`-action attached to `rule` that returns `false` (remember that actions are disabled within `at<>`).

### State Mismatch

When an action's `apply()` or `apply0()` expects different states than those present in the parsing run there will either be a possibly not very helpful compiler error, or it will compile without a call to the action, depending on whether `tao::pegtl::nothing<>` is used as base class of the primary action class template.

By deriving an action specialisation from either `tao::pegtl::require_apply` or `tao::pegtl::require_apply0`, as appropriate, a -- potentially more helpful -- compiler error can be provoked, so when the grammar contains `my_rule` and the action is `my_action` then silently compiling without a call to `apply0()` is no longer possible.

```c++
template<>
struct my_action< my_rule >
  : require_apply0
{
   static void apply0( double )
   {
      // ...
   }
}
```

Note that deriving from `require_apply` or `require_apply0` is optional and usually only used for troubleshooting.

## Legacy Actions

See the [section on legacy-style action rules](Rule-Reference.md#action-rules).

Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
