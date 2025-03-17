Acknowledgements
----------------

In range-v3, I have integrated many ideas that come from other people. I would be remiss to not mention them. Many others helped either directly or indirectly in a variety of ways. In no particular order...

| Contributor                 | Contribution |
|-----------------------------|------|
| Jeremy Siek                 | Container algorithms (in Boost pre-history), Boost.Iterators |
| Thorston Ottoson            | Boost.Range v1 |
| Neil Groves                 | Boost.Range v2 |
| David Abrahams, Thomas Witt | Boost.Iterators, Sentinels |
| Sean Parent                 | ASL, Projections, View / Range distinction, much Generic Program wisdom besides |
| Dietmar Kühl                | Array Traits, Property Map |
| Andrew Sutton               | C++ Concepts "Lite", Origin Libraries, Palo Alto Report |
| Doug Gregor                 | C++0x Concepts |
| Casey Carter                | Co-author and Editor, Ranges TS; major code contributions |
| Gonzalo Brito Gadeschi      | Many ideas, bug reports, and code contributions |
| Alexander Stepanov          | STL, Generic Programming, Iterators, Elements of Programming, etc. |
| Bjarne Stroustrup           | A tireless effort to add proper support for Generic Programming to C++, early support for my Ranges proposal |
| Herb Sutter                 | Early support for my Ranges proposal |
| The Standard C++ Foundation | A generous grant supporting my Ranges work |

An Abreviated History
--------------------

**Range v1**

I came to Boost in the early 2000's. By that time, Boost already had a Range library (Thorston Ottoson's). At this time, Boost.Range was little more that the `begin` and `end` free functions, and range-based overloads of the STL algorithms that dispatched to the iterator-based overloads in namespace `std`.

Boost also already had the Iterators library by David Abrahams and Jeremy Siek. This library had iterator adaptors like `filter_iterator` and `transform_iterator`.

**Range v2**

It seemed natural to me that the Range library and the adaptors from the Iterators library should be combined. I wrote the `filter` and `transform` range adaptors, commandeered the pipe operator (`|`) from bash for chaining, and put a rough library together called Range_ex in the Boost File Vault (which would later become the Boost Sandbox). I saw problems with my design and never finished it.

A few years later, Neil Groves picked up some of the ideas in my Range\_ex, polished them a great deal, published his own Range\_ex library, and submitted it to Boost. It became Boost.Range v2. At the time of writing (March, 2017), it is the version still shipping with Boost.

**Range v3**

In 2013, I published a blog post called ["Out Parameters, Move Semantics, and Stateful Algorithms"](http://ericniebler.com/2013/10/13/out-parameters-vs-move-semantics/) that turned my mind to ranges once again. Following that, it became clear to me that the Boost.Range library, designed for C++98, needed a facelift for the post-C++11 world. I began what I believed at the time would be a quick hack to bring Boost.Range into the modern world. I called it "Range v3", thinking it would become the third major version of the Boost.Range library. Subsequent posts detailed the evolution of my thinking as range-v3 took shape.

**Standardization**

Around this time, some big thinkers in the C++ community were working to resurrect the effort to add Concepts to C++. They published a paper ([N3351](http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2012/n3351.pdf)) that would become known as the **"Palo Alto Report"** which detailed the necessary and sufficient language and library support for a concept-checked version of the Standard Template Library. The authors of the paper included Alexander Stepanov, Bjarne Stroustrup, Sean Parent, Andrew Sutton, and more. Andrew Sutton began working in earnest to realize the core language changes, an effort that became known as "Concepts Lite". It became the Concepts TS and ultimately C++20's `concept` language feature.

I realized early on that Concepts Lite, or something like it, would become part of Standard C++. Recognizing that C++ would need a concept-ified Standard Library to go with the language feature, I began evolving range-v3 in that direction, eventually submitting ["Ranges for the Standard Library, Revision 1"](http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2014/n4128.html) to the C++ Standardization Committee, together with Andrew Sutton and Sean Parent. The Committee approved the direction in late 2014, and so it goes...

As of today (2022-06), C++20 has shipped with modest ranges support in the `std::ranges` namespace, with *much* more to come in C++23 and beyond. Stay tuned.
