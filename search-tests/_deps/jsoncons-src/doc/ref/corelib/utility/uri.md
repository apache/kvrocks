### jsoncons::uri

```cpp
#include <jsoncons/utility/uri.hpp>

```
The class `uri` represents a Uniform Resource Identifier (URI) reference.

#### Constructor

    uri();

    explicit uri(jsoncons::string_view str);
Constructs a `uri` by parsing the given string.

    uri(jsoncons::string_view scheme,
        jsoncons::string_view userinfo,
        jsoncons::string_view host,
        jsoncons::string_view port,
        jsoncons::string_view path,
        jsoncons::string_view query = "",
        jsoncons::string_view fragment = "")
Constructs a `uri` from the given parts. It is assumed that the parts
are not already %-encoded, encoding is performed during construction.
 
    uri(const uri& other, uri_fragment_part_t, jsoncons::string_view fragment);
Constructs a `uri` from `other`, replacing it's fragment part with `fragment`.
It is assumed that `fragment` is not already %-encoded, encoding is performed 
during construction.

    uri(const uri& other);
Copy constructor.

    uri(uri&& other) noexcept;
Move constructor.

#### Assignment
    
    uri& operator=(const uri& other);
Copy assignment.
    
    uri& operator=(uri&& other) noexcept;
Move assignment.

#### Member functions

    jsoncons::string_view scheme() const noexcept;
Returns the scheme part of this URI. The scheme is the first part of the URI, before the `:` character.

    std::string userinfo() const;
Returns the decoded userinfo part of this URI.

    jsoncons::string_view encoded_userinfo() const noexcept;
Returns the encoded userinfo part of this URI.

    jsoncons::string_view host() const noexcept;
Returns the host part of this URI.

    jsoncons::string_view port() const noexcept;
Returns the port number of this URI.

    std::string authority() const;
Returns the decoded authority part of this URI.

    jsoncons::string_view encoded_authority() const noexcept;
Returns the encoded authority part of this URI.

    std::string path() const;
Returns the decoded path part of this URI.

    jsoncons::string_view encoded_path() const noexcept;
Returns the encoded path part of this URI.

    std::string query() const;
Returns the decoded query part of this URI.

    jsoncons::string_view encoded_query() const noexcept;
Returns the encoded query part of this URI.

    std::string fragment() const;
Returns the decoded fragment part of this URI.

    jsoncons::string_view encoded_fragment() const noexcept;
Returns the encoded fragment part of this URI.

    bool is_absolute() const noexcept;
Returns true if this URI is absolute, false if it is relative.
An absolute URI has a scheme part.

    bool is_opaque() const noexcept;
Returns true if this URI is opaque, otherwise false.
An opaque URI is an absolute URI whose scheme-specific part does not begin with a slash character ('/').

    uri base() const noexcept;
Returns the base uri. The base uri includes the scheme, userinfo, host, port, and path parts,
but not the query or fragment.     

    bool has_scheme() const noexcept;
Returns true if this URI has a scheme part, otherwise false.

    bool has_userinfo() const noexcept;
Returns true if this URI has a userinfo part, otherwise false.

    bool has_authority() const noexcept;
Returns true if this URI has an authority part, otherwise false.

    bool has_host() const noexcept;
Returns true if this URI has a host part, otherwise false.

    bool has_port() const noexcept;
Returns true if this URI has a port number, otherwise false.

    bool has_path() const noexcept;
Returns true if this URI has a path part, otherwise false.

    bool has_query() const noexcept;
Returns true if this URI has a query part, otherwise false.

    bool has_fragment() const noexcept;
Returns true if this URI has a fragment part, otherwise false.

    uri resolve(jsoncons::string_view reference) const;
Resolve `reference` as a URI relative to this URI.

    uri resolve(const uri& reference) const;
Resolve `reference` as a URI relative to this URI.

    const std::string& string() const noexcept;
Returns a URI string.

    static uri parse(jsoncons::string_view str, std::error_code& ec);
Creates a `uri` by parsing the given string. If a parse error is
encountered, returns a default constructed `uri` and sets `ec`.

    friend std::ostream& operator<<(std::ostream& os, const uri& uri_);

### Examples

#### Parts
  
```cpp
#include <jsoncons/utility/uri.hpp>

int main()
{
    jsoncons::uri uri{ "https://github.com/danielaparker/jsoncons/tree/master/doc/ref/corelib/utility/uri.md#Examples" };

    std::cout << "uri: " << uri << "\n";
    std::cout << "base: " << uri.base() << "\n";
    std::cout << "scheme: " << uri.scheme() << "\n";
    std::cout << "authority: " << uri.authority() << "\n";
    std::cout << "userinfo: " << uri.userinfo() << "\n";
    std::cout << "path: " << uri.path() << "\n";
    std::cout << "query: " << uri.query() << "\n";
    std::cout << "fragment: " << uri.fragment() << "\n";
}
```

Output:

```
uri: https://github.com/danielaparker/jsoncons/tree/master/doc/ref/corelib/utility/uri.md#Examples
base: https://github.com/danielaparker/jsoncons/tree/master/doc/ref/corelib/utility/uri.md
scheme: https
authority: github.com
userinfo:
path: /danielaparker/jsoncons/tree/master/doc/ref/corelib/utility/uri.md
query:
fragment: Examples
```

#### Resolve reference relative to a base URI
  
```cpp
#include <jsoncons/utility/uri.hpp>

int main()
{
    jsoncons::uri uri{ "https://github.com/danielaparker/jsoncons/" };

    auto uri1 = uri.resolve("tree/master/doc/ref/corelib/utility/uri.md#Examples");
    std::cout << "(1) " << uri1 << "\n";

    auto uri2 = uri1.resolve("../../../../../../blob/master/include/jsoncons/utility/uri.hpp");
    std::cout << "(2) " << uri2 << "\n";

    auto uri3 = uri2.resolve("file:///~calendar");
    std::cout << "(3) " << uri3 << "\n";
}
```

Output:

```
(1) https://github.com/danielaparker/jsoncons/tree/master/doc/ref/corelib/utility/uri.md#Examples
(2) https://github.com/danielaparker/jsoncons/blob/master/include/jsoncons/utility/uri.hpp
(3) file:///~calendar
```

