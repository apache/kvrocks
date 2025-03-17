### jsoncons::bigint

```cpp
#include <jsoncons/utility/bigint.hpp>

typedef basic_bigint<Allocator = std::allocator<uint8_t>> bigint;
```
The `bigint` class is an instantiation of the `basic_bigint` class template that uses `std::allocator<uint8_t>` as the allocator type.

An arbitrary-precision integer.

#### Constructor

    bigint();

    explicit bigint(const Allocator& alloc);

    explicit bigint(const char* str);
Constructs a bigint from the decimal string representation of a bigint. 

    explicit bigint(const char* data, std::size_t length);
Constructs a bigint from the decimal string representation of a bigint. 

    explicit bigint(const char* str, const Allocator& alloc);

    bigint(int signum, std::initializer_list<uint8_t> magnitude);
Constructs a bigint from the sign-magnitude representation. 
The magnitude is an unsigned integer `n` encoded as a byte string data item in big-endian byte-order.
If the value of signum is 1, the value of the bigint is `n`. 
If the value of signum is -1, the value of the bigint is `-1 - n`. 
An empty list means a zero value.

    bigint(int signum, std::initializer_list<uint8_t> magnitude, const Allocator& alloc);

    bigint(const bigint& s); 

    bigint(bigint&& s); 

#### Assignment

    bigint& operator=(const bigint& s);

    bigint& operator=(bigint&& s);

#### Accessors

    template <typename Ch,typename Traits,typename Alloc>
    void dump(std::basic_string<Ch,Traits,Alloc>& data) const

    template <typename Alloc>
    void dump(int& signum, std::vector<uint8_t,Alloc>& data) const

#### Arithmetic operators

    explicit operator bool() const

    explicit operator int64_t() const

    explicit operator uint64_t() const

    explicit operator double() const

    explicit operator long double() const

    bigint operator-() const

    bigint& operator+=( const bigint& y )

    bigint& operator-=( const bigint& y )

    bigint& operator*=( int64_t y )

    bigint& operator*=( uint64_t y )

    bigint& operator*=( bigint y )

    bigint& operator/=( const bigint& divisor )

    bigint& operator%=( const bigint& divisor )

    bigint& operator<<=( uint64_t k )

    bigint& operator>>=(uint64_t k)

    bigint& operator++()

    bigint operator++(int)

    bigint& operator--()

    bigint operator--(int)

    bigint& operator|=( const bigint& a )

    bigint& operator^=( const bigint& a )

    bigint& operator&=( const bigint& a )

#### Non-member functions

    bool operator==(const bigint& lhs, const bigint& rhs);

    bool operator!=(const bigint& lhs, const bigint& rhs);

    template <typename CharT>
    friend std::basic_ostream<CharT>& operator<<(std::basic_ostream<CharT>& os, const bigint& o);

#### Global arithmetic operators

    bool operator==( const bigint& x, const bigint& y )

    bool operator==( const bigint& x, int y )

    bool operator!=( const bigint& x, const bigint& y )

    bool operator!=( const bigint& x, int y )

    bool operator<( const bigint& x, const bigint& y )

    bool operator<( const bigint& x, int64_t y )

    bool operator>( const bigint& x, const bigint& y )

    bool operator>( const bigint& x, int y )

    bool operator<=( const bigint& x, const bigint& y )

    bool operator<=( const bigint& x, int y )

    bool operator>=( const bigint& x, const bigint& y )

    bool operator>=( const bigint& x, int y )

    bigint operator+( bigint x, const bigint& y )

    bigint operator+( bigint x, int64_t y )

    bigint operator-( bigint x, const bigint& y )

    bigint operator-( bigint x, int64_t y )

    bigint operator*( int64_t x, const bigint& y )

    bigint operator*( bigint x, const bigint& y )

    bigint operator*( bigint x, int64_t y )

    bigint operator/( bigint x, const bigint& y )

    bigint operator/( bigint x, int y )

    bigint operator%( bigint x, const bigint& y )

    bigint operator<<( bigint u, unsigned k )

    bigint operator<<( bigint u, int k )

    bigint operator>>( bigint u, unsigned k )

    bigint operator>>( bigint u, int k )

    bigint operator|( bigint x, const bigint& y )

    bigint operator|( bigint x, int y )

    bigint operator|( bigint x, unsigned y )

    bigint operator^( bigint x, const bigint& y )

    bigint operator^( bigint x, int y )

    bigint operator^( bigint x, unsigned y )

    bigint operator&( bigint x, const bigint& y )

    bigint operator&( bigint x, int y )

    bigint operator&( bigint x, unsigned y )

    bigint abs( const bigint& a )

    bigint power( bigint x, unsigned n )

    bigint sqrt( const bigint& a )

### Examples


### Examples

#### Initializing with bigint

```cpp
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    std::string s = "-18446744073709551617";

    json j(bigint(s.c_str()));

    std::cout << "(1) " << j.as<bigint>() << "\n\n";

    std::cout << "(2) " << j.as<std::string>() << "\n\n";

    std::cout << "(3) ";
    j.dump(std::cout);
    std::cout << "\n\n";

    std::cout << "(4) ";
    auto options1 = json_options{}
        .bigint_format(bignum_format_kind::raw);
    j.dump(std::cout, options1);
    std::cout << "\n\n";

    std::cout << "(5) ";
    auto options2 = json_options{}
        .bigint_format(bignum_format_kind::base64url);
    j.dump(std::cout, options2);
    std::cout << "\n\n";
}
```
Output:
```
(1) -18446744073709551617

(2) -18446744073709551617

(3) "-18446744073709551617"

(4) -18446744073709551617

(5) "~AQAAAAAAAAAB"
```

#### Integer overflow during parsing

```cpp
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    std::string s = "-18446744073709551617";

    json j = json::parse(s);

    std::cout << "(1) ";
    j.dump(std::cout);
    std::cout << "\n\n";

    std::cout << "(2) ";
    auto options1 = json_options{}
        .bigint_format(bignum_format_kind::raw);
    j.dump(std::cout, options1);
    std::cout << "\n\n";

    std::cout << "(3) ";
    auto options2 = json_options{}
        .bigint_format(bignum_format_kind::base64url);
    j.dump(std::cout, options2);
    std::cout << "\n\n";
}
```
Output:
```
(1) -18446744073709551617

(2) -18446744073709551617

(3) "~AQAAAAAAAAAB"
```

