// Copyright 2018 vDaniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_UTILITY_BIGINT_HPP
#define JSONCONS_UTILITY_BIGINT_HPP

#include <algorithm> // std::max, std::min, std::reverse
#include <cassert> // assert
#include <climits>
#include <cmath> // std::fmod
#include <cstdint>
#include <cstring> // std::memcpy
#include <iostream>
#include <limits> // std::numeric_limits
#include <memory> // std::allocator
#include <string> // std::string
#include <type_traits> // std::enable_if
#include <vector> // std::vector

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/config/jsoncons_config.hpp>

namespace jsoncons {

/*
This implementation is based on Chapter 2 and Appendix A of
Ammeraal, L. (1996) Algorithms and Data Structures in C++,
Chichester: John Wiley.

*/

namespace detail {

    template <typename Allocator>
    class basic_bigint_base
    {
    public:
        using allocator_type = Allocator;
        using basic_type_allocator_type = typename std::allocator_traits<allocator_type>:: template rebind_alloc<uint64_t>;

    private:
        basic_type_allocator_type alloc_;
    public:
       using allocator_traits_type = std::allocator_traits<basic_type_allocator_type>;
       using stored_allocator_type = allocator_type;
       using pointer = typename allocator_traits_type::pointer;
       using value_type = typename allocator_traits_type::value_type;
       using size_type = std::size_t;
       using pointer_traits = std::pointer_traits<pointer>;

        basic_bigint_base()
            : alloc_()
        {
        }
        explicit basic_bigint_base(const allocator_type& alloc)
            : alloc_(basic_type_allocator_type(alloc))
        {
        }

        basic_type_allocator_type get_allocator() const
        {
            return alloc_;
        }
    };

} // namespace detail

template <typename Allocator = std::allocator<uint64_t>>
class basic_bigint : protected detail::basic_bigint_base<Allocator>
{
    using base_t = detail::basic_bigint_base<Allocator>;

    static constexpr uint64_t max_short_storage_size = 2;
public:

    using size_type = typename base_t::size_type;
    using value_type = typename base_t::value_type;
    using base_t::get_allocator;
    using bigint_type = basic_bigint<Allocator>;

    static constexpr uint64_t max_basic_type = (std::numeric_limits<uint64_t>::max)();
    static constexpr uint64_t basic_type_bits = sizeof(uint64_t) * 8;  // Number of bits
    static constexpr uint64_t basic_type_halfBits = basic_type_bits/2;

    static constexpr uint16_t word_length = 4; // Use multiples of word_length words
    static constexpr uint64_t r_mask = (uint64_t(1) << basic_type_halfBits) - 1;
    static constexpr uint64_t l_mask = max_basic_type - r_mask;
    static constexpr uint64_t l_bit = max_basic_type - (max_basic_type >> 1);

private:

    struct common_storage
    {
        uint8_t is_dynamic_:1; 
        uint8_t is_negative_:1; 
        size_type length_;
    };

    struct short_storage
    {
        uint8_t is_dynamic_:1; 
        uint8_t is_negative_:1; 
        size_type length_;
        uint64_t values_[max_short_storage_size];

        short_storage()
            : is_dynamic_(false), 
              is_negative_(false),
              length_(0),
              values_{0,0}
        {
        }

        template <typename T>
        short_storage(T n, 
                      typename std::enable_if<std::is_integral<T>::value &&
                                              sizeof(T) <= sizeof(int64_t) &&
                                              std::is_signed<T>::value>::type* = 0)
            : is_dynamic_(false), 
              is_negative_(n < 0),
              length_(n == 0 ? 0 : 1)
        {
            values_[0] = n < 0 ? (uint64_t(0)-static_cast<uint64_t>(n)) : static_cast<uint64_t>(n);
            values_[1] = 0;
        }

        template <typename T>
        short_storage(T n, 
                      typename std::enable_if<std::is_integral<T>::value &&
                                              sizeof(T) <= sizeof(int64_t) &&
                                              !std::is_signed<T>::value>::type* = 0)
            : is_dynamic_(false), 
              is_negative_(false),
              length_(n == 0 ? 0 : 1)
        {
            values_[0] = n;
            values_[1] = 0;
        }

        template <typename T>
        short_storage(T n, 
                      typename std::enable_if<std::is_integral<T>::value &&
                                              sizeof(int64_t) < sizeof(T) &&
                                              std::is_signed<T>::value>::type* = 0)
            : is_dynamic_(false), 
              is_negative_(n < 0),
              length_(n == 0 ? 0 : max_short_storage_size)
        {
            using unsigned_type = typename std::make_unsigned<T>::type;

            auto u = n < 0 ? (unsigned_type(0)-static_cast<unsigned_type>(n)) : static_cast<unsigned_type>(n);
            values_[0] = uint64_t(u & max_basic_type);;
            u >>= basic_type_bits;
            values_[1] = uint64_t(u & max_basic_type);;
        }

        template <typename T>
        short_storage(T n, 
                      typename std::enable_if<std::is_integral<T>::value &&
                                              sizeof(int64_t) < sizeof(T) &&
                                              !std::is_signed<T>::value>::type* = 0)
            : is_dynamic_(false), 
              is_negative_(false),
              length_(n == 0 ? 0 : max_short_storage_size)
        {
            values_[0] = uint64_t(n & max_basic_type);;
            n >>= basic_type_bits;
            values_[1] = uint64_t(n & max_basic_type);;
        }

        short_storage(const short_storage& stor)
            : is_dynamic_(false), 
              is_negative_(stor.is_negative_),
              length_(stor.length_)
        {
            values_[0] = stor.values_[0];
            values_[1] = stor.values_[1];
        }

        short_storage& operator=(const short_storage& stor) = delete;
        short_storage& operator=(short_storage&& stor) = delete;
    };

    struct dynamic_storage
    {
        using real_allocator_type = typename std::allocator_traits<Allocator>:: template rebind_alloc<uint64_t>;
        using pointer = typename std::allocator_traits<real_allocator_type>::pointer;

        uint8_t is_dynamic_:1; 
        uint8_t is_negative_:1; 
        size_type length_;
        size_type capacity_;
        pointer data_;

        dynamic_storage()
            : is_dynamic_(true), 
              is_negative_(false),
              length_(0),
              capacity_(0),
              data_(nullptr)
        {
        }

        dynamic_storage(const dynamic_storage& stor, real_allocator_type alloc)
            : is_dynamic_(true), 
              is_negative_(stor.is_negative_),
              length_(stor.length_),
              capacity_(round_up(stor.length_)),
              data_(nullptr)
        {
            data_ = std::allocator_traits<real_allocator_type>::allocate(alloc, capacity_);
            JSONCONS_TRY
            {
                std::allocator_traits<real_allocator_type>::construct(alloc, extension_traits::to_plain_pointer(data_));
            }
            JSONCONS_CATCH(...)
            {
                std::allocator_traits<real_allocator_type>::deallocate(alloc, data_, capacity_);
                JSONCONS_RETHROW;
            }
            JSONCONS_ASSERT(stor.data_ != nullptr);
            std::memcpy(data_, stor.data_, size_type(stor.length_*sizeof(uint64_t)));
        }

        dynamic_storage(dynamic_storage&& stor) noexcept
            : is_dynamic_(true), 
              is_negative_(stor.is_negative_),
              length_(stor.length_),
              capacity_(stor.capacity_),
              data_(stor.data_)
        {
            stor.length_ = 0;
            stor.capacity_ = 0;
            stor.data_ = nullptr;
        }

        void destroy(const real_allocator_type& a) noexcept
        {
            if (data_ != nullptr)
            {
                real_allocator_type alloc(a);

                std::allocator_traits<real_allocator_type>::destroy(alloc, extension_traits::to_plain_pointer(data_));
                std::allocator_traits<real_allocator_type>::deallocate(alloc, data_,capacity_);
            }
        }

        void reserve(size_type n, const real_allocator_type& a)
        {
            real_allocator_type alloc(a);

            size_type capacity_new = round_up(n);
            uint64_t* data_old = data_;
            data_ = std::allocator_traits<real_allocator_type>::allocate(alloc, capacity_new);
            if (length_ > 0)
            {
                std::memcpy( data_, data_old, size_type(length_*sizeof(uint64_t)));
            }
            if (capacity_ > 0 && data_ != nullptr)
            {
                std::allocator_traits<real_allocator_type>::deallocate(alloc, data_old, capacity_);
            }
            capacity_ = capacity_new;
        }

        // Find suitable new block size
        constexpr size_type round_up(size_type i) const noexcept 
        {
            return (i/word_length + 1) * word_length;
        }
    };

    union
    {
        common_storage common_stor_;
        short_storage short_stor_;
        dynamic_storage dynamic_stor_;
    };

public:
    basic_bigint()
    {
        ::new (&short_stor_) short_storage();
    }

    explicit basic_bigint(const Allocator& alloc)
        : base_t(alloc)
    {
        ::new (&short_stor_) short_storage();
    }


    basic_bigint(const basic_bigint<Allocator>& n)
        : base_t(n.get_allocator())
    {
        if (!n.is_dynamic())
        {
            ::new (&short_stor_) short_storage(n.short_stor_);
        }
        else
        {
            ::new (&dynamic_stor_) dynamic_storage(n.dynamic_stor_, get_allocator());
        }
    }

    basic_bigint(basic_bigint<Allocator>&& other) noexcept
        : base_t(other.get_allocator())
    {
        if (!other.is_dynamic())
        {
            ::new (&short_stor_) short_storage(other.short_stor_);
        }
        else
        {
            ::new (&dynamic_stor_) dynamic_storage(std::move(other.dynamic_stor_));
        }
    }

    template <typename Integer>
    basic_bigint(Integer n, 
                 typename std::enable_if<std::is_integral<Integer>::value>::type* = 0)
    {
        ::new (&short_stor_) short_storage(n);
    }

    ~basic_bigint() noexcept
    {
        destroy();
    }

    constexpr bool is_dynamic() const
    {
        return common_stor_.is_dynamic_;
    }

    constexpr size_type length() const
    {
        return common_stor_.length_;
    }

    constexpr size_type capacity() const
    {
        return is_dynamic() ? dynamic_stor_.capacity_ : max_short_storage_size;
    }

    bool is_negative() const
    {
        return common_stor_.is_negative_;
    }

    void is_negative(bool value) 
    {
        common_stor_.is_negative_ = value;
    }

    const uint64_t* data() const
    {
        const uint64_t* p = is_dynamic() ? dynamic_stor_.data_ : short_stor_.values_;
        JSONCONS_ASSERT(p != nullptr);
        return p;
    }

    uint64_t* data() 
    {
        uint64_t* p = is_dynamic() ? dynamic_stor_.data_ : short_stor_.values_;
        JSONCONS_ASSERT(p != nullptr);
        return p;
    }

    template <typename CharT>
    static basic_bigint<Allocator> from_string(const std::basic_string<CharT>& s)
    {
        return from_string(s.data(), s.length());
    }

    template <typename CharT>
    static basic_bigint<Allocator> from_string(const CharT* s)
    {
        return from_string(s, std::char_traits<CharT>::length(s));
    }

    template <typename CharT>
    static basic_bigint<Allocator> from_string(const CharT* data, size_type length)
    {
        bool neg;
        if (*data == '-')
        {
            neg = true;
            data++;
            --length;
        }
        else
        {
            neg = false;
        }

        basic_bigint<Allocator> v = 0;
        for (size_type i = 0; i < length; i++)
        {
            CharT c = data[i];
            switch (c)
            {
                case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                    v = (v * 10) + (uint64_t)(c - '0');
                    break;
                default:
                    JSONCONS_THROW(std::runtime_error(std::string("Invalid digit ") + "\'" + (char)c + "\'"));
            }
        }

        if (neg)
        {
            v.common_stor_.is_negative_ = true;
        }

        return v;
    }

    template <typename CharT>
    static basic_bigint<Allocator> from_string_radix(const CharT* data, size_type length, uint8_t radix)
    {
        if (!(radix >= 2 && radix <= 16))
        {
            JSONCONS_THROW(std::runtime_error("Unsupported radix"));
        }

        bool neg;
        if (*data == '-')
        {
            neg = true;
            data++;
            --length;
        }
        else
        {
            neg = false;
        }

        basic_bigint<Allocator> v = 0;
        for (size_type i = 0; i < length; i++)
        {
            CharT c = data[i];
            uint64_t d;
            switch (c)
            {
                case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                    d = (uint64_t)(c - '0');
                    break;
                case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':
                    d = (uint64_t)(c - ('a' - 10));
                    break;
                case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                    d = (uint64_t)(c - ('A' - 10));
                    break;
                default:
                    JSONCONS_THROW(std::runtime_error(std::string("Invalid digit in radix ") + std::to_string(radix) + ": \'" + (char)c + "\'"));
            }
            if (d >= radix)
            {
                JSONCONS_THROW(std::runtime_error(std::string("Invalid digit in radix ") + std::to_string(radix) + ": \'" + (char)c + "\'"));
            }
            v = (v * radix) + d;
        }

        if ( neg )
        {
            v.common_stor_.is_negative_ = true;
        }
        return v;
    }

    static basic_bigint from_bytes_be(int signum, const uint8_t* str, std::size_t n)
    {
        static const double radix_log2 = std::log2(next_power_of_two(256));
        // Estimate how big the result will be, so we can pre-allocate it.
        double bits = radix_log2 * n;
        double big_digits = std::ceil(bits / 64.0);
        //std::cout << "ESTIMATED: " << big_digits << "\n";

        bigint_type v = 0;
        v.reserve(static_cast<std::size_t>(big_digits));

        if (n > 0)
        {
            for (std::size_t i = 0; i < n; i++)
            {
                v = (v * 256) + (uint64_t)(str[i]);
            }
        }
        //std::cout << "ACTUAL: " << v.length() << "\n";

        if (signum < 0)
        {
            v.common_stor_.is_negative_ = true;
        }

        return v;
    }

    uint64_t* begin() { return is_dynamic() ? dynamic_stor_.data_ : short_stor_.values_; }
    const uint64_t* begin() const { return is_dynamic() ? dynamic_stor_.data_ : short_stor_.values_; }
    uint64_t* end() { return begin() + length(); }
    const uint64_t* end() const { return begin() + length(); }

    void resize(size_type new_length)
    {
        size_type old_length = common_stor_.length_;
        reserve(new_length);
        common_stor_.length_ = new_length;

        if (old_length < new_length)
        {
            if (is_dynamic())
            {
                std::memset(dynamic_stor_.data_+old_length, 0, size_type((new_length-old_length)*sizeof(uint64_t)));
            }
            else
            {
                JSONCONS_ASSERT(new_length <= max_short_storage_size);
                for (size_type i = old_length; i < max_short_storage_size; ++i)
                {
                    short_stor_.values_[i] = 0;
                }
            }
        }
    }

    void reserve(size_type n)
    {
       if (capacity() < n)
       {
           if (!is_dynamic())
           {
               size_type size = short_stor_.length_;
               size_type is_neg = short_stor_.is_negative_;
               uint64_t values[max_short_storage_size] = {short_stor_.values_[0], short_stor_.values_[1]};

               ::new (&dynamic_stor_) dynamic_storage();
               dynamic_stor_.reserve(n, get_allocator());
               dynamic_stor_.length_ = size;
               dynamic_stor_.is_negative_ = is_neg;
               dynamic_stor_.data_[0] = values[0];
               dynamic_stor_.data_[1] = values[1];
           }
           else
           {
               dynamic_stor_.reserve(n, get_allocator());
           }
       }
    }

    // operators

    bool operator!() const
    {
        return length() == 0 ? true : false;
    }

    basic_bigint operator-() const
    {
        basic_bigint<Allocator> v(*this);
        v.common_stor_.is_negative_ = !v.is_negative();
        return v;
    }

    basic_bigint& operator=( const basic_bigint<Allocator>& y )
    {
        if ( this != &y )
        {
            resize( y.length() );
            common_stor_.is_negative_ = y.is_negative();
            if ( y.length() > 0 )
            {
                std::memcpy( data(), y.data(), size_type(y.length()*sizeof(uint64_t)) );
            }
        }
        return *this;
    }

    basic_bigint& operator+=( const basic_bigint<Allocator>& y )
    {
        const uint64_t* y_data = y.data();
        
        if ( is_negative() != y.is_negative() )
            return *this -= -y;
        uint64_t d;
        uint64_t carry = 0;

        resize( (std::max)(y.length(), length()) + 1 );
        uint64_t* this_data = data();

        for (size_type i = 0; i < length(); i++ )
        {
            if ( i >= y.length() && carry == 0 )
                break;
            d = this_data[i] + carry;
            carry = d < carry;
            if ( i < y.length() )
            {
                this_data[i] = d + y_data[i];
                if ( this_data[i] < d )
                    carry = 1;
            }
            else
                this_data[i] = d;
        }
        reduce();
        return *this;
    }

    basic_bigint& operator-=( const basic_bigint<Allocator>& y )
    {
        const uint64_t* y_data = y.data();

        if ( is_negative() != y.is_negative() )
            return *this += -y;
        if ( (!is_negative() && y > *this) || (is_negative() && y < *this) )
            return *this = -(y - *this);
        uint64_t borrow = 0;
        uint64_t d;
        for (size_type i = 0; i < length(); i++ )
        {
            if ( i >= y.length() && borrow == 0 )
                break;
            d = data()[i] - borrow;
            borrow = d > data()[i];
            if ( i < y.length())
            {
                data()[i] = d - y_data[i];
                if ( data()[i] > d )
                    borrow = 1;
            }
            else 
                data()[i] = d;
        }
        reduce();
        return *this;
    }

    basic_bigint& operator*=( int64_t y )
    {
        *this *= uint64_t(y < 0 ? -y : y);
        if ( y < 0 )
            common_stor_.is_negative_ = !is_negative();
        return *this;
    }

    basic_bigint& operator*=( uint64_t y )
    {
        size_type len0 = length();
        uint64_t hi;
        uint64_t lo;
        uint64_t dig = data()[0];
        uint64_t carry = 0;

        resize( length() + 1 );
        uint64_t* this_data = data();

        size_type i = 0;
        for (i = 0; i < len0; i++ )
        {
            DDproduct( dig, y, hi, lo );
            this_data[i] = lo + carry;
            dig = this_data[i+1];
            carry = hi + (this_data[i] < lo);
        }
        this_data[i] = carry;
        reduce();
        return *this;
    }

    basic_bigint& operator*=(const basic_bigint<Allocator>& y)
    {
        const uint64_t* y_data = y.data();

        if ( length() == 0 || y.length() == 0 )
                    return *this = 0;
        bool difSigns = is_negative() != y.is_negative();
        if ( length() + y.length() == max_short_storage_size ) // length() = y.length() = 1
        {
            uint64_t a = data()[0], b = y_data[0];
            data()[0] = a * b;
            if ( data()[0] / a != b )
            {
                resize( max_short_storage_size );
                DDproduct( a, b, data()[1], data()[0] );
            }
            common_stor_.is_negative_ = difSigns;
            return *this;
        }
        if ( length() == 1 )  //  && y.length() > 1
        {
            uint64_t digit = data()[0];
            *this = y;
            *this *= digit;
        }
        else
        {
            if ( y.length() == 1 )
                *this *= y_data[0];
            else
            {
                size_type lenProd = length() + y.length(), jA, jB;
                uint64_t sumHi = 0, sumLo, hi, lo,
                sumLo_old, sumHi_old, carry=0;
                basic_bigint<Allocator> x = *this;
                const uint64_t* x_data = x.data();
                resize( lenProd ); // Give *this length lenProd
                uint64_t* this_data = data();

                for (size_type i = 0; i < lenProd; i++ )
                {
                    sumLo = sumHi;
                    sumHi = carry;
                    carry = 0;
                    for ( jA=0; jA < x.length(); jA++ )
                    {
                        jB = i - jA;
                        if ( jB >= 0 && jB < y.length() )
                        {
                            DDproduct( x_data[jA], y_data[jB], hi, lo );
                            sumLo_old = sumLo;
                            sumHi_old = sumHi;
                            sumLo += lo;
                            if ( sumLo < sumLo_old )
                                sumHi++;
                            sumHi += hi;
                            carry += (sumHi < sumHi_old);
                        }
                    }
                    this_data[i] = sumLo;
                }
            }
        }
       reduce();
       common_stor_.is_negative_ = difSigns;
       return *this;
    }

    basic_bigint& operator/=( const basic_bigint<Allocator>& divisor )
    {
        basic_bigint<Allocator> r;
        divide( divisor, *this, r, false );
        return *this;
    }

    basic_bigint& operator%=( const basic_bigint<Allocator>& divisor )
    {
        basic_bigint<Allocator> q;
        divide( divisor, q, *this, true );
        return *this;
    }

    basic_bigint& operator<<=( uint64_t k )
    {
        size_type q = size_type(k / basic_type_bits);
        if ( q ) // Increase common_stor_.length_ by q:
        {
            resize(length() + q);
            uint64_t* this_data = data();
            for (size_type i = length(); i-- > 0; )
                this_data[i] = ( i < q ? 0 : this_data[i - q]);
            k %= basic_type_bits;
        }
        if ( k )  // 0 < k < basic_type_bits:
        {
            uint64_t k1 = basic_type_bits - k;
            uint64_t mask = (uint64_t(1) << k) - uint64_t(1);
            resize( length() + 1 );
            uint64_t* this_data = data();
            for (size_type i = length(); i-- > 0; )
            {
                this_data[i] <<= k;
                if ( i > 0 )
                    this_data[i] |= (this_data[i-1] >> k1) & mask;
            }
        }
        reduce();
        return *this;
    }

    basic_bigint& operator>>=(uint64_t k)
    {
        size_type q = size_type(k / basic_type_bits);
        if ( q >= length() )
        {
            resize( 0 );
            return *this;
        }
        if (q > 0)
        {
            memmove( data(), data()+q, size_type((length() - q)*sizeof(uint64_t)) );
            resize( size_type(length() - q) );
            k %= basic_type_bits;
            if ( k == 0 )
            {
                reduce();
                return *this;
            }
        }

        uint64_t* this_data = data();
        size_type n = size_type(length() - 1);
        int64_t k1 = basic_type_bits - k;
        uint64_t mask = (uint64_t(1) << k) - 1;
        for (size_type i = 0; i <= n; i++)
        {
            this_data[i] >>= k;
            if ( i < n )
                this_data[i] |= ((this_data[i+1] & mask) << k1);
        }
        reduce();
        return *this;
    }

    basic_bigint& operator++()
    {
        *this += 1;
        return *this;
    }

    basic_bigint<Allocator> operator++(int)
    {
        basic_bigint<Allocator> old = *this;
        ++(*this);
        return old;
    }

    basic_bigint<Allocator>& operator--()
    {
        *this -= 1;
        return *this;
    }

    basic_bigint<Allocator> operator--(int)
    {
        basic_bigint<Allocator> old = *this;
        --(*this);
        return old;
    }

    basic_bigint& operator|=( const basic_bigint<Allocator>& a )
    {
        if ( length() < a.length() )
        {
            resize( a.length() );
        }

        const uint64_t* qBegin = a.begin();
        const uint64_t* q =      a.end() - 1;
        uint64_t*       p =      begin() + a.length() - 1;

        while ( q >= qBegin )
        {
            *p-- |= *q--;
        }

        reduce();

        return *this;
    }

    basic_bigint& operator^=( const basic_bigint<Allocator>& a )
    {
        if ( length() < a.length() )
        {
            resize( a.length() );
        }

        const uint64_t* qBegin = a.begin();
        const uint64_t* q = a.end() - 1;
        uint64_t* p = begin() + a.length() - 1;

        while ( q >= qBegin )
        {
            *p-- ^= *q--;
        }

        reduce();

        return *this;
    }

    basic_bigint& operator&=( const basic_bigint<Allocator>& a )
    {
        size_type old_length = length();

        resize( (std::min)( length(), a.length() ) );

        const uint64_t* pBegin = begin();
        uint64_t* p = end() - 1;
        const uint64_t* q = a.begin() + length() - 1;

        while ( p >= pBegin )
        {
            *p-- &= *q--;
        }

        const size_type new_length = length();
        if ( old_length > new_length )
        {
            if (is_dynamic())
            {
                std::memset( dynamic_stor_.data_ + new_length, 0, size_type(old_length - new_length*sizeof(uint64_t)) );
            }
            else
            {
                JSONCONS_ASSERT(new_length <= max_short_storage_size);
                for (size_type i = new_length; i < max_short_storage_size; ++i)
                {
                    short_stor_.values_[i] = 0;
                }
            }
        }

        reduce();

        return *this;
    }

    explicit operator bool() const
    {
       return length() != 0 ? true : false;
    }

    explicit operator int64_t() const
    {
       int64_t x = 0;
       if ( length() > 0 )
       {
           x = data() [0];
       }

       return is_negative() ? -x : x;
    }

    explicit operator uint64_t() const
    {
       uint64_t u = 0;
       if ( length() > 0 )
       {
           u = data() [0];
       }

       return u;
    }

    explicit operator double() const
    {
        double x = 0.0;
        double factor = 1.0;
        double values = (double)max_basic_type + 1.0;

        const uint64_t* p = begin();
        const uint64_t* pEnd = end();
        while ( p < pEnd )
        {
            x += *p*factor;
            factor *= values;
            ++p;
        }

       return is_negative() ? -x : x;
    }

    explicit operator long double() const
    {
        long double x = 0.0;
        long double factor = 1.0;
        long double values = (long double)max_basic_type + 1.0;

        const uint64_t* p = begin();
        const uint64_t* pEnd = end();
        while ( p < pEnd )
        {
            x += *p*factor;
            factor *= values;
            ++p;
        }

       return is_negative() ? -x : x;
    }

    template <typename Alloc>
    void write_bytes_be(int& signum, std::vector<uint8_t,Alloc>& data) const
    {
        basic_bigint<Allocator> n(*this);
        signum = (n < 0) ? -1 : (n > 0 ? 1 : 0); 

        basic_bigint<Allocator> divisor(256);

        while (n >= 256)
        {
            basic_bigint<Allocator> q;
            basic_bigint<Allocator> r;
            n.divide(divisor, q, r, true);
            n = q;
            data.push_back((uint8_t)(uint64_t)r);
        }
        if (n >= 0)
        {
            data.push_back((uint8_t)(uint64_t)n);
        }
        std::reverse(data.begin(),data.end());
    }

    std::string to_string() const
    {
        std::string s;
        write_string(s);
        return s;
    }

    template <typename Ch,typename Traits,typename Alloc>
    void write_string(std::basic_string<Ch,Traits,Alloc>& data) const
    {
        basic_bigint<Allocator> v(*this);

        std::size_t len = (v.length() * basic_type_bits / 3) + 2;
        data.reserve(len);

        static uint64_t p10 = 1;
        static uint64_t ip10 = 0;

        if ( v.length() == 0 )
        {
            data.push_back('0');
        }
        else
        {
            uint64_t r;
            if ( p10 == 1 )
            {
                while ( p10 <= (std::numeric_limits<uint64_t>::max)()/10 )
                {
                    p10 *= 10;
                    ip10++;
                }
            }                     
            // p10 is max unsigned power of 10
            basic_bigint<Allocator> R;
            basic_bigint<Allocator> LP10 = p10; // LP10 = p10 = ::pow(10, ip10)

            do
            {
                v.divide( LP10, v, R, true );
                r = (R.length() ? R.data()[0] : 0);
                for ( size_type j=0; j < ip10; j++ )
                {
                    data.push_back(char(r % 10 + '0'));
                    r /= 10;
                    if ( r + v.length() == 0 )
                        break;
                }
            } 
            while ( v.length() );
            if (is_negative())
            {
                data.push_back('-');
            }
            std::reverse(data.begin(),data.end());
        }
    }

    std::string to_string_hex() const
    {
        std::string s;
        write_string_hex(s);
        return s;
    }

    template <typename Ch,typename Traits,typename Alloc>
    void write_string_hex(std::basic_string<Ch,Traits,Alloc>& data) const
    {
        basic_bigint<Allocator> v(*this);

        std::size_t len = (v.length() * basic_bigint<Allocator>::basic_type_bits / 3) + 2;
        data.reserve(len);
        // 1/3 > ln(2)/ln(10)
        static uint64_t p10 = 1;
        static uint64_t ip10 = 0;

        if ( v.length() == 0 )
        {
            data.push_back('0');
        }
        else
        {
            uint64_t r;
            if ( p10 == 1 )
            {
                while ( p10 <= (std::numeric_limits<uint64_t>::max)()/16 )
                {
                    p10 *= 16;
                    ip10++;
                }
            }                     // p10 is max unsigned power of 16
            basic_bigint<Allocator> R;
            basic_bigint<Allocator> LP10 = p10; // LP10 = p10 = ::pow(16, ip10)
            do
            {
                v.divide( LP10, v, R, true );
                r = (R.length() ? R.data()[0] : 0);
                for ( size_type j=0; j < ip10; j++ )
                {
                    uint8_t c = r % 16;
                    data.push_back((c < 10) ? ('0' + c) : ('A' - 10 + c));
                    r /= 16;
                    if ( r + v.length() == 0 )
                        break;
                }
            } 
            while (v.length());

            if (is_negative())
            {
                data.push_back('-');
            }
            std::reverse(data.begin(),data.end());
        }
    }

//  Global Operators

    friend bool operator==( const basic_bigint<Allocator>& x, const basic_bigint<Allocator>& y ) noexcept
    {
        return x.compare(y) == 0 ? true : false;
    }

    friend bool operator==( const basic_bigint<Allocator>& x, int y ) noexcept
    {
        return x.compare(y) == 0 ? true : false;
    }

    friend bool operator!=( const basic_bigint<Allocator>& x, const basic_bigint<Allocator>& y ) noexcept
    {
        return x.compare(y) != 0 ? true : false;
    }

    friend bool operator!=( const basic_bigint<Allocator>& x, int y ) noexcept
    {
        return x.compare(basic_bigint<Allocator>(y)) != 0 ? true : false;
    }

    friend bool operator<( const basic_bigint<Allocator>& x, const basic_bigint<Allocator>& y ) noexcept
    {
       return x.compare(y) < 0 ? true : false;
    }

    friend bool operator<( const basic_bigint<Allocator>& x, int64_t y ) noexcept
    {
       return x.compare(y) < 0 ? true : false;
    }

    friend bool operator>( const basic_bigint<Allocator>& x, const basic_bigint<Allocator>& y ) noexcept
    {
        return x.compare(y) > 0 ? true : false;
    }

    friend bool operator>( const basic_bigint<Allocator>& x, int y ) noexcept
    {
        return x.compare(basic_bigint<Allocator>(y)) > 0 ? true : false;
    }

    friend bool operator<=( const basic_bigint<Allocator>& x, const basic_bigint<Allocator>& y ) noexcept
    {
        return x.compare(y) <= 0 ? true : false;
    }

    friend bool operator<=( const basic_bigint<Allocator>& x, int y ) noexcept
    {
        return x.compare(y) <= 0 ? true : false;
    }

    friend bool operator>=( const basic_bigint<Allocator>& x, const basic_bigint<Allocator>& y ) noexcept
    {
        return x.compare(y) >= 0 ? true : false;
    }

    friend bool operator>=( const basic_bigint<Allocator>& x, int y ) noexcept
    {
        return x.compare(y) >= 0 ? true : false;
    }

    friend basic_bigint<Allocator> operator+( basic_bigint<Allocator> x, const basic_bigint<Allocator>& y )
    {
        return x += y;
    }

    friend basic_bigint<Allocator> operator+( basic_bigint<Allocator> x, int64_t y )
    {
        return x += y;
    }

    friend basic_bigint<Allocator> operator-( basic_bigint<Allocator> x, const basic_bigint<Allocator>& y )
    {
        return x -= y;
    }

    friend basic_bigint<Allocator> operator-( basic_bigint<Allocator> x, int64_t y )
    {
        return x -= y;
    }

    friend basic_bigint<Allocator> operator*( int64_t x, const basic_bigint<Allocator>& y )
    {
        return basic_bigint<Allocator>(y) *= x;
    }

    friend basic_bigint<Allocator> operator*( basic_bigint<Allocator> x, const basic_bigint<Allocator>& y )
    {
        return x *= y;
    }

    friend basic_bigint<Allocator> operator*( basic_bigint<Allocator> x, int64_t y )
    {
        return x *= y;
    }

    friend basic_bigint<Allocator> operator/( basic_bigint<Allocator> x, const basic_bigint<Allocator>& y )
    {
        return x /= y;
    }

    friend basic_bigint<Allocator> operator/( basic_bigint<Allocator> x, int y )
    {
        return x /= y;
    }

    friend basic_bigint<Allocator> operator%( basic_bigint<Allocator> x, const basic_bigint<Allocator>& y )
    {
        return x %= y;
    }

    friend basic_bigint<Allocator> operator<<( basic_bigint<Allocator> u, unsigned k )
    {
        return u <<= k;
    }

    friend basic_bigint<Allocator> operator<<( basic_bigint<Allocator> u, int k )
    {
        return u <<= k;
    }

    friend basic_bigint<Allocator> operator>>( basic_bigint<Allocator> u, unsigned k )
    {
        return u >>= k;
    }

    friend basic_bigint<Allocator> operator>>( basic_bigint<Allocator> u, int k )
    {
        return u >>= k;
    }

    friend basic_bigint<Allocator> operator|( basic_bigint<Allocator> x, const basic_bigint<Allocator>& y )
    {
        return x |= y;
    }

    friend basic_bigint<Allocator> operator|( basic_bigint<Allocator> x, int y )
    {
        return x |= y;
    }

    friend basic_bigint<Allocator> operator|( basic_bigint<Allocator> x, unsigned y )
    {
        return x |= y;
    }

    friend basic_bigint<Allocator> operator^( basic_bigint<Allocator> x, const basic_bigint<Allocator>& y )
    {
        return x ^= y;
    }

    friend basic_bigint<Allocator> operator^( basic_bigint<Allocator> x, int y )
    {
        return x ^= y;
    }

    friend basic_bigint<Allocator> operator^( basic_bigint<Allocator> x, unsigned y )
    {
        return x ^= y;
    }

    friend basic_bigint<Allocator> operator&( basic_bigint<Allocator> x, const basic_bigint<Allocator>& y )
    {
        return x &= y;
    }

    friend basic_bigint<Allocator> operator&( basic_bigint<Allocator> x, int y )
    {
        return x &= y;
    }

    friend basic_bigint<Allocator> operator&( basic_bigint<Allocator> x, unsigned y )
    {
        return x &= y;
    }

    friend basic_bigint<Allocator> abs( const basic_bigint<Allocator>& a )
    {
        if ( a.is_negative() )
        {
            return -a;
        }
        return a;
    }

    friend basic_bigint<Allocator> power( basic_bigint<Allocator> x, unsigned n )
    {
        basic_bigint<Allocator> y = 1;

        while ( n )
        {
            if ( n & 1 )
            {
                y *= x;
            }
            x *= x;
            n >>= 1;
        }

        return y;
    }

    friend basic_bigint<Allocator> sqrt( const basic_bigint<Allocator>& a )
    {
        basic_bigint<Allocator> x = a;
        basic_bigint<Allocator> b = a;
        basic_bigint<Allocator> q;

        b <<= 1;
        while ( (void)(b >>= 2), b > 0 )
        {
            x >>= 1;
        }
        while ( x > (q = a/x) + 1 || x < q - 1 )
        {
            x += q;
            x >>= 1;
        }
        return x < q ? x : q;
    }

    template <typename CharT>
    friend std::basic_ostream<CharT>& operator<<(std::basic_ostream<CharT>& os, const basic_bigint<Allocator>& v)
    {
        std::basic_string<CharT> s;
        v.write_string(s); 
        os << s;

        return os;
    }

    int compare( const basic_bigint<Allocator>& y ) const noexcept
    {
        const uint64_t* y_data = y.data();

        if ( is_negative() != y.is_negative() )
            return y.is_negative() - is_negative();
        int code = 0;
        if ( length() == 0 && y.length() == 0 )
            code = 0;
        else if ( length() < y.length() )
            code = -1;
        else if ( length() > y.length() )
            code = +1;
        else
        {
            for (size_type i = length(); i-- > 0; )
            {
                if (data()[i] > y_data[i])
                {
                    code = 1;
                    break;
                }
                else if (data()[i] < y_data[i])
                {
                    code = -1;
                    break;
                }
            }
        }
        return is_negative() ? -code : code;
    }

    void divide( basic_bigint<Allocator> denom, basic_bigint<Allocator>& quot, basic_bigint<Allocator>& rem, bool remDesired ) const
    {
        if ( denom.length() == 0 )
        {
            JSONCONS_THROW(std::runtime_error( "Zero divide." ));
        }
        bool quot_neg = is_negative() ^ denom.is_negative();
        bool rem_neg = is_negative();
        int x = 0;
        basic_bigint<Allocator> num = *this;
        num.common_stor_.is_negative_ = denom.common_stor_.is_negative_ = false;
        if ( num < denom )
        {
            quot = uint64_t(0);
            rem = num;
            rem.common_stor_.is_negative_ = rem_neg;
            return;
        }
        if ( denom.length() == 1 && num.length() == 1 )
        {
            quot = uint64_t( num.data()[0]/denom.data()[0] );
            rem = uint64_t( num.data()[0]%denom.data()[0] );
            quot.common_stor_.is_negative_ = quot_neg;
            rem.common_stor_.is_negative_ = rem_neg;
            return;
        }
        else if (denom.length() == 1 && (denom.data()[0] & l_mask) == 0 )
        {
            // Denominator fits into a half word
            uint64_t divisor = denom.data()[0], dHi = 0,
                     q1, r, q2, dividend;
            quot.resize(length());
            for (size_type i=length(); i-- > 0; )
            {
                dividend = (dHi << basic_type_halfBits) | (data()[i] >> basic_type_halfBits);
                q1 = dividend/divisor;
                r = dividend % divisor;
                dividend = (r << basic_type_halfBits) | (data()[i] & r_mask);
                q2 = dividend/divisor;
                dHi = dividend % divisor;
                quot.data()[i] = (q1 << basic_type_halfBits) | q2;
            }
            quot.reduce();
            rem = dHi;
            quot.common_stor_.is_negative_ = quot_neg;
            rem.common_stor_.is_negative_ = rem_neg;
            return;
        }
        basic_bigint<Allocator> num0 = num, denom0 = denom;
        int second_done = normalize(denom, num, x);
        size_type l = denom.length() - 1;
        size_type n = num.length() - 1;
        quot.resize(n - l);
        for (size_type i=quot.length(); i-- > 0; )
            quot.data()[i] = 0;
        rem = num;
        if ( rem.data()[n] >= denom.data()[l] )
        {
            rem.resize(rem.length() + 1);
            n++;
            quot.resize(quot.length() + 1);
        }
        uint64_t d = denom.data()[l];
        for ( size_type k = n; k > l; k-- )
        {
            uint64_t q = DDquotient(rem.data()[k], rem.data()[k-1], d);
            subtractmul( rem.data() + k - l - 1, denom.data(), l + 1, q );
            quot.data()[k - l - 1] = q;
        }
        quot.reduce();
        quot.common_stor_.is_negative_ = quot_neg;
        if ( remDesired )
        {
            unnormalize(rem, x, second_done);
            rem.common_stor_.is_negative_ = rem_neg;
        }
    }
private:
    void destroy() noexcept
    {
        if (is_dynamic())
        {
            dynamic_stor_.destroy(get_allocator());
        }
    }
    void DDproduct( uint64_t A, uint64_t B,
                    uint64_t& hi, uint64_t& lo ) const
    // Multiplying two digits: (hi, lo) = A * B
    {
        uint64_t hiA = A >> basic_type_halfBits, loA = A & r_mask,
                   hiB = B >> basic_type_halfBits, loB = B & r_mask,
                   mid1, mid2, old;

        lo = loA * loB;
        hi = hiA * hiB;
        mid1 = loA * hiB;
        mid2 = hiA * loB;
        old = lo;
        lo += mid1 << basic_type_halfBits;
            hi += (lo < old) + (mid1 >> basic_type_halfBits);
        old = lo;
        lo += mid2 << basic_type_halfBits;
            hi += (lo < old) + (mid2 >> basic_type_halfBits);
    }

    uint64_t DDquotient( uint64_t A, uint64_t B, uint64_t d ) const
    // Divide double word (A, B) by d. Quotient = (qHi, qLo)
    {
        uint64_t left, middle, right, qHi, qLo, x, dLo1,
                   dHi = d >> basic_type_halfBits, dLo = d & r_mask;
        qHi = A/(dHi + 1);
        // This initial guess of qHi may be too small.
        middle = qHi * dLo;
        left = qHi * dHi;
        x = B - (middle << basic_type_halfBits);
        A -= (middle >> basic_type_halfBits) + left + (x > B);
        B = x;
        dLo1 = dLo << basic_type_halfBits;
        // Increase qHi if necessary:
        while ( A > dHi || (A == dHi && B >= dLo1) )
        {
            x = B - dLo1;
            A -= dHi + (x > B);
            B = x;
            qHi++;
        }
        qLo = ((A << basic_type_halfBits) | (B >> basic_type_halfBits))/(dHi + 1);
        // This initial guess of qLo may be too small.
        right = qLo * dLo;
        middle = qLo * dHi;
        x = B - right;
        A -= (x > B);
        B = x;
        x = B - (middle << basic_type_halfBits);
            A -= (middle >> basic_type_halfBits) + (x > B);
        B = x;
        // Increase qLo if necessary:
        while ( A || B >= d )
        {
            x = B - d;
            A -= (x > B);
            B = x;
            qLo++;
        }
        return (qHi << basic_type_halfBits) + qLo;
    }

    void subtractmul( uint64_t* a, uint64_t* b, size_type n, uint64_t& q ) const
    // a -= q * b: b in n positions; correct q if necessary
    {
        uint64_t hi, lo, d, carry = 0;
        size_type i;
        for ( i = 0; i < n; i++ )
        {
            DDproduct( b[i], q, hi, lo );
            d = a[i];
            a[i] -= lo;
            if ( a[i] > d )
                carry++;
            d = a[i + 1];
            a[i + 1] -= hi + carry;
            carry = a[i + 1] > d;
        }
        if ( carry ) // q was too large
        {
            q--;
            carry = 0;
            for ( i = 0; i < n; i++ )
            {
                d = a[i] + carry;
                carry = d < carry;
                a[i] = d + b[i];
                if ( a[i] < d )
                    carry = 1;
            }
            a[n] = 0;
        }
    }

    int normalize( basic_bigint<Allocator>& denom, basic_bigint<Allocator>& num, int& x ) const
    {
        size_type r = denom.length() - 1;
        uint64_t y = denom.data()[r];

        x = 0;
        while ( (y & l_bit) == 0 )
        {
            y <<= 1;
            x++;
        }
        denom <<= x;
        num <<= x;
        if ( r > 0 && denom.data()[r] < denom.data()[r-1] )
        {
            denom *= max_basic_type;
                    num *= max_basic_type;
            return 1;
        }
        return 0;
    }

    void unnormalize( basic_bigint<Allocator>& rem, int x, int secondDone ) const
    {
        if ( secondDone )
        {
            rem /= max_basic_type;
        }
        if ( x > 0 )
        {
            rem >>= x;
        }
        else
        {
            rem.reduce();
        }
    }

    size_type round_up(size_type i) const // Find suitable new block size
    {
        return (i/word_length + 1) * word_length;
    }

    void reduce()
    {
        uint64_t* p = end() - 1;
        uint64_t* pBegin = begin();
        while ( p >= pBegin )
        {
            if ( *p )
            {
                break;
            }
            --common_stor_.length_;
            --p;
        }
        if ( length() == 0 )
        {
            common_stor_.is_negative_ = false;
        }
    }
 
    static uint64_t next_power_of_two(uint64_t n) {
        n = n - 1;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }
};

using bigint = basic_bigint<std::allocator<uint8_t>>;

} // namespace jsoncons

#endif // JSONCONS_UTILITY_BIGINT_HPP
