// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_COMMON_FORMAT_HPP
#define JSONCONS_EXT_JSONSCHEMA_COMMON_FORMAT_HPP

#include <cassert>
#include <cstddef>
#include <exception>
#include <functional>
#include <iostream>
#include <set>
#include <string>
#include <system_error>

#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/utility/uri.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonschema/common/validator.hpp>

#if defined(JSONCONS_HAS_STD_REGEX)
#include <regex>
#endif

namespace jsoncons {
namespace jsonschema {

    inline bool is_digit(char c)
    {
        return c >= '0' && c <= '9';
    }
    
    inline bool is_vchar(char c)
    {
        return c >= 0x21 && c <= 0x7E;        
    }
    
    inline bool is_alpha(char c)
    {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }
    
    inline
    bool is_atext( char c)
    {
        switch (c)
        {
            case '!':
            case '#':
            case '$':
            case '%':
            case '&':
            case '\'':
            case '*':
            case '+':
            case '-':
            case '/':
            case '=':
            case '?':
            case '^':
            case '_':
            case '`':
            case '{':
            case '|':
            case '}':
            case '~':
                return true;
            default:
                return is_digit(c) || is_alpha(c);
        }
    }

    inline
    bool is_dtext( char c)
    {
        return (c >= 33 && c <= 90) || (c >= 94 && c <= 126); 
    }
    
    

    //  RFC 5322, section 3.4.1
    inline
    bool validate_email_rfc5322(const std::string& s)
    {
        enum class state_t {local_part,atom,dot_atom,quoted_string,amp,domain,domain_name,domain_literal,done};

        state_t state = state_t::local_part;
        std::size_t part_length = 0;

        std::size_t length = s.size();
        for (std::size_t i = 0; i < length; ++i)
        {
            const char c = s[i];
            switch (state)
            {
                case state_t::local_part:
                {
                    if (is_atext(c))
                    {
                        state = state_t::atom;
                    }
                    else if (c == '"')
                    {
                        state = state_t::quoted_string;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::dot_atom:
                {
                    if (is_atext(c))
                    {
                        ++part_length;
                        state = state_t::atom;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::atom:
                {
                    switch (c)
                    {
                        case '@':
                            state = state_t::domain;
                            part_length = 0;
                            break;
                        case '.':
                            state = state_t::dot_atom;
                            ++part_length;
                            break;
                        default:
                            if (is_atext(c))
                            {
                                ++part_length;
                            }
                            else
                            {
                                return false;
                            }
                            break;
                    }
                    break;
                }
                case state_t::quoted_string:
                {
                    if (c == '\"')
                    {
                        state = state_t::amp;
                    }
                    else
                    {
                        ++part_length;
                    }
                    break;
                }
                case state_t::amp:
                {
                    if (c == '@')
                    {
                        state = state_t::domain;
                        part_length = 0;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::domain:
                {
                    if (c == '[')
                    {
                         state = state_t::domain_literal;
                    }
                    else if (is_digit(c) || is_alpha(c))
                    {
                        state = state_t::domain_name;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::domain_literal:
                {
                    if (c == ']')
                    {
                        state = state_t::done;
                    }
                    else if (!is_dtext(c))
                    {
                        return false;
                    }
                    break;
                }
                case state_t::domain_name:
                {
                    switch (c)
                    {
                        case '.':
                            if (part_length == 0)
                            {
                                return false;
                            }
                            part_length = 0;
                            break;
                        default:
                        {
                            if (is_digit(c) || is_alpha(c) || c == '-')
                            {
                                ++part_length;
                            }
                            else
                            {
                                return false;
                            }
                        }
                        break;
                    }
                    break;
                }
                default:
                    break;
            }
        }

        return state == state_t::domain_name || state == state_t::done;
    }

    // RFC 2673, Section 3.2

    inline
    bool validate_ipv6_rfc2373(const std::string& s)
    {
        enum class state_t{start,expect_hexdig_or_unspecified,
                              hexdig, decdig,expect_unspecified, unspecified};

        state_t state = state_t::start;

        std::size_t digit_count = 0;
        std::size_t piece_count = 0;
        std::size_t piece_count2 = 0;
        bool has_unspecified = false;
        std::size_t dec_value = 0;

        for (std::size_t i = 0; i < s.length(); ++i)
        {
            const char c = s[i];
            switch (state)
            {
                case state_t::start:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                        case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                        case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':
                            state = state_t::hexdig;
                            ++digit_count;
                            piece_count = 0;
                            break;
                        case ':':
                            if (!has_unspecified)
                            {
                                state = state_t::expect_unspecified;
                            }
                            else
                            {
                                return false;
                            }
                            break;
                        default:
                            return false;
                    }
                    break;
                }
                case state_t::expect_hexdig_or_unspecified:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            dec_value = dec_value*10 + static_cast<std::size_t>(c - '0'); // just in case this piece is followed by a dot
                            state = state_t::hexdig;
                            ++digit_count;
                            break;
                        case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                        case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':
                            state = state_t::hexdig;
                            ++digit_count;
                            break;
                        case ':':
                            if (!has_unspecified)
                            {
                                has_unspecified = true;
                                state = state_t::unspecified;
                            }
                            else
                            {
                                return false;
                            }
                            break;
                        default:
                            return false;
                    }
                    break;
                }
                case state_t::expect_unspecified:
                {
                    if (c == ':')
                    {
                        has_unspecified = true;
                        state = state_t::unspecified;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::hexdig:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                        case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                        case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':
                            ++digit_count;
                            break;
                        case ':':
                            if (digit_count <= 4)
                            {
                                ++piece_count;
                                digit_count = 0;
                                dec_value = 0;
                                state = state_t::expect_hexdig_or_unspecified;
                            }
                            else
                            {
                                return false;
                            }
                            break;
                        case '.':
                            if (piece_count == 6 || has_unspecified)
                            {
                                ++piece_count2;
                                state = state_t::decdig;
                                dec_value = 0;
                            }
                            else
                            {
                                return false;
                            }
                            break;
                        default:
                            return false;
                    }
                    break;
                }
                case state_t::decdig:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            dec_value = dec_value*10 + static_cast<std::size_t>(c - '0');
                            ++digit_count;
                            break;
                        case '.':
                            if (dec_value > 0xff)
                            {
                                return false;
                            }
                            digit_count = 0;
                            dec_value = 0;
                            ++piece_count2;
                            break;
                        default:
                            return false;
                    }
                    break;
                }
                case state_t::unspecified:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                        case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                        case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':
                            state = state_t::hexdig;
                            ++digit_count;
                            break;
                        default:
                            return false;
                    }
                    break;
                }
                default:
                    return false;
            }
        }

        switch (state)
        {
            case state_t::unspecified:
                return piece_count <= 8;
            case state_t::hexdig:
                if (digit_count <= 4)
                {
                    ++piece_count;
                    return digit_count > 0 && (piece_count == 8 || (has_unspecified && piece_count <= 8));
                }
                else
                {
                    return false;
                }
            case state_t::decdig:
                ++piece_count2;
                if (dec_value > 0xff)
                {
                    return false;
                }
                return digit_count > 0 && piece_count2 == 4;
            default:
                return false;
        }
    }

    // RFC 2673, Section 3.2

    inline
    bool validate_ipv4_rfc2673(const std::string& s)
    {
        enum class state_t {expect_indicator_or_dotted_quad,decbyte,
                              bindig, octdig, hexdig};

        state_t state = state_t::expect_indicator_or_dotted_quad;

        std::size_t digit_count = 0;
        std::size_t decbyte_count = 0;
        std::size_t value = 0;

        for (std::size_t i = 0; i < s.length(); ++i)
        {
            const char c = s[i];
            switch (state)
            {
                case state_t::expect_indicator_or_dotted_quad:
                {
                    switch (c)
                    {
                        case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            state = state_t::decbyte;
                            decbyte_count = 0;
                            digit_count = 1;
                            value = 0;
                            break;
                        case 'b':
                            state = state_t::bindig;
                            digit_count = 0;
                            break;
                        case '0':
                            state = state_t::octdig;
                            digit_count = 0;
                            break;
                        case 'x':
                            state = state_t::hexdig;
                            digit_count = 0;
                            break;
                        default:
                            return false;
                    }
                    break;
                }
                case state_t::bindig:
                {
                    if (digit_count >= 256)
                    {
                        return false;
                    }
                    switch (c)
                    {
                        case '0':case '1':
                            ++digit_count;
                            break;
                        default:
                            return false;
                    }
                    break;
                }
                case state_t::octdig:
                {
                    if (digit_count >= 86)
                    {
                        return false;
                    }
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':
                            ++digit_count;
                            break;
                        default:
                            return false;
                    }
                    break;
                }
                case state_t::hexdig:
                {
                    if (digit_count >= 64)
                    {
                        return false;
                    }
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                        case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                        case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':
                            ++digit_count;
                            break;
                        default:
                            return false;
                    }
                    break;
                }
                case state_t::decbyte:
                {
                    if (decbyte_count >= 4)
                    {
                        return false;
                    }
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                        {
                            if (digit_count >= 3)
                            {
                                return false;
                            }
                            ++digit_count;
                            value = value*10 + static_cast<std::size_t>(c - '0');
                            if (value > 255)
                            {
                                return false;
                            }
                            break;
                        }
                        case '.':
                            if (decbyte_count > 3)
                            {
                                return false;
                            }
                            ++decbyte_count;
                            digit_count = 0;
                            value = 0;
                            break;
                        default:
                            return false;
                    }
                    break;
                }
                default:
                    return false;
            }
        }

        switch (state)
        {
            case state_t::decbyte:
                if (digit_count > 0)
                {
                    ++decbyte_count;
                }
                else
                {
                    return false;
                }
                return (decbyte_count == 4) ? true : false;
            case state_t::bindig:
                return digit_count > 0 ? true : false;
            case state_t::octdig:
                return digit_count > 0 ? true : false;
            case state_t::hexdig:
                return digit_count > 0 ? true : false;
            default:
                return false;
        }
    }

    // RFC 1034, Section 3.1
    inline
    bool validate_hostname_rfc1034(const std::string& hostname)
    {
        enum class state_t {start_label,expect_letter_or_digit_or_hyphen_or_dot};

        state_t state = state_t::start_label;
        std::size_t length = hostname.length() - 1;
        std::size_t label_length = 0;

        for (std::size_t i = 0; i < length; ++i)
        {
            const char c = hostname[i];
            switch (state)
            {
                case state_t::start_label:
                {
                    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))
                    {
                        ++label_length;
                        state = state_t::expect_letter_or_digit_or_hyphen_or_dot;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::expect_letter_or_digit_or_hyphen_or_dot:
                {
                    if (c == '.')
                    {
                        label_length = 0;
                        state = state_t::start_label;
                    }
                    else if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                               (c >= '0' && c < '9') || c == '-'))
                    {
                        return false;
                    }
                    if (++label_length > 63)
                    {
                        return false;
                    }
                    break;
                }
            }
        }

        char last = hostname.back();
        if (!((last >= 'a' && last <= 'z') || (last >= 'A' && last <= 'Z') || (last >= '0' && last < '9')))
        {
            return false;
        }
        return true;
    }

    inline
    bool is_leap_year(std::size_t year)
    {
        return (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    }

    inline
    std::size_t days_in_month(std::size_t year, std::size_t month)
    {
        switch (month)
        {
            case 1: return 31;
            case 2: return is_leap_year(year) ? 29 : 28;
            case 3: return 31;
            case 4: return 30;
            case 5: return 31;
            case 6: return 30;
            case 7: return 31;
            case 8: return 31;
            case 9: return 30;
            case 10: return 31;
            case 11: return 30;
            case 12: return 31;
            default:
                JSONCONS_UNREACHABLE();
                break;
        }
    }

    enum class date_time_type {date_time,date,time};
    // RFC 3339, Section 5.6
    inline
    bool validate_date_time_rfc3339(const std::string& s, date_time_type type)
    {
        enum class state_t {fullyear,month,mday,hour,minute,second,secfrac,z,offset_hour,offset_minute};

        std::size_t piece_length = 0;
        std::size_t year = 0;
        std::size_t month = 0;
        std::size_t mday = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int secfrac = 0;
        int offset_signum = 0;
        int offset_hour = 0;
        int offset_minute = 0;      
        
        state_t state = (type == date_time_type::time) ? state_t::hour : state_t::fullyear;

        for (const char c : s)
        {
            switch (state)
            {
                case state_t::fullyear:
                {
                    if (piece_length < 4 && (c >= '0' && c <= '9'))
                    {
                        piece_length++;
                        year = year*10 + static_cast<std::size_t>(c - '0');
                    }
                    else if (c == '-' && piece_length == 4)
                    {
                        state = state_t::month;
                        piece_length = 0;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::month:
                {
                    if (piece_length < 2 && (c >= '0' && c <= '9'))
                    {
                        piece_length++;
                        month = month*10 + static_cast<std::size_t>(c - '0');
                    }
                    else if (c == '-' && piece_length == 2 && (month >=1 && month <= 12))
                    {
                        state = state_t::mday;
                        piece_length = 0;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::mday:
                {
                    if (piece_length < 2 && (c >= '0' && c <= '9'))
                    {
                        piece_length++;
                        mday = mday *10 + static_cast<std::size_t>(c - '0');
                    }
                    else if ((c == 'T' || c == 't') && piece_length == 2 && (mday <= days_in_month(year, month)))
                    {
                        piece_length = 0;
                        state = state_t::hour;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::hour:
                {
                    if (piece_length < 2 && (c >= '0' && c <= '9'))
                    {
                        piece_length++;
                        hour = hour*10 + static_cast<int>(c - '0');
                    }
                    else if (c == ':' && piece_length == 2 && (/*hour >=0 && */ hour <= 23))
                    {
                        state = state_t::minute;
                        piece_length = 0;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::minute:
                {
                    if (piece_length < 2 && (c >= '0' && c <= '9'))
                    {
                        piece_length++;
                        minute = minute*10 + static_cast<int>(c - '0');
                    }
                    else if (c == ':' && piece_length == 2 && (/*minute >=0 && */minute <= 59))
                    {
                        state = state_t::second;
                        piece_length = 0;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::second:
                {
                    if (piece_length < 2 && (c >= '0' && c <= '9'))
                    {
                        piece_length++;
                        second = second*10 + static_cast<int>(c - '0');
                    }
                    else if (piece_length == 2 && second <= 60) // 00-58, 00-59, 00-60 based on leap second rules
                    {
                        switch (c)
                        {
                            case '.':
                                state = state_t::secfrac;
                                break;
                            case '+':
                                offset_signum = 1;
                                piece_length = 0;
                                state = state_t::offset_hour;
                                break;
                            case '-':
                                offset_signum = -1;
                                piece_length = 0;
                                state = state_t::offset_hour;
                                break;
                            case 'Z':
                            case 'z':
                                state = state_t::z;
                                break;
                            default:
                                return false;
                        }
                    }
                    break;
                }
                case state_t::secfrac:
                {
                    if (c >= '0' && c <= '9')
                    {
                        secfrac = secfrac*10 + static_cast<int>(c - '0');
                    }
                    else
                    {
                        switch (c)
                        {
                            case '+':
                                offset_signum = 1;
                                piece_length = 0;
                                state = state_t::offset_hour;
                                break;
                            case '-':
                                offset_signum = -1;
                                piece_length = 0;
                                state = state_t::offset_hour;
                                break;
                            case 'Z':
                            case 'z':
                                state = state_t::z;
                                break;
                            default:
                                return false;
                        }
                    }
                    break;
                }
                case state_t::offset_hour:
                {
                    if (piece_length < 2 && (c >= '0' && c <= '9'))
                    {
                        piece_length++;
                        offset_hour = offset_hour*10 + static_cast<int>(c - '0');
                    }
                    else if (c == ':' && piece_length == 2 && (/*offset_hour >=0 && */offset_hour <= 23))
                    {
                        piece_length = 0;
                        state = state_t::offset_minute;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::offset_minute:
                {
                    if (piece_length < 2 && (c >= '0' && c <= '9'))
                    {
                        piece_length++;
                        offset_minute = offset_minute*10 + static_cast<int>(c - '0');
                    }
                    else if (c == ':' && piece_length == 2 && (/*offset_minute >=0 && */offset_minute <= 59))
                    {
                        piece_length = 0;
                    }
                    else
                    {
                        return false;
                    }
                    break;
                }
                case state_t::z:
                    return false; // Nothing follows z
                default:
                    return false;
            }
        }
        
        switch (state)
        {
            case state_t::hour:
            case state_t::minute:
            case state_t::second:
            case state_t::secfrac:
                return false;
            default:
                break;
        }
        
        if (offset_hour < 0 || offset_hour > 23)
        {
            return false;
        }
        if (offset_minute < 0 || offset_minute > 59)
        {
            return false;
        }
        if (offset_signum == -1)
        {
            offset_hour = -offset_hour;
            offset_minute = -offset_minute;
        }
        auto day_minutes = hour * 60 + minute - (offset_hour * 60 + offset_minute);
        if (day_minutes < 0)
        {
            day_minutes += 60 * 24;
        }
        hour = day_minutes % 24;
        minute = day_minutes / 24;
        
        if (hour == 23 && minute == 59)
        {
            if (second < 0 || second > 60)
            {
                return false;
            }
        }
        else
        {
            if (second < 0 || second > 59)
            {
                return false;
            }
        }

        if (type == date_time_type::date)
        {
            return state == state_t::mday && piece_length == 2 && (mday >= 1 && mday <= days_in_month(year, month));
        }
        return state == state_t::offset_minute || state == state_t::z || state == state_t::secfrac;
    }

    // format checkers
    using validate_format = std::function<walk_result(const validation_message_factory& message_factory,
        const jsonpointer::json_pointer& eval_path, 
        const jsonpointer::json_pointer& instance_location, 
        const std::string&, 
        error_reporter& reporter)>;

    inline
    walk_result uri_check(const validation_message_factory& message_factory, 
        const jsonpointer::json_pointer& eval_path,
        const jsonpointer::json_pointer& instance_location, 
        const std::string& str,
        error_reporter& reporter)
    {
        std::error_code ec;
        auto u = uri::parse(str, ec);
        if (ec)
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path,
                instance_location, 
                "'" + str + "': " + ec.message()));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        else if (!u.is_absolute())
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path,
                instance_location, 
                "'" + str + "' is not an absolute URI."));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        return walk_result::advance;
    }

    inline
    walk_result uri_reference_check(const validation_message_factory& message_factory, const jsonpointer::json_pointer& eval_path,
        const jsonpointer::json_pointer& instance_location, 
        const std::string& str,
        error_reporter& reporter)
    {
        std::error_code ec;
        auto u = uri::parse(str, ec);
        if (ec)
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path,
                instance_location, 
                "'" + str + "': " + ec.message()));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        return walk_result::advance;
    }

    inline
    walk_result jsonpointer_check(const validation_message_factory& message_factory, const jsonpointer::json_pointer& eval_path,
        const jsonpointer::json_pointer& instance_location, 
        const std::string& str,
        error_reporter& reporter)
    {
        std::error_code ec;
        jsonpointer::json_pointer::parse(str, ec);
        if (ec)
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path,
                instance_location, 
                "'" + str + "' is not a valid JSONPointer."));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        return walk_result::advance;
    }

    
    inline
    walk_result rfc3339_date_check(const validation_message_factory& message_factory, const jsonpointer::json_pointer& eval_path,
                            const jsonpointer::json_pointer& instance_location, 
                            const std::string& value,
                            error_reporter& reporter)
    {
        if (!validate_date_time_rfc3339(value,date_time_type::date))
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path,
                instance_location, 
                "'" + value + "' is not a RFC 3339 date string."));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        return walk_result::advance;
    }

    inline
    walk_result rfc3339_time_check(const validation_message_factory& message_factory, const jsonpointer::json_pointer& eval_path,
                            const jsonpointer::json_pointer& instance_location, 
                            const std::string &value,
                            error_reporter& reporter)
    {
        if (!validate_date_time_rfc3339(value, date_time_type::time))        
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path,
                instance_location, 
                "'" + value + "' is not a RFC 3339 time string."));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        return walk_result::advance;
    }

    inline
    walk_result rfc3339_date_time_check(const validation_message_factory& message_factory, const jsonpointer::json_pointer& eval_path,
                                 const jsonpointer::json_pointer& instance_location, 
                                 const std::string &value,
                                 error_reporter& reporter)
    {
        if (!validate_date_time_rfc3339(value, date_time_type::date_time))        
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path,  
                instance_location, 
                "'" + value + "' is not a RFC 3339 date-time string."));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        return walk_result::advance;
    }

    inline
    walk_result email_check(const validation_message_factory& message_factory, const jsonpointer::json_pointer& eval_path,
                     const jsonpointer::json_pointer& instance_location, 
                     const std::string& value,
                     error_reporter& reporter) 
    {
        if (!validate_email_rfc5322(value))        
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path, 
                instance_location, 
                "'" + value + "' is not a valid email address as defined by RFC 5322."));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        return walk_result::advance;
    } 

    inline
    walk_result hostname_check(const validation_message_factory& message_factory, const jsonpointer::json_pointer& eval_path,
                        const jsonpointer::json_pointer& instance_location, 
                        const std::string& value,
                        error_reporter& reporter) 
    {
        if (!validate_hostname_rfc1034(value))
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path, 
                instance_location, 
                "'" + value + "' is not a valid hostname as defined by RFC 3986 Appendix A."));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        return walk_result::advance;
    } 

    inline
    walk_result ipv4_check(const validation_message_factory& message_factory, const jsonpointer::json_pointer& eval_path,
                    const jsonpointer::json_pointer& instance_location, 
                    const std::string& value,
                    error_reporter& reporter) 
    {
        if (!validate_ipv4_rfc2673(value))
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path, 
                instance_location, 
                "'" + value + "' is not a valid IPv4 address as defined by RFC 2673."));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        return walk_result::advance;
    } 

    inline
    walk_result ipv6_check(const validation_message_factory& message_factory, const jsonpointer::json_pointer& eval_path,
                    const jsonpointer::json_pointer& instance_location, 
                    const std::string& value,
                    error_reporter& reporter) 
    {
        if (!validate_ipv6_rfc2373(value))
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path, 
                instance_location, 
                "'" + value + "' is not a valid IPv6 address as defined by RFC 2373."));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
        return walk_result::advance;
    } 

    inline
    walk_result regex_check(const validation_message_factory& message_factory, const jsonpointer::json_pointer& eval_path,
                     const jsonpointer::json_pointer& instance_location, 
                     const std::string& value,
                     error_reporter& reporter) 
    {
#if defined(JSONCONS_HAS_STD_REGEX)
        try 
        {
            std::regex re(value, std::regex::ECMAScript);
        } 
        catch (const std::exception& e) 
        {
            walk_result result = reporter.error(message_factory.make_validation_message(
                eval_path, 
                instance_location, 
                "'" + value + "' is not a valid ECMAScript regular expression. " + e.what()));
            if (result == walk_result::abort)
            {
                return result;
            }
        }
#endif
        return walk_result::advance;
    } 

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_FORMAT_VALIDATOR_HPP
