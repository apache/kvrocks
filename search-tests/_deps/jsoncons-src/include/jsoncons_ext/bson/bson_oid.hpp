#ifndef JSONCONS_EXT_BSON_BSON_OID_HPP
#define JSONCONS_EXT_BSON_BSON_OID_HPP

/*
 *  Implements class oid_t and non member function bson_oid_to_string
 *  
 *  Based on the libjson functions bson_oid_to_string 
 *  and bson_oid_init_from_string_unsafe , available at
 *  https://github.com/mongodb/mongo-c-driver/blob/master/src/libbson/src/bson/bson-oid.h
 *  and https://github.com/mongodb/mongo-c-driver/blob/master/src/libbson/src/bson/bson-oid.c
 *  
*/

/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <type_traits>

#include <jsoncons/config/jsoncons_config.hpp>

namespace jsoncons { namespace bson {

    class oid_t
    {
        std::array<uint8_t,12> bytes_;
    public:
        using iterator = std::array<uint8_t,12>::iterator;
        using const_iterator = std::array<uint8_t,12>::const_iterator;

        oid_t(const std::array<uint8_t,12>& bytes)
            : bytes_(bytes)
        {
        }
        oid_t(uint8_t data[12])
        {
            std::memcpy(bytes_.data(),data,12);
        }

        oid_t(const string_view& str)
        {
            for (std::size_t i = 0; i < bytes_.size(); i++) 
            {
               bytes_[i] = ((parse_hex_char (str[2 * i]) << 4) |
                           (parse_hex_char (str[2 * i + 1])));
            }
        }

        const uint8_t* data() const
        {
            return bytes_.data();
        }

        std::size_t size() const
        {
            return bytes_.size();
        }

        iterator begin()
        {
            return bytes_.begin();
        }

        iterator end()
        {
            return bytes_.end();
        }

        const_iterator begin() const
        {
            return bytes_.begin();
        }

        const_iterator end() const
        {
            return bytes_.end();
        }

    private:

        static uint8_t parse_hex_char (char hex)
        {
           switch (hex) {
           case '0':
              return 0;
           case '1':
              return 1;
           case '2':
              return 2;
           case '3':
              return 3;
           case '4':
              return 4;
           case '5':
              return 5;
           case '6':
              return 6;
           case '7':
              return 7;
           case '8':
              return 8;
           case '9':
              return 9;
           case 'a':
           case 'A':
              return 0xa;
           case 'b':
           case 'B':
              return 0xb;
           case 'c':
           case 'C':
              return 0xc;
           case 'd':
           case 'D':
              return 0xd;
           case 'e':
           case 'E':
              return 0xe;
           case 'f':
           case 'F':
              return 0xf;
           default:
              return 0;
           }
        }
    };

namespace detail {

        inline
        const uint16_t* get_hex_char_pairs(std::true_type) // big endian
        {
            static const uint16_t hex_char_pairs[] = {
               12336, 12337, 12338, 12339, 12340, 12341, 12342, 12343, 12344, 12345, 12385,
               12386, 12387, 12388, 12389, 12390, 12592, 12593, 12594, 12595, 12596, 12597,
               12598, 12599, 12600, 12601, 12641, 12642, 12643, 12644, 12645, 12646, 12848,
               12849, 12850, 12851, 12852, 12853, 12854, 12855, 12856, 12857, 12897, 12898,
               12899, 12900, 12901, 12902, 13104, 13105, 13106, 13107, 13108, 13109, 13110,
               13111, 13112, 13113, 13153, 13154, 13155, 13156, 13157, 13158, 13360, 13361,
               13362, 13363, 13364, 13365, 13366, 13367, 13368, 13369, 13409, 13410, 13411,
               13412, 13413, 13414, 13616, 13617, 13618, 13619, 13620, 13621, 13622, 13623,
               13624, 13625, 13665, 13666, 13667, 13668, 13669, 13670, 13872, 13873, 13874,
               13875, 13876, 13877, 13878, 13879, 13880, 13881, 13921, 13922, 13923, 13924,
               13925, 13926, 14128, 14129, 14130, 14131, 14132, 14133, 14134, 14135, 14136,
               14137, 14177, 14178, 14179, 14180, 14181, 14182, 14384, 14385, 14386, 14387,
               14388, 14389, 14390, 14391, 14392, 14393, 14433, 14434, 14435, 14436, 14437,
               14438, 14640, 14641, 14642, 14643, 14644, 14645, 14646, 14647, 14648, 14649,
               14689, 14690, 14691, 14692, 14693, 14694, 24880, 24881, 24882, 24883, 24884,
               24885, 24886, 24887, 24888, 24889, 24929, 24930, 24931, 24932, 24933, 24934,
               25136, 25137, 25138, 25139, 25140, 25141, 25142, 25143, 25144, 25145, 25185,
               25186, 25187, 25188, 25189, 25190, 25392, 25393, 25394, 25395, 25396, 25397,
               25398, 25399, 25400, 25401, 25441, 25442, 25443, 25444, 25445, 25446, 25648,
               25649, 25650, 25651, 25652, 25653, 25654, 25655, 25656, 25657, 25697, 25698,
               25699, 25700, 25701, 25702, 25904, 25905, 25906, 25907, 25908, 25909, 25910,
               25911, 25912, 25913, 25953, 25954, 25955, 25956, 25957, 25958, 26160, 26161,
               26162, 26163, 26164, 26165, 26166, 26167, 26168, 26169, 26209, 26210, 26211,
               26212, 26213, 26214};

            return hex_char_pairs;
        }

        inline
        const uint16_t* get_hex_char_pairs(std::false_type) // little endian
        {
            static const uint16_t hex_char_pairs[] = {
                12336, 12592, 12848, 13104, 13360, 13616, 13872, 14128, 14384, 14640, 24880,
                25136, 25392, 25648, 25904, 26160, 12337, 12593, 12849, 13105, 13361, 13617,
                13873, 14129, 14385, 14641, 24881, 25137, 25393, 25649, 25905, 26161, 12338,
                12594, 12850, 13106, 13362, 13618, 13874, 14130, 14386, 14642, 24882, 25138,
                25394, 25650, 25906, 26162, 12339, 12595, 12851, 13107, 13363, 13619, 13875,
                14131, 14387, 14643, 24883, 25139, 25395, 25651, 25907, 26163, 12340, 12596,
                12852, 13108, 13364, 13620, 13876, 14132, 14388, 14644, 24884, 25140, 25396,
                25652, 25908, 26164, 12341, 12597, 12853, 13109, 13365, 13621, 13877, 14133,
                14389, 14645, 24885, 25141, 25397, 25653, 25909, 26165, 12342, 12598, 12854,
                13110, 13366, 13622, 13878, 14134, 14390, 14646, 24886, 25142, 25398, 25654,
                25910, 26166, 12343, 12599, 12855, 13111, 13367, 13623, 13879, 14135, 14391,
                14647, 24887, 25143, 25399, 25655, 25911, 26167, 12344, 12600, 12856, 13112,
                13368, 13624, 13880, 14136, 14392, 14648, 24888, 25144, 25400, 25656, 25912,
                26168, 12345, 12601, 12857, 13113, 13369, 13625, 13881, 14137, 14393, 14649,
                24889, 25145, 25401, 25657, 25913, 26169, 12385, 12641, 12897, 13153, 13409,
                13665, 13921, 14177, 14433, 14689, 24929, 25185, 25441, 25697, 25953, 26209,
                12386, 12642, 12898, 13154, 13410, 13666, 13922, 14178, 14434, 14690, 24930,
                25186, 25442, 25698, 25954, 26210, 12387, 12643, 12899, 13155, 13411, 13667,
                13923, 14179, 14435, 14691, 24931, 25187, 25443, 25699, 25955, 26211, 12388,
                12644, 12900, 13156, 13412, 13668, 13924, 14180, 14436, 14692, 24932, 25188,
                25444, 25700, 25956, 26212, 12389, 12645, 12901, 13157, 13413, 13669, 13925,
                14181, 14437, 14693, 24933, 25189, 25445, 25701, 25957, 26213, 12390, 12646,
                12902, 13158, 13414, 13670, 13926, 14182, 14438, 14694, 24934, 25190, 25446,
                25702, 25958, 26214};

            return hex_char_pairs;
        }

        inline
        void init_hex_char_pairs(const oid_t& oid, uint16_t* data)
        {
            const uint8_t* bytes = oid.data();
            const uint16_t* gHexCharPairs = get_hex_char_pairs(std::integral_constant<bool, jsoncons::endian::native == jsoncons::endian::big>());

            data[0] = gHexCharPairs[bytes[0]];
            data[1] = gHexCharPairs[bytes[1]];
            data[2] = gHexCharPairs[bytes[2]];
            data[3] = gHexCharPairs[bytes[3]];
            data[4] = gHexCharPairs[bytes[4]];
            data[5] = gHexCharPairs[bytes[5]];
            data[6] = gHexCharPairs[bytes[6]];
            data[7] = gHexCharPairs[bytes[7]];
            data[8] = gHexCharPairs[bytes[8]];
            data[9] = gHexCharPairs[bytes[9]];
            data[10] = gHexCharPairs[bytes[10]];
            data[11] = gHexCharPairs[bytes[11]];
        }

} // namespace detail

    template <typename StringT>
    inline 
    void to_string(const oid_t& oid, StringT& s)
    {
        s.resize(24);
        detail::init_hex_char_pairs(oid, reinterpret_cast<uint16_t*>(&s[0]));
    }

} // namespace bson
} // namespace jsoncons

#endif // JSONCONS_EXT_BSON_BSON_OID_HPP
