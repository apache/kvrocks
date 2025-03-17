/**
 * MIT License
 *
 * Copyright (c) 2017 Thibaut Goetghebuer-Planchon <tessil@gmx.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#ifndef TSL_UTILS_H
#define TSL_UTILS_H

#include <boost/numeric/conversion/cast.hpp>
#include <memory>
#include <ostream>
#include <string>

class move_only_test {
 public:
  explicit move_only_test(std::int64_t value)
      : m_value(new std::string(std::to_string(value))) {}

  explicit move_only_test(std::string value)
      : m_value(new std::string(std::move(value))) {}

  move_only_test(const move_only_test&) = delete;
  move_only_test(move_only_test&&) = default;
  move_only_test& operator=(const move_only_test&) = delete;
  move_only_test& operator=(move_only_test&&) = default;

  friend std::ostream& operator<<(std::ostream& stream,
                                  const move_only_test& value) {
    if (value.m_value == nullptr) {
      stream << "null";
    } else {
      stream << *value.m_value;
    }

    return stream;
  }

  friend bool operator==(const move_only_test& lhs, const move_only_test& rhs) {
    if (lhs.m_value == nullptr || rhs.m_value == nullptr) {
      return lhs.m_value == nullptr && rhs.m_value == nullptr;
    } else {
      return *lhs.m_value == *rhs.m_value;
    }
  }

  friend bool operator!=(const move_only_test& lhs, const move_only_test& rhs) {
    return !(lhs == rhs);
  }

  friend bool operator<(const move_only_test& lhs, const move_only_test& rhs) {
    if (lhs.m_value == nullptr && rhs.m_value == nullptr) {
      return false;
    } else if (lhs.m_value == nullptr) {
      return true;
    } else if (rhs.m_value == nullptr) {
      return false;
    } else {
      return *lhs.m_value < *rhs.m_value;
    }
  }

  std::string value() const { return *m_value; }

 private:
  std::unique_ptr<std::string> m_value;
};

class throw_move_test {
 public:
  explicit throw_move_test(std::int64_t value) : m_value(value) {}

  throw_move_test(const throw_move_test&) = default;

  throw_move_test(throw_move_test&& other) noexcept(false)
      : m_value(other.m_value) {}

  throw_move_test& operator=(const throw_move_test&) = default;

  throw_move_test& operator=(throw_move_test&& other) noexcept(false) {
    m_value = other.m_value;
    return *this;
  }

  operator std::int64_t() const { return m_value; }

 private:
  std::int64_t m_value;
};

class utils {
 public:
  template <typename CharT>
  static std::basic_string<CharT> get_key(std::size_t counter);

  template <typename T>
  static T get_value(std::size_t counter);

  template <typename TMap>
  static TMap get_filled_map(std::size_t nb_elements,
                             std::size_t burst_threshold);
};

template <>
inline std::basic_string<char> utils::get_key<char>(std::size_t counter) {
  return "Key " + std::to_string(counter);
}

template <>
inline std::int64_t utils::get_value<std::int64_t>(std::size_t counter) {
  return boost::numeric_cast<std::int64_t>(counter * 2);
}

template <>
inline std::string utils::get_value<std::string>(std::size_t counter) {
  return "Value " + std::to_string(counter);
}

template <>
inline throw_move_test utils::get_value<throw_move_test>(std::size_t counter) {
  return throw_move_test(boost::numeric_cast<std::int64_t>(counter * 2));
}

template <>
inline move_only_test utils::get_value<move_only_test>(std::size_t counter) {
  return move_only_test(boost::numeric_cast<std::int64_t>(counter * 2));
}

template <typename TMap>
inline TMap utils::get_filled_map(std::size_t nb_elements,
                                  std::size_t burst_threshold) {
  using char_tt = typename TMap::char_type;
  using value_tt = typename TMap::mapped_type;

  TMap map(burst_threshold);
  for (std::size_t i = 0; i < nb_elements; i++) {
    map.insert(utils::get_key<char_tt>(i), utils::get_value<value_tt>(i));
  }

  return map;
}

/**
 * serializer helpers to test serialize(...) and deserialize(...) functions
 */
class serializer {
 public:
  serializer() { m_ostream.exceptions(m_ostream.badbit | m_ostream.failbit); }

  template <class T>
  void operator()(const T& val) {
    serialize_impl(val);
  }

  void operator()(const char* data, std::uint64_t size) {
    m_ostream.write(data, size);
  }

  std::string str() const { return m_ostream.str(); }

 private:
  void serialize_impl(const std::string& val) {
    serialize_impl(boost::numeric_cast<std::uint64_t>(val.size()));
    m_ostream.write(val.data(), val.size());
  }

  void serialize_impl(const move_only_test& val) {
    serialize_impl(val.value());
  }

  template <class T, typename std::enable_if<
                         std::is_arithmetic<T>::value>::type* = nullptr>
  void serialize_impl(const T& val) {
    m_ostream.write(reinterpret_cast<const char*>(&val), sizeof(val));
  }

 private:
  std::stringstream m_ostream;
};

class deserializer {
 public:
  deserializer(const std::string& init_str = "") : m_istream(init_str) {
    m_istream.exceptions(m_istream.badbit | m_istream.failbit |
                         m_istream.eofbit);
  }

  template <class T>
  T operator()() {
    return deserialize_impl<T>();
  }

  void operator()(char* data_out, std::uint64_t size) {
    m_istream.read(data_out, boost::numeric_cast<std::size_t>(size));
  }

 private:
  template <class T, typename std::enable_if<
                         std::is_same<std::string, T>::value>::type* = nullptr>
  T deserialize_impl() {
    const std::size_t str_size =
        boost::numeric_cast<std::size_t>(deserialize_impl<std::uint64_t>());

    // TODO std::string::data() return a const pointer pre-C++17. Avoid the
    // inefficient double allocation.
    std::vector<char> chars(str_size);
    m_istream.read(chars.data(), str_size);

    return std::string(chars.data(), chars.size());
  }

  template <class T, typename std::enable_if<std::is_same<
                         move_only_test, T>::value>::type* = nullptr>
  move_only_test deserialize_impl() {
    return move_only_test(deserialize_impl<std::string>());
  }

  template <class T, typename std::enable_if<
                         std::is_arithmetic<T>::value>::type* = nullptr>
  T deserialize_impl() {
    T val;
    m_istream.read(reinterpret_cast<char*>(&val), sizeof(val));

    return val;
  }

 private:
  std::stringstream m_istream;
};

#endif
