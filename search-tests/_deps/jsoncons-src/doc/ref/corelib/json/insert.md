### jsoncons::basic_json::insert

```cpp
template <typename T>
array_iterator insert(const_array_iterator pos, T&& val); (1)

template <typename InputIt>
array_iterator insert(const_array_iterator pos, InputIt first, InputIt last); (2)
```
(1) Adds a new json element at the specified position of a json array, shifting all elements currently at or above that position to the right.
The argument `val` is forwarded to the `json` constructor as `std::forward<T>(val)`.
Returns an `array_iterator` that points to the new value
Throws `std::domain_error` if not an array.  

(2) Inserts elements from range [first, last) before pos.

```
template <typename InputIt>
void insert(InputIt first, InputIt last); (3)
```

(3) Inserts elements from range `[first, last)` into a json object. 
    If multiple elements in the range have the same key, the first element in the range is inserted.
    The function template parameter `InputIt` represents an input
    iterator type that iterates over elements of type `key_value_type`,
    or alternatively over elements of type `std::pair<T1,T2>` where `T1` is convertible to `key_type` and `T2` is convertible to `basic_json`. 

### Examples

#### Creating an array of elements 
```cpp
json cities(json_array_arg);       // an empty array
std::cout << cities << '\n';  // output is "[]"

cities.push_back("Toronto");  
cities.push_back("Vancouver");
// Insert "Montreal" at beginning of array
cities.insert(cities.array_range().begin(),"Montreal");  

std::cout << cities << '\n';
```
Output:
```
[]
["Montreal","Toronto","Vancouver"]
```
#### Creating an array of elements with reserved storage 
```cpp
json cities(json_array_arg);  
cities.reserve(10);  // storage is reserved
std::cout << "capacity=" << cities.capacity() 
          << ", size=" << cities.size() << '\n';

cities.push_back("Toronto");  
cities.push_back("Vancouver");
cities.insert(cities.array_range().begin(),"Montreal");
std::cout << "capacity=" << cities.capacity() 
          << ", size=" << cities.size() << '\n';

std::cout << cities << '\n';
```
Output:
```
capacity=10, size=0
capacity=10, size=3
["Montreal","Toronto","Vancouver"]
```

### Copy two std::map's into a json 

```cpp
std::map<std::string,double> m1 = {{"f",4},{"e",5},{"d",6}};
std::map<std::string,double> m2 = {{"c",1},{"b",2},{"a",3}};

json j;
j.insert(m1.begin(),m1.end());
j.insert(m2.begin(),m2.end());

std::cout << j << "\n";
```
Output:
```
{"a":3.0,"b":2.0,"c":1.0,"d":6.0,"e":5.0,"f":4.0}
```

### Copy two std::map's into an ojson 

```cpp
std::map<std::string,double> m1 = {{"f",4},{"e",5},{"d",6}};
std::map<std::string,double> m2 = {{"c",1},{"b",2},{"a",3}};

ojson j;
j.insert(m1.begin(),m1.end());
j.insert(m2.begin(),m2.end());

std::cout << j << "\n";
```
Output:
```
{"d":6.0,"e":5.0,"f":4.0,"a":3.0,"b":2.0,"c":1.0}
```

### Move two std::map's into a json 

```cpp
std::map<std::string,double> m1 = {{"a",1},{"b",2},{"c",3}};
std::map<std::string,double> m2 = {{"d",4},{"e",5},{"f",6}};

json j;
j.insert(std::make_move_iterator(m1.begin()),std::make_move_iterator(m1.end()));
j.insert(std::make_move_iterator(m2.begin()),std::make_move_iterator(m2.end()));

std::cout << j << "\n";
```
Output:
```
{"a":1.0,"b":2.0,"c":3.0,"d":4.0,"e":5.0,"f":6.0}
```

### See also

[push_back](push_back.md)


