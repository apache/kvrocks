### jsoncons::basic_json::operator[]

```cpp
proxy_type operator[](const string_view_type& key);             (1)  (until 1.0.0)
reference operator[](const string_view_type& key);                   (since 1.0.0)

const_reference operator[](const string_view_type& key) const;  (2)

reference operator[](std::size_t i);                            (3)

const_reference operator[](std::size_t i) const;                (4)
```

(1) Unitl 1.0.0, returns a "reference-like" proxy object that can be used to access 
or assign to the underlying keyed value. 
  
Since 0.179, returns a reference to the value mapped to `key`, 
inserting a default constructed value mapped to `key` if no such key already exists 
(a default constructed value is an empty object.)

(2) Until 1.0.0, if `key` exists, returns a const reference to the `basic_json` value mapped to `key`, otherwise throws.

Since 1.0.0, returns a const reference to the `basic_json` value mapped to `key`, returning a const reference to a default 
constructed `basic_json` value with static storage duration if no such key already exists.

(3) Returns a reference to the value at index i in a `basic_json` object or array.
Throws `std::domain_error` if not an object or array.

(4) Returns a `const_reference` to the value at index i in a `basic_json` object or array.
Throws `std::domain_error` if not an object or array.

Exceptions

(1) Throws `std::domain_error` if not an object. Until 1.0.0, throws a `std::out_of_range` if 
assigning to a `basic_json` and the key does not exist. Since 1.0.0, inserts a default constructed
key-value pair if the key does not exist. 

(2) Throws `std::domain_error` if not an object. Until 1.0.0, throws a `std::out_of_range` if 
the key does not exist. Since 1.0.0, does 

(3) Throws `std::domain_error` if not an array.

(4) Throws `std::domain_error` if not an array.

#### Notes

Unlike `std::map::operator[]`, a new element is never inserted into the container 
when this operator is used for reading but the key does not exist.

### Examples

#### Assigning to an object when the key does not exist

```cpp
int main()
{
    json image_formats(json_array_arg, {"JPEG","PSD","TIFF","DNG"});

    json color_spaces(json_array_arg);
    color_spaces.push_back("sRGB");
    color_spaces.push_back("AdobeRGB");
    color_spaces.push_back("ProPhoto RGB");

    json export_settings;
    export_settings["File Format Options"]["Color Spaces"] = std::move(color_spaces);
    export_settings["File Format Options"]["Image Formats"] = std::move(image_formats);

    std::cout << pretty_print(export_settings) << "\n\n";
}
```
Output:
```
{
    "File Format Options": {
        "Color Spaces": ["sRGB", "AdobeRGB", "ProPhoto RGB"],
        "Image Formats": ["JPEG", "PSD", "TIFF", "DNG"]
    }
}
```
Note that if `file_export["File Format Options"]` doesn’t exist, the statement
```
file_export["File Format Options"]["Color Spaces"] = std::move(color_spaces)
```
creates "File Format Options" as an object and puts "Color Spaces" in it.

