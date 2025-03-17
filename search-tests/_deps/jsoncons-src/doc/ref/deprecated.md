## Deprecated Features

As the `jsoncons` library has evolved, names have sometimes changed. To ease transition, jsoncons deprecates the old names but continues to support many of them. The deprecated names can be suppressed by defining macro JSONCONS_NO_DEPRECATED, which is recommended for new code.

Category/class|Old name|Replacement
--------|-----------|--------------
__corelib__|&nbsp;|&nbsp;
`basic_json_parser`|&nbsp;|&nbsp;
&nbsp;|`basic_json_parser(std::function<bool(json_errc,const ser_context&)>, const TempAllocator&`|Set error handler in options
&nbsp;|`basic_json_parser(const basic_json_decode_options<char_type>&,std::function<bool(json_errc,const ser_context&)>, const TempAllocator&`|Set error handler in options
&nbsp;|`void update(string_view_type)`|Use `set_buffer` once or provide a chunk reader
&nbsp;|`void update(const char_type*, std::size_t)`|Use `set_buffer` once or provide a chunk reader
`basic_json_reader`|&nbsp;|&nbsp;
&nbsp;|Constructors with `err_handler` parameter|Set `err_handler` in options instead
`basic_json_options`|&nbsp;|&nbsp;
&nbsp;|`bigint_chars_format`|`bignum_chars_format`
&nbsp;|`bigint_chars_format::number`|`bignum_chars_format::raw`


