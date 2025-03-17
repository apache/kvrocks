// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <string>
#include <cwchar>
#ifdef _MSC_VER
#include <codecvt>
#endif
#include <jsoncons/json.hpp>
#include <fstream>
#include <iomanip>

using namespace jsoncons;

void wjson_object()
{
    wjson j;
    j[L"field1"] = L"test";
    j[L"field2"] = 3.9;
    j[L"field3"] = true;
    std::wcout << j << L"\n";
}

#if 0
void wjson_escape_u2()
{
#ifdef _MSC_VER
    std::wstring input = L"[\"\\u007F\\u07FF\\u0800\"]";
    std::wistringstream is(input);

    wjson val = wjson::parse(is);

    std::wstring s = val[0].as<std::wstring>();
    std::cout << "length=" << s.length() << '\n';
    std::cout << "Hex dump: [";
    for (std::size_t i = 0; i < s.size(); ++i)
    {
        if (i != 0)
            std::cout << " ";
        uint32_t u(s[i] >= 0 ? s[i] : 256 + s[i] );
        std::cout << "0x"  << std::hex<< std::setfill('0') << std::setw(2) << u;
    }
    std::cout << "]" << '\n';

    std::wofstream os("./output/xxx.txt");
    os.imbue(std::locale(os.getloc(), new std::codecvt_utf8_utf16<wchar_t>));
    
    auto options = wjson_options{}
        .escape_all_non_ascii(true);

    os << pretty_print(val,options) << L"\n";
#endif
}
#endif

void wjson_surrogate_pair()
{
#ifdef _MSC_VER
    std::wstring input = L"[\"\\uD950\\uDF21\"]";
    std::wistringstream is(input);

    wjson val = wjson::parse(is);

    std::wstring s = val[0].as<std::wstring>();
    std::cout << "length=" << s.length() << '\n';
    std::cout << "Hex dump: [";
    for (std::size_t i = 0; i < s.size(); ++i)
    {
        if (i != 0)
            std::cout << " ";
        uint32_t u(s[i] >= 0 ? s[i] : 256 + s[i] );
        std::cout << "0x"  << std::hex<< std::setfill('0') << std::setw(2) << u;
    }
    std::cout << "]" << '\n';
#endif
}

int main()
{
    std::cout << "\nwjson examples\n\n";
    wjson_object();
    //wjson_escape_u2();
    wjson_surrogate_pair();
    std::cout << '\n';
}


