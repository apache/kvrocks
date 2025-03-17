// Use span

#include "nonstd/span.hpp"
#include <array>
#include <vector>
#include <iostream>

std::ptrdiff_t size( nonstd::span<const int> spn )
{
    return spn.size();
}

int main()
{
    int arr[] = { 1, };

    std::cout <<
        "C-array:" << size( arr ) <<
        " array:"  << size( std::array <int, 2>{ 1, 2, } ) <<
        " vector:" << size( std::vector<int   >{ 1, 2, 3, } );
}

#if 0
cl -EHsc -I../include 01-basic.cpp && 01-basic.exe
g++ -std=c++11 -Wall -I../include -o 01-basic.exe 01-basic.cpp && 01-basic.exe

Output: C-array:1 array:2 vector:3
#endif
