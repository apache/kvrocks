//! [ints example]
#include <iostream>
#include <vector>
#include <range/v3/view/iota.hpp>

int main()
{
    auto numbers = ranges::views::ints(3, 7);

    std::cout << numbers << '\n';
}
//! [ints example]
