//! [filter example]
#include <iostream>
#include <vector>
#include <range/v3/view/filter.hpp>

int main()
{
    std::vector<int> numbers{1, 2, 3, 4};

    auto even = numbers
        // Keep only the even numbers
        | ranges::views::filter([](const int& num) {
          return num % 2 == 0;
        });

    std::cout << even << '\n';
}
//! [filter example]
