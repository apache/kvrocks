//! [transform example]
#include <iostream>
#include <vector>
#include <range/v3/view/transform.hpp>

int main()
{
    std::vector<int> numbers{1, 2, 3};

    auto halved = numbers
        // Divide each integer by 2, converting it into a double
        | ranges::views::transform([](const int& num) {
          return num / 2.0;
        });

    std::cout << halved << '\n';
}
//! [transform example]
