#include <range/v3/view/sample.hpp>
#include <range/v3/algorithm/equal.hpp>
#include <numeric>
#include <vector>
#include <random>
#include "../simple_test.hpp"
#include "../test_utils.hpp"

using namespace ranges;

int main ()
{
    std::mt19937 engine;

    std::vector<int> pop(100);
    std::iota(std::begin(pop), std::end(pop), 0);
    {
        constexpr int N = 32;
        std::array<int, N> tmp;
        auto rng = pop | views::sample(N, engine);
        using Rng = decltype(rng);
        CPP_assert(input_range<Rng> && view_<Rng>);
        CPP_assert(!forward_range<Rng>);
        ranges::copy(rng, tmp.begin());
        rng = pop | views::sample(N, engine);
        CHECK(!ranges::equal(rng, tmp));
        engine = decltype(engine){};
        rng = pop | views::sample(N, engine);
        CHECK(ranges::equal(rng, tmp));
    }

    {
        int const some_ints[] = {0,1,2,3,4,5,6,7,8};
        auto rng = debug_input_view<int const>{some_ints} | views::sample(4, engine);
        using Rng = decltype(rng);
        CPP_assert(input_range<Rng> && view_<Rng>);
        CPP_assert(!forward_range<Rng>);
        CHECK(ranges::distance(rng) == 4);
    }

    return ::test_result();
}
