#include <vector>

#include <range/v3/view/drop.hpp>
#include <range/v3/view/view.hpp>

#include "../simple_test.hpp"
#include "../test_utils.hpp"

using namespace ranges;

struct my_drop_base_fn
{
    template<typename Rng>
    auto operator()(Rng && rng, range_difference_t<Rng> n) const
    {
        return drop_view<views::all_t<Rng>>(views::all(static_cast<Rng &&>(rng)), n);
    }
};
struct my_drop_fn : my_drop_base_fn
{
    using my_drop_base_fn::operator();

    template<typename Int>
    constexpr auto operator()(Int n) const
    {
        return make_view_closure([=](auto && rng) {
            return my_drop_base_fn{}(std::forward<decltype(rng)>(rng), n); });
    }
};
RANGES_INLINE_VARIABLE(my_drop_fn, my_drop)

/// #https://github.com/ericniebler/range-v3/issues/1169
void constexpr_test_1169()
{
#if RANGES_CXX_CONSTEXPR >= RANGES_CXX_CONSTEXPR_LAMBDAS
    constexpr auto const drop1 = my_drop(1);
    constexpr auto const drop3 = drop1 | my_drop(2);

    std::vector<int> vec = {1, 2, 3, 4};
    check_equal(vec | drop3, {4});
#endif
    (void)my_drop;
}

int main()
{
    constexpr_test_1169();
    return test_result();
}
