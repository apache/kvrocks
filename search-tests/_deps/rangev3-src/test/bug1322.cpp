#include <concepts/concepts.hpp>

struct S
{};

template <typename T>
void foobar(T &&) {}

#if CPP_CXX_CONCEPTS
template <typename T>
    requires concepts::totally_ordered<T>
void foobar(T &&) {}
#endif

int main()
{
    std::pair<S, int> p;
    foobar(p);
}
