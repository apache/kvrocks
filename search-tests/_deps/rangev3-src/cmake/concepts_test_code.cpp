#if !defined(__cpp_concepts) || __cpp_concepts == 0
#error "Sorry, Charlie. No concepts"
#else
#if __cpp_concepts <= 201507L
#define concept concept bool
#endif

template<class>
concept True = true;

template<class T>
constexpr bool test(T)
{
    return false;
}

template<class T>
    requires True<T>
constexpr bool test(T)
{
    return true;
}

int main()
{
    static_assert(::test(42), "");
}

#endif
