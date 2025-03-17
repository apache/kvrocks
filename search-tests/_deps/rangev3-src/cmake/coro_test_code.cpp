#if defined(__cpp_coroutines) && defined(__has_include)
#if __has_include(<coroutine>)
#include <coroutine>
namespace std_coro = std;
#elif __has_include(<experimental/coroutine>)
#include <experimental/coroutine>
namespace std_coro = std::experimental;
#else
#error Either the compiler or the library lacks support for coroutines
#endif
#else
#error Either the compiler or the library lacks support for coroutines
#endif

struct present
{
    struct promise_type
    {
        int result;

        present get_return_object() { return {*this}; }
        std_coro::suspend_never initial_suspend() { return {}; }
        std_coro::suspend_never final_suspend() noexcept { return {}; }
        void return_value(int i) { result = i; }
        void unhandled_exception() {}
    };

    promise_type& promise;

    bool await_ready() const { return true; }
    void await_suspend(std_coro::coroutine_handle<>) const {}
    int await_resume() const { return promise.result; }
};

present f(int n)
{
    if (n < 2)
        co_return 1;
    else
        co_return n * co_await f(n - 1);
}

int main()
{
    return f(5).promise.result != 120;
}
