#include <jsoncons/json.hpp>
#include <memory_resource> 
#include <cassert>
#include <jsoncons/json.hpp>
#include <memory_resource> 
#include <cassert>

void propagtion()
{
    char buffer[1024];
    std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
    std::pmr::polymorphic_allocator<char> alloc(&pool);

    std::string key = "key too long for short string";
    std::string value = "string too long for short string";

    jsoncons::pmr::json j{ jsoncons::json_object_arg, alloc };
    assert(j.get_allocator().resource() == &pool);

    j.try_emplace(key, value);

    auto it = std::search(std::begin(buffer), std::end(buffer), key.begin(), key.end());
    assert(it != std::end(buffer));
    it = std::search(std::begin(buffer), std::end(buffer), value.begin(), value.end());
    assert(it != std::end(buffer));
}

void copy_construction()
{
    char buffer[1024];
    std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
    std::pmr::polymorphic_allocator<char> alloc(&pool);

    jsoncons::pmr::json j{ "String too long for short string", alloc };
    assert(j.is_string());
    assert(j.get_allocator().resource() == &pool);

    jsoncons::pmr::json j1(j);
    assert(j1 == j);
    assert(j1.get_allocator() == std::allocator_traits<std::pmr::polymorphic_allocator<char>>::
        select_on_container_copy_construction(j.get_allocator()));
    assert(j1.get_allocator() == std::pmr::polymorphic_allocator<char>{}); // expected result for pmr allocators
}

void allocator_extended_copy_construction() // extended copy
{
    char buffer[1024];
    std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
    std::pmr::polymorphic_allocator<char> alloc(&pool);

    char buffer1[1024];
    std::pmr::monotonic_buffer_resource pool1{ std::data(buffer1), std::size(buffer1) };
    std::pmr::polymorphic_allocator<char> alloc1(&pool1);

    jsoncons::pmr::json j{ "String too long for short string", alloc };
    assert(j.is_string());
    assert(j.get_allocator().resource() == &pool);

    jsoncons::pmr::json j1(j, alloc1);
    assert(j1 == j);
    assert(j1.get_allocator().resource() == &pool1);
}

void move_construction() // move
{
    char buffer[1024];
    std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
    std::pmr::polymorphic_allocator<char> alloc(&pool);

    jsoncons::pmr::json j{ "String too long for short string", alloc};
    assert(j.is_string());
    assert(j.get_allocator().resource() == &pool);

    jsoncons::pmr::json j1(std::move(j));
    assert(j1.is_string());
    assert(j1.get_allocator().resource() == &pool);
    assert(j.is_null());
}

void allocator_extended_move_construction() // move
{
    char buffer[1024];
    std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
    std::pmr::polymorphic_allocator<char> alloc(&pool);

    char buffer1[1024];
    std::pmr::monotonic_buffer_resource pool1{ std::data(buffer1), std::size(buffer1) };
    std::pmr::polymorphic_allocator<char> alloc1(&pool1);

    jsoncons::pmr::json j{ "String too long for short string", alloc };
    assert(j.is_string());
    assert(j.get_allocator().resource() == &pool);

    jsoncons::pmr::json j1(std::move(j), alloc1);
    assert(j1.is_string());
    assert(j1.get_allocator().resource() == &pool1);
}

void copy_assignment() // assignment
{
    char buffer[1024];
    std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
    std::pmr::polymorphic_allocator<char> alloc(&pool);

    char buffer1[1024];
    std::pmr::monotonic_buffer_resource pool1{ std::data(buffer1), std::size(buffer1) };
    std::pmr::polymorphic_allocator<char> alloc1(&pool1);

    jsoncons::pmr::json j{ "String too long for short string", alloc };
    assert(j.is_string());
    assert(j.get_allocator().resource() == &pool);

    // copy long string to number
    jsoncons::pmr::json j1{10};
    j1 = j;
    assert(j1.is_string());
    assert(j1.get_allocator() == std::allocator_traits<std::pmr::polymorphic_allocator<char>>::
        select_on_container_copy_construction(j.get_allocator()));
    assert(j1.get_allocator() == std::pmr::polymorphic_allocator<char>{});

    // copy long string to array
    jsoncons::pmr::json j2{ jsoncons::json_array_arg, {1,2,3,4}, 
        jsoncons::semantic_tag::none, alloc1};
    j2 = j;
    assert(j2.is_string());
    assert(j2.get_allocator().resource() == &pool1);
}

void move_assignment() // assignment
{
    char buffer[1024];
    std::pmr::monotonic_buffer_resource pool{ std::data(buffer), std::size(buffer) };
    std::pmr::polymorphic_allocator<char> alloc(&pool);

    jsoncons::pmr::json j{ "String too long for short string", alloc };
    assert(j.is_string());
    assert(j.get_allocator().resource() == &pool);

    jsoncons::pmr::json j1{ 10 };
    assert(j1.is_number());

    j1 = std::move(j);
    assert(j1.is_string());
    assert(j1.get_allocator().resource() == &pool);
    assert(j.is_number());
}

int main()
{
    propagtion();
    copy_construction();
    allocator_extended_copy_construction();
    move_construction();
    allocator_extended_move_construction();
    copy_assignment();
    move_assignment();
}

