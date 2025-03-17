source tests/includes/init-tests.tcl

start_server {tags {"delprefix"}} {
    r flushdb

    # Adding keys with different prefixes
    r set user:1 "Alice"
    r set user:2 "Bob"
    r set order:1 "Order123"
    r set order:2 "Order456"
    r set random "SomeValue"

    # Ensure the keys exist
    assert_equal "Alice" [r get user:1]
    assert_equal "Bob" [r get user:2]
    assert_equal "Order123" [r get order:1]
    assert_equal "Order456" [r get order:2]
    assert_equal "SomeValue" [r get random]

    # Delete keys with prefix "user:"
    assert_equal 2 [r delprefix user:]

    # Ensure the correct keys were deleted
    assert_equal {nil} [r get user:1]
    assert_equal {nil} [r get user:2]
    assert_equal "Order123" [r get order:1]
    assert_equal "Order456" [r get order:2]
    assert_equal "SomeValue" [r get random]

    # Delete keys with prefix "order:"
    assert_equal 2 [r delprefix order:]

    # Ensure the correct keys were deleted
    assert_equal {nil} [r get order:1]
    assert_equal {nil} [r get order:2]
    assert_equal "SomeValue" [r get random]

    # Delete a non-existent prefix
    assert_equal 0 [r delprefix nonexisting:]

    # Ensure "random" key still exists
    assert_equal "SomeValue" [r get random]
}
