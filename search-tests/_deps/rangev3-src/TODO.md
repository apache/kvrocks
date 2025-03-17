* Add contiguous iterator utilities. How about `is_contiguous_iterator` and `as_contiguous_range`:

    ```
    CPP_template(typename I, typename S)(
        requires RandomAccessIterator<I> &&
            SizedSentinel<S, I> &&
            is_contiguous_iterator<I>())
    subrange<std::add_pointer_t<iter_reference_t<I>>>
    as_contiguous_range(I begin, S end)
    {
        if(begin == end)
            return {nullptr, nullptr};
        else
            return {addressof(*begin), addressof(*begin) + (end - begin)};
    }
    ```
* Longer-term goals:
  - Make `inplace_merge` work with forward iterators
  - Make the sorting algorithms work with forward iterators

* Maybe iterators are not necessarily countable. Is there a relation between
  the ability to be able to subtract two iterators to find the distance, and
  with the existence of a DistanceType associated type? Think of:
  - counted iterators (subtractable regardless of traversal category)
  - repeat_view iterators (*not* subtractable but could be random access otherwise)
  - infinite ranges (only countable with an infinite precision integer which we lack)
