package com.getindata.tutorial.base.input.utils;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.IntStream;

public class MergedIterator<T> implements Iterator<T> {

    private final List<Iterator<T>> iterators;
    private final Map<Integer, T> values = new HashMap<>();
    private final Comparator<T> comparator;

    public MergedIterator(List<Iterator<T>> it, Comparator<T> comparator) {
        this.comparator = comparator;
        iterators = new ArrayList<>(it);
        IntStream.range(0, iterators.size()).forEach(
                this::updateValue
        );
    }

    private void updateValue(int i) {
        final Iterator<T> iterator = iterators.get(i);
        if (iterator.hasNext()) {
            values.put(i, iterator.next());
        } else {
            values.remove(i);
        }
    }

    // NOT THREAD SAFE
    @Override
    public boolean hasNext() {
        // if you use .hasNext() on the backing iterators, you'll never get the last element
        return !values.isEmpty();
    }

    // NOT THREAD SAFE
    @Override
    public T next() {

        // returning null instead of throwing exceptions makes it easier to use parameterized tests
        if (!hasNext()) {
            return null;
        }

        final Entry<Integer, T> min = values.entrySet().stream()
                .min(Comparator.comparing(Entry::getValue, comparator)).get();
        final Integer key = min.getKey();
        final T value = min.getValue();

        updateValue(key);

        return value;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported.");
    }

}
