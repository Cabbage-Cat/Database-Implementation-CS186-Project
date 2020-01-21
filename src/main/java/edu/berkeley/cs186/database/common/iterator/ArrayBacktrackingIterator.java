package edu.berkeley.cs186.database.common.iterator;

/**
 * Backtracking iterator over an array.
 */
public class ArrayBacktrackingIterator<T> extends IndexBacktrackingIterator<T> {
    protected T[] array;

    public ArrayBacktrackingIterator(T[] array) {
        super(array.length);
        this.array = array;
    }

    @Override
    protected int getNextNonempty(int currentIndex) {
        return currentIndex + 1;
    }

    @Override
    protected T getValue(int index) {
        return this.array[index];
    }
}

