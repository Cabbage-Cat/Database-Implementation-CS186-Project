package edu.berkeley.cs186.database.common.iterator;

import java.util.NoSuchElementException;

/**
 * Empty backtracking iterator.
 */
public class EmptyBacktrackingIterator<T> implements BacktrackingIterator<T> {
    @Override public boolean hasNext() { return false; }
    @Override public T next() { throw new NoSuchElementException(); }
    @Override public void markPrev() {}
    @Override public void markNext() {}
    @Override public void reset() {}
}
