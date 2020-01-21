package edu.berkeley.cs186.database.common.iterator;

import java.util.Iterator;

public interface BacktrackingIterator<T> extends Iterator<T> {
    /**
     * markPrev() marks the last returned value of the iterator, which is the last
     * returned value of next().
     *
     * Calling markPrev() on an iterator that has not yielded a record yet,
     * or that has not yielded a record since the last reset() call does nothing.
     */
    void markPrev();

    /**
     * markNext() marks the next returned value of the iterator, which is the
     * value returned by the next call of next().
     *
     * Calling markNext() on an iterator that has no records left,
     * or that has not yielded a record since the last reset() call does nothing.
     */
    void markNext();

    /**
     * reset() resets the iterator to the last marked location.
     *
     * The next next() call should return the value that was marked - if markPrev()
     * was used, this is the value returned by the next() call before markPrev(), and if
     * markNext() was used, this is the value returned by the next() call after markNext().
     * If neither mark methods were called, reset() does nothing. You may reset() to the same
     * point as many times as desired, as long as neither mark method is called again.
     */
    void reset();
}

