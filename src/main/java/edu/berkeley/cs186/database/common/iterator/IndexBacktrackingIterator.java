package edu.berkeley.cs186.database.common.iterator;

import java.util.NoSuchElementException;

/**
 * Partial implementation of a backtracking iterator over an indexable collection
 * with some indices possibly not matching to a value.
 */
public abstract class IndexBacktrackingIterator<T> implements BacktrackingIterator<T> {
    private int maxIndex;
    private int prevIndex = -1;
    private int nextIndex = -1;
    private int markedIndex = -1;
    private int firstIndex = -1;

    public IndexBacktrackingIterator(int maxIndex) {
        this.maxIndex = maxIndex;
    }

    /**
     * Get the next nonempty index. Initial call uses -1.
     * @return next nonempty index or the max index if no more values.
     */
    protected abstract int getNextNonempty(int currentIndex);

    /**
     * Get the value at the given index. Index will always be a value returned
     * by getNextNonempty.
     * @param index index to get value at
     * @return value at index
     */
    protected abstract T getValue(int index);

    @Override
    public boolean hasNext() {
        if (this.nextIndex == -1) {
            this.nextIndex = this.firstIndex = this.getNextNonempty(-1);
        }
        return this.nextIndex < this.maxIndex;
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        T value = getValue(this.nextIndex);
        this.prevIndex = this.nextIndex;
        this.nextIndex = this.getNextNonempty(this.nextIndex);
        return value;
    }

    @Override
    public void markPrev() {
        // The second condition prevents using mark/reset/mark/reset/.. to
        // move the iterator backwards.
        if (this.nextIndex <= this.firstIndex || this.prevIndex < this.markedIndex) {
            return;
        }
        this.markedIndex = this.prevIndex;
    }

    @Override
    public void markNext() {
        if (this.nextIndex >= this.maxIndex) {
            return;
        }
        this.markedIndex = this.nextIndex;
    }

    @Override
    public void reset() {
        if (this.markedIndex == -1) {
            return;
        }
        this.nextIndex = this.markedIndex;
    }
}
