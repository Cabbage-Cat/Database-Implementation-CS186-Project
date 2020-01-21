package edu.berkeley.cs186.database.common.iterator;

/**
 * Iterator that concatenates a bunch of backtracking iterables together.
 */
public class ConcatBacktrackingIterator<T> implements BacktrackingIterator<T> {
    private BacktrackingIterator<BacktrackingIterable<T>> outerIterator;

    private BacktrackingIterator<T> prevItemIterator;
    private BacktrackingIterator<T> nextItemIterator;
    private BacktrackingIterator<T> markItemIterator;

    private boolean markMidIterator;

    public ConcatBacktrackingIterator(BacktrackingIterable<BacktrackingIterable<T>> outerIterable) {
        this(outerIterable.iterator());
    }

    public ConcatBacktrackingIterator(BacktrackingIterator<BacktrackingIterable<T>> outerIterator) {
        this.outerIterator = outerIterator;
        this.prevItemIterator = null;
        this.nextItemIterator = new EmptyBacktrackingIterator<>();
        this.markItemIterator = null;
        this.markMidIterator = false;

        this.moveNextToNonempty();
    }

    private void moveNextToNonempty() {
        while (!this.nextItemIterator.hasNext() && this.outerIterator.hasNext()) {
            this.nextItemIterator = this.outerIterator.next().iterator();
        }
    }

    @Override
    public boolean hasNext() {
        return this.nextItemIterator.hasNext();
    }

    @Override
    public T next() {
        T item = this.nextItemIterator.next();
        this.prevItemIterator = this.nextItemIterator;
        this.moveNextToNonempty();
        return item;
    }

    @Override
    public void markPrev() {
        if (this.prevItemIterator == null) {
            return;
        }
        this.markItemIterator = this.prevItemIterator;
        this.markItemIterator.markPrev();
        this.outerIterator.markPrev();
        this.markMidIterator = (this.prevItemIterator == this.nextItemIterator);
    }

    @Override
    public void markNext() {
        this.markItemIterator = this.nextItemIterator;
        this.markItemIterator.markNext();
        this.outerIterator.markNext();
        this.markMidIterator = false;
    }

    @Override
    public void reset() {
        if (this.markItemIterator == null) {
            return;
        }
        this.prevItemIterator = null;
        this.nextItemIterator = this.markItemIterator;
        this.nextItemIterator.reset();

        this.outerIterator.reset();
        // either next = prev at time of markPrev, or next = nonempty after prev
        // we want outerIterator.next() to return iterable after prev; it returns next
        // skipping the empty iterables after prev is also ok -- since they would be skipped anyways
        if (this.markMidIterator) {
            this.outerIterator.next();
        }
    }
}