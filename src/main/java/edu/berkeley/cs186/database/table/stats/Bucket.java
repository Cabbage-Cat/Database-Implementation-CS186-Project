package edu.berkeley.cs186.database.table.stats;

import java.util.Objects;
import java.util.HashSet;

/**
 * A histogram bucket. There are two types of buckets:
 *
 *   1. A bounded bucket `new Bucket<T>(start, stop)` represents a count of
 *      values in the range [start, stop).
 *   2. An unbounded bucket `new Bucket<T>(start)` represents a count of values
 *      in the range [start, infinity).
 */
public class Bucket<T> {
    // If end is not null, then this bucket corresponds to range [start, stop).
    // If end is null, then this bucket corresponds to the single value start.
    private T start;
    private T end;
    private int count;
    private int distinctCount;

    //todo fix later
    private HashSet<Float> dictionary;

    public Bucket(T start) {
        this(start, null);
    }

    public Bucket(T start, T end) {
        this.start = start;
        this.end = end;
        this.count = 0;

        this.distinctCount = 0;
        this.dictionary = new HashSet<>();
    }

    public T getStart() {
        return start;
    }

    public T getEnd() {
        return end;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setDistinctCount(int count) {
        this.distinctCount = count;
        dictionary = new HashSet<>();
    }

    public int getDistinctCount() {
        return this.distinctCount + dictionary.size();
    }

    public void increment(float val) {
        count ++;
        dictionary.add(val);
    }

    public void decrement(float val) {
        count --;
        dictionary.remove(val);
    }

    @Override
    public String toString() {
        return String.format("[%s,%s):%d", start, end, count);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Bucket<?>)) {
            return false;
        }
        Bucket<?> b = (Bucket<?>) o;
        boolean startEquals = start.equals(b.start);
        boolean endEquals = (end == null && b.end == null) ||
                            (end != null && b.end != null && end.equals(b.end));
        boolean countEquals = count == b.count;
        return startEquals && endEquals && countEquals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, count);
    }
}
