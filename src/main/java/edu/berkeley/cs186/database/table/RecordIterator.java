package edu.berkeley.cs186.database.table;

import java.util.Iterator;

import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.DatabaseException;

/**
 * A RecordIterator wraps an Iterator<RecordId> to form an BacktrackingIterator<Record>.
 * For example,
 *
 *   Iterator<RecordId> ridIterator = t.ridIterator();
 *   RecordIterator recordIterator = new RecordIterator(t, ridIterator);
 *   recordIterator.next(); // equivalent to t.getRecord(ridIterator.next())
 *   recordIterator.next(); // equivalent to t.getRecord(ridIterator.next())
 *   recordIterator.next(); // equivalent to t.getRecord(ridIterator.next())
 */
public class RecordIterator implements BacktrackingIterator<Record> {
    private Iterator<RecordId> ridIter;
    private Table table;

    public RecordIterator(Table table, Iterator<RecordId> ridIter) {
        this.ridIter = ridIter;
        this.table = table;
    }

    @Override
    public boolean hasNext() {
        return ridIter.hasNext();
    }

    @Override
    public Record next() {
        try {
            return table.getRecord(ridIter.next());
        } catch (DatabaseException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void markPrev() {
        if (ridIter instanceof BacktrackingIterator) {
            ((BacktrackingIterator) ridIter).markPrev();
        } else {
            throw new UnsupportedOperationException("Cannot markPrev using underlying iterator");
        }
    }

    @Override
    public void markNext() {
        if (ridIter instanceof BacktrackingIterator) {
            ((BacktrackingIterator) ridIter).markNext();
        } else {
            throw new UnsupportedOperationException("Cannot markNext using underlying iterator");
        }
    }

    @Override
    public void reset() {
        if (ridIter instanceof BacktrackingIterator) {
            ((BacktrackingIterator) ridIter).reset();
        } else {
            throw new UnsupportedOperationException("Cannot reset using underlying iterator");
        }
    }
}

