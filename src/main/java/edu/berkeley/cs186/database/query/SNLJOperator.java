package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

public class SNLJOperator extends JoinOperator {
    public SNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource,
              rightSource,
              leftColumnName,
              rightColumnName,
              transaction,
              JoinType.SNLJ);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        int numLeftRecords = getLeftSource().getStats().getNumRecords();

        int numRightPages = getRightSource().getStats().getNumPages();
        int numLeftPages = getLeftSource().getStats().getNumPages();

        return numLeftRecords * numRightPages + numLeftPages;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     * Note that the left table is the "outer" loop and the right table is the "inner" loop.
     */
    private class SNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Record> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Record> rightIterator;
        // The current record on the left page
        private Record leftRecord;
        // The current record on the right page
        private Record rightRecord;
        // The next record to return
        private Record nextRecord;

        public SNLJIterator() {
            super();
            this.rightIterator = SNLJOperator.this.getRecordIterator(this.getRightTableName());
            this.leftIterator = SNLJOperator.this.getRecordIterator(this.getLeftTableName());

            this.nextRecord = null;

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            // We mark the first record so we can reset to it when we advance the left record.
            if (rightRecord != null) {
                rightIterator.markPrev();
            } else { return; }

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * After this method is called, rightRecord will contain the first record in the rightSource.
         * There is always a first record. If there were no first records (empty rightSource)
         * then the code would not have made it this far. See line 66.
         */
        private void resetRightRecord() {
            this.rightIterator.reset();
            assert(rightIterator.hasNext());
            rightRecord = rightIterator.next();
        }

        /**
         * Advances the left record
         *
         * The thrown exception means we're done: there is no next record
         * It causes this.fetchNextRecord (the caller) to hand control to its caller.
         */
        private void nextLeftRecord() {
            if (!leftIterator.hasNext()) { throw new NoSuchElementException("All Done!"); }
            leftRecord = leftIterator.next();
        }

        /**
         * Pre-fetches what will be the next record, and puts it in this.nextRecord.
         * Pre-fetching simplifies the logic of this.hasNext() and this.next()
         */
        private void fetchNextRecord() {
            if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch"); }
            this.nextRecord = null;
            do {
                if (this.rightRecord != null) {
                    // We have both a left record and a right record, so we compare the join values
                    // and combine the rows if there is a match.
                    DataBox leftJoinValue = this.leftRecord.getValues().get(SNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(SNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                    }
                    this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                } else {
                    nextLeftRecord();
                    resetRightRecord();
                }
            } while (!hasNext());
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}

