package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Table;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            Table t1 = getTransaction().getTable(getLeftTableName());

            SortOperator s1 = new SortOperator(getTransaction(), getLeftTableName(),
                    new LeftRecordComparator());
            String tempT1 = s1.sort();
            SortOperator s2 = new SortOperator(getTransaction(), getRightTableName(), new RightRecordComparator());
            String tempT2 = s2.sort();

            leftIterator = getRecordIterator(tempT1);
            rightIterator = getRecordIterator(tempT2);
            nextRecord = null;
            leftRecord = leftIterator.hasNext()? leftIterator.next() : null;
            rightRecord = rightIterator.hasNext()? rightIterator.next() : null;
            marked = false;
            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        private void fetchNextRecord() {
            if (leftRecord == null) {
                throw new NoSuchElementException("No more Records!");
            }
            nextRecord = null;
            if (rightRecord == null && !marked)
                { throw new NoSuchElementException("No more Records!"); };
            if (rightRecord == null) {
                rightIterator.reset();
                rightRecord = rightIterator.next();
                leftRecord = leftIterator.next();
                if (leftRecord == null) {
                    throw new NoSuchElementException("No more Records!");
                }
                marked = false;
            }
            do {
                if (!marked) {
                    if (leftRecord == null) { throw new NoSuchElementException("Done!"); }
                    DataBox l = leftRecord.getValues().get(getLeftColumnIndex());
                    DataBox r = rightRecord.getValues().get(getRightColumnIndex());
                    while (l.compareTo(r) < 0) {
                        if (!leftIterator.hasNext()) { throw new NoSuchElementException("Done!"); }
                        leftRecord = leftIterator.next();
                        l = leftRecord.getValues().get(getLeftColumnIndex());
                    }
                    while (l.compareTo(r) > 0) {
                        if (!rightIterator.hasNext()) { throw new NoSuchElementException("Done!"); }
                        rightRecord = rightIterator.next();
                        r = rightRecord.getValues().get(getRightColumnIndex());
                    }
                    rightIterator.markPrev();
                    marked = true;
                }

                DataBox l = leftRecord.getValues().get(getLeftColumnIndex());
                DataBox r = rightRecord.getValues().get(getRightColumnIndex());
                if (l.equals(r)) {
                    List<DataBox> values = new ArrayList<>(leftRecord.getValues());
                    values.addAll(new ArrayList<>(rightRecord.getValues()));
                    nextRecord = new Record(values);
                    rightRecord = rightIterator.hasNext()? rightIterator.next() : null;
                } else {
                    rightIterator.reset();
                    rightRecord = rightIterator.next();
                    leftRecord = leftIterator.hasNext()? leftIterator.next() : null;
                    marked = false;
                }
            } while (nextRecord == null);
        }
        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement
            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement
            if (nextRecord == null) { throw new NoSuchElementException(); }
            Record retn = nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return retn;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
