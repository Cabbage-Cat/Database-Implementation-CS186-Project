package edu.berkeley.cs186.database.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.MarkerRecord;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

class GroupByOperator extends QueryOperator {
    private int groupByColumnIndex;
    private String groupByColumn;
    private TransactionContext transaction;

    /**
     * Create a new GroupByOperator that pulls from source and groups by groupByColumn.
     *
     * @param source the source operator of this operator
     * @param transaction the transaction containing this operator
     * @param groupByColumn the column to group on
     */
    GroupByOperator(QueryOperator source,
                    TransactionContext transaction,
                    String groupByColumn) {
        super(OperatorType.GROUPBY, source);
        Schema sourceSchema = this.getSource().getOutputSchema();
        this.transaction = transaction;
        this.groupByColumn = this.checkSchemaForColumn(sourceSchema, groupByColumn);

        this.groupByColumnIndex = sourceSchema.getFieldNames().indexOf(this.groupByColumn);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Iterator<Record> iterator() {
        return new GroupByIterator();
    }

    @Override
    protected Schema computeSchema() {
        return this.getSource().getOutputSchema();
    }

    @Override
    public String str() {
        return "type: " + this.getType() +
               "\ncolumn: " + this.groupByColumn;
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    @Override
    public TableStats estimateStats() {
        return this.getSource().getStats();
    }

    @Override
    public int estimateIOCost() {
        return this.getSource().getIOCost();
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     * Returns a marker record between the records of different groups, e.g.
     * [group 1 record] [group 1 record] [marker record] [group 2 record] ...
     */
    private class GroupByIterator implements Iterator<Record> {
        private MarkerRecord markerRecord;
        private Map<String, String> hashGroupTempTables;
        private int currCount;
        private Iterator<String> keyIter;
        private Iterator<Record> rIter;

        private GroupByIterator() {
            Iterator<Record> sourceIterator = GroupByOperator.this.getSource().iterator();
            this.markerRecord = MarkerRecord.getMarker();
            this.hashGroupTempTables = new HashMap<>();
            this.currCount = 0;
            this.rIter = null;
            while (sourceIterator.hasNext()) {
                Record record = sourceIterator.next();
                DataBox groupByColumn = record.getValues().get(GroupByOperator.this.groupByColumnIndex);
                String tableName;
                if (!this.hashGroupTempTables.containsKey(groupByColumn.toString())) {
                    tableName = GroupByOperator.this.transaction.createTempTable(
                                    GroupByOperator.this.getSource().getOutputSchema());
                    this.hashGroupTempTables.put(groupByColumn.toString(), tableName);
                } else {
                    tableName = this.hashGroupTempTables.get(groupByColumn.toString());
                }
                GroupByOperator.this.transaction.addRecord(tableName, record.getValues());
            }
            this.keyIter = hashGroupTempTables.keySet().iterator();
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.keyIter.hasNext() || (this.rIter != null && this.rIter.hasNext());
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            while (this.hasNext()) {
                if (this.rIter != null && this.rIter.hasNext()) {
                    return this.rIter.next();
                } else if (this.keyIter.hasNext()) {
                    String key = this.keyIter.next();
                    String tableName = this.hashGroupTempTables.get(key);
                    Iterator<Record> prevIter = this.rIter;
                    try {
                        this.rIter = GroupByOperator.this.transaction.getRecordIterator(tableName);
                    } catch (DatabaseException de) {
                        throw new NoSuchElementException();
                    }
                    if (prevIter != null && ++this.currCount < this.hashGroupTempTables.size()) {
                        return markerRecord;
                    }
                }
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
