package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.index.BPlusTree;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.Iterator;
import java.util.NoSuchElementException;

class IndexScanOperator extends QueryOperator {
    private TransactionContext transaction;
    private String tableName;
    private String columnName;
    private PredicateOperator predicate;
    private DataBox value;

    private int columnIndex;

    /**
     * An index scan operator.
     *
     * @param transaction the transaction containing this operator
     * @param tableName the table to iterate over
     * @param columnName the name of the column the index is on
     */
    IndexScanOperator(TransactionContext transaction,
                      String tableName,
                      String columnName,
                      PredicateOperator predicate,
                      DataBox value) {
        super(OperatorType.INDEXSCAN);
        this.tableName = tableName;
        this.transaction = transaction;
        this.columnName = columnName;
        this.predicate = predicate;
        this.value = value;
        this.setOutputSchema(this.computeSchema());
        columnName = this.checkSchemaForColumn(this.getOutputSchema(), columnName);
        this.columnIndex = this.getOutputSchema().getFieldNames().indexOf(columnName);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public boolean isIndexScan() {
        return true;
    }

    @Override
    public String str() {
        return "type: " + this.getType() +
               "\ntable: " + this.tableName +
               "\ncolumn: " + this.columnName +
               "\noperator: " + this.predicate +
               "\nvalue: " + this.value;
    }

    /**
     * Returns the column name that the index scan is on
     *
     * @return columnName
     */
    public String getColumnName() {
        return this.columnName;
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    @Override
    public TableStats estimateStats() {
        TableStats stats;

        try {
            stats = this.transaction.getStats(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }

        return stats.copyWithPredicate(this.columnIndex,
                                       this.predicate,
                                       this.value);
    }

    /**
     * Estimates the IO cost of executing this query operator.
     * @return estimate IO cost
     */
    @Override
    public int estimateIOCost() {
        int height = transaction.getTreeHeight(tableName, columnName);
        int order = transaction.getTreeOrder(tableName, columnName);
        TableStats tableStats = transaction.getStats(tableName);

        int count = tableStats.getHistograms().get(columnIndex).copyWithPredicate(predicate,
                    value).getCount();
        // 2 * order entries/leaf node, but leaf nodes are 50-100% full; we use a fill factor of
        // 75% as a rough estimate
        return (int) (height + Math.ceil(count / (1.5 * order)) + count);
    }

    @Override
    public Iterator<Record> iterator() {
        return new IndexScanIterator();
    }

    @Override
    public Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class IndexScanIterator implements Iterator<Record> {
        private Iterator<Record> sourceIterator;
        private Record nextRecord;

        private IndexScanIterator() {
            this.nextRecord = null;
            if (IndexScanOperator.this.predicate == PredicateOperator.EQUALS) {
                this.sourceIterator = IndexScanOperator.this.transaction.lookupKey(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName,
                                          IndexScanOperator.this.value);
            } else if (IndexScanOperator.this.predicate == PredicateOperator.LESS_THAN ||
                       IndexScanOperator.this.predicate == PredicateOperator.LESS_THAN_EQUALS) {
                this.sourceIterator = IndexScanOperator.this.transaction.sortedScan(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName);
            } else if (IndexScanOperator.this.predicate == PredicateOperator.GREATER_THAN) {
                this.sourceIterator = IndexScanOperator.this.transaction.sortedScanFrom(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName,
                                          IndexScanOperator.this.value);
                while (this.sourceIterator.hasNext()) {
                    Record r = this.sourceIterator.next();

                    if (r.getValues().get(IndexScanOperator.this.columnIndex)
                            .compareTo(IndexScanOperator.this.value) > 0) {
                        this.nextRecord = r;
                        break;
                    }
                }
            } else if (IndexScanOperator.this.predicate == PredicateOperator.GREATER_THAN_EQUALS) {
                this.sourceIterator = IndexScanOperator.this.transaction.sortedScanFrom(
                                          IndexScanOperator.this.tableName,
                                          IndexScanOperator.this.columnName,
                                          IndexScanOperator.this.value);
            }
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord != null) {
                return true;
            }
            if (IndexScanOperator.this.predicate == PredicateOperator.LESS_THAN) {
                if (this.sourceIterator.hasNext()) {
                    Record r = this.sourceIterator.next();
                    if (r.getValues().get(IndexScanOperator.this.columnIndex)
                            .compareTo(IndexScanOperator.this.value) >= 0) {
                        return false;
                    }
                    this.nextRecord = r;
                    return true;
                }
                return false;
            } else if (IndexScanOperator.this.predicate == PredicateOperator.LESS_THAN_EQUALS) {
                if (this.sourceIterator.hasNext()) {
                    Record r = this.sourceIterator.next();
                    if (r.getValues().get(IndexScanOperator.this.columnIndex)
                            .compareTo(IndexScanOperator.this.value) > 0) {
                        return false;
                    }
                    this.nextRecord = r;
                    return true;
                }
                return false;
            }
            if (this.sourceIterator.hasNext()) {
                this.nextRecord = this.sourceIterator.next();
                return true;
            }
            return false;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (this.hasNext()) {
                Record r = this.nextRecord;
                this.nextRecord = null;
                return r;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
