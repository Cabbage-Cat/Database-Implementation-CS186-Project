package edu.berkeley.cs186.database.query;

import java.util.Iterator;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

class SequentialScanOperator extends QueryOperator {
    private TransactionContext transaction;
    private String tableName;

    /**
     * Creates a new SequentialScanOperator that provides an iterator on all tuples in a table.
     *
     * NOTE: Sequential scans don't take a source operator because they must always be at the bottom
     * of the DAG.
     *
     * @param transaction
     * @param tableName
     */
    SequentialScanOperator(TransactionContext transaction,
                           String tableName) {
        this(OperatorType.SEQSCAN, transaction, tableName);
    }

    protected SequentialScanOperator(OperatorType type,
                                     TransactionContext transaction,
                                     String tableName) {
        super(type);
        this.transaction = transaction;
        this.tableName = tableName;
        this.setOutputSchema(this.computeSchema());

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public String getTableName() {
        return this.tableName;
    }

    @Override
    public boolean isSequentialScan() {
        return true;
    }

    @Override
    public Iterator<Record> iterator() {
        return this.transaction.getRecordIterator(tableName);
    }

    @Override
    public Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    @Override
    public String str() {
        return "type: " + this.getType() +
               "\ntable: " + this.tableName;
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    @Override
    public TableStats estimateStats() {
        try {
            return this.transaction.getStats(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    @Override
    public int estimateIOCost() {
        try {
            return this.transaction.getNumDataPages(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }
}
