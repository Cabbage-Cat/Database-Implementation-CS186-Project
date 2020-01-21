package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.index.BPlusTreeMetadata;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * Internal transaction-specific methods, used for implementing parts of the database.
 *
 * The transaction context for the transaction currently running on the current thread
 * can be fetched via TransactionContext::getTransaction; it is only set during the middle
 * of a Transaction call.
 */
public interface TransactionContext extends AutoCloseable {
    Map<Long, List<TransactionContext>> threadTransactions = new ConcurrentHashMap<>();

    /**
     * Fetches the current transaction running on this thread.
     * @return transaction actively running on this thread or null if none
     */
    static TransactionContext getTransaction() {
        long threadId = Thread.currentThread().getId();
        List<TransactionContext> transactions = threadTransactions.get(threadId);
        if (transactions != null && transactions.size() > 0) {
            return transactions.get(transactions.size() - 1);
        }
        return null;
    }

    /**
     * Sets the current transaction running on this thread.
     * @param transaction transaction currently running
     */
    static void setTransaction(TransactionContext transaction) {
        long threadId = Thread.currentThread().getId();
        threadTransactions.putIfAbsent(threadId, new ArrayList<>());
        threadTransactions.get(threadId).add(transaction);
    }

    /**
     * Unsets the current transaction running on this thread.
     */
    static void unsetTransaction() {
        long threadId = Thread.currentThread().getId();
        List<TransactionContext> transactions = threadTransactions.get(threadId);
        if (transactions == null || transactions.size() == 0) {
            throw new IllegalStateException("no transaction to unset");
        }
        transactions.remove(transactions.size() - 1);
    }

    // Status ///////////////////////////////////////////////////////////////

    /**
     * @return transaction number
     */
    long getTransNum();

    int getWorkMemSize();

    @Override
    void close();

    // Temp Tables and Aliasing /////////////////////////////////////////////

    /**
     * Create a temporary table within this transaction.
     *
     * @param schema the table schema
     * @return name of the tempTable
     */
    String createTempTable(Schema schema);

    /**
     * Deletes all temporary tables within this transaction.
     */
    void deleteAllTempTables();

    /**
     * Specify an alias mapping for this transaction. Recursive aliasing is
     * not allowed.
     * @param aliasMap mapping of alias names to original table names
     */
    void setAliasMap(Map<String, String> aliasMap);

    /**
     * Clears any aliases set.
     */
    void clearAliasMap();

    // Indices //////////////////////////////////////////////////////////////

    /**
     * Perform a check to see if the database has an index on this (table,column).
     *
     * @param tableName  the name of the table
     * @param columnName the name of the column
     * @return boolean if the index exists
     */
    boolean indexExists(String tableName, String columnName);

    void updateIndexMetadata(BPlusTreeMetadata metadata);

    // Scans ////////////////////////////////////////////////////////////////

    Iterator<Record> sortedScan(String tableName, String columnName);

    Iterator<Record> sortedScanFrom(String tableName, String columnName, DataBox startValue);

    Iterator<Record> lookupKey(String tableName, String columnName, DataBox key);

    BacktrackingIterator<Record> getRecordIterator(String tableName);

    BacktrackingIterator<Page> getPageIterator(String tableName);

    BacktrackingIterator<Record> getBlockIterator(String tableName, Iterator<Page> block, int maxPages);

    boolean contains(String tableName, String columnName, DataBox key);

    // Record Operations /////////////////////////////////////////////////////

    RecordId addRecord(String tableName, List<DataBox> values);

    RecordId deleteRecord(String tableName, RecordId rid);

    Record getRecord(String tableName, RecordId rid);

    RecordId updateRecord(String tableName, List<DataBox> values, RecordId rid);

    void runUpdateRecordWhere(String tableName, String targetColumnName,
                              UnaryOperator<DataBox> targetValue,
                              String predColumnName, PredicateOperator predOperator, DataBox predValue);

    void runDeleteRecordWhere(String tableName, String predColumnName, PredicateOperator predOperator,
                              DataBox predValue);

    // Table/Schema /////////////////////////////////////////////////////////

    /**
     * @param tableName name of table to get schema of
     * @return schema of table
     */
    Schema getSchema(String tableName);

    /**
     * Same as getSchema, except all column names are fully qualified (tableName.colName).
     *
     * @param tableName name of table to get schema of
     * @return schema of table
     */
    Schema getFullyQualifiedSchema(String tableName);

    Table getTable(String tableName);

    // Statistics ///////////////////////////////////////////////////////////

    /**
     * @param tableName name of table to get stats of
     * @return TableStats object of the table
     */
    TableStats getStats(String tableName);

    /**
     * @param tableName name of table
     * @return number of data pages used by the table
     */
    int getNumDataPages(String tableName);

    /**
     * @param tableName name of table
     * @return number of entries that fit on one page for the table
     */
    int getNumEntriesPerPage(String tableName);

    /**
     * @param tableName name of table
     * @return size of a single row for the table
     */
    int getEntrySize(String tableName);

    /**
     * @param tableName name of table
     * @return number of records in the table
     */
    long getNumRecords(String tableName);

    /**
     * @param tableName name of table
     * @param columnName name of column
     * @return order of B+ tree index on tableName.columnName
     */
    int getTreeOrder(String tableName, String columnName);

    /**
     * @param tableName name of table
     * @param columnName name of column
     * @return height of B+ tree index on tableName.columnName
     */
    int getTreeHeight(String tableName, String columnName);

    // Synchronization //////////////////////////////////////////////////////

    /**
     * prepareBlock acquires the lock backing the condition variable that the transaction
     * waits on. Must be called before block(), and is used to ensure that the unblock() call
     * corresponding to the following block() call cannot be run before the transaction blocks.
     */
    void prepareBlock();

    /**
     * Blocks the transaction (and thread). prepareBlock() must be called first.
     */
    void block();

    /**
     * Unblocks the transaction (and thread running the transaction).
     */
    void unblock();

    /**
     * @return if the transaction is blocked
     */
    boolean getBlocked();
}
