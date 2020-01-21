package edu.berkeley.cs186.database.concurrency;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import edu.berkeley.cs186.database.AbstractTransactionContext;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.index.BPlusTreeMetadata;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.*;
import edu.berkeley.cs186.database.table.stats.TableStats;

/**
 * A dummy transaction class that only supports checking/setting active/blocked
 * status. Used for testing locking code without requiring an instance
 * of the database.
 */
public class DummyTransactionContext extends AbstractTransactionContext {
    private long tNum;
    private LoggingLockManager lockManager;
    private boolean active = true;

    public DummyTransactionContext(LoggingLockManager lockManager, long tNum) {
        this.lockManager = lockManager;
        this.tNum = tNum;
    }

    @Override
    public long getTransNum() {
        return this.tNum;
    }

    @Override
    public String createTempTable(Schema schema) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void deleteAllTempTables() {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void setAliasMap(Map<String, String> aliasMap) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void clearAliasMap() {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public boolean indexExists(String tableName, String columnName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Iterator<Record> sortedScan(String tableName, String columnName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Iterator<Record> sortedScanFrom(String tableName, String columnName,
                                           DataBox startValue) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Iterator<Record> lookupKey(String tableName, String columnName,
                                      DataBox key) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public boolean contains(String tableName, String columnName, DataBox key) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public RecordId addRecord(String tableName, List<DataBox> values) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public int getWorkMemSize() {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public RecordId deleteRecord(String tableName, RecordId rid)  {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Record getRecord(String tableName, RecordId rid) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public RecordIterator getRecordIterator(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public RecordId updateRecord(String tableName, List<DataBox> values,
                                 RecordId rid)  {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public BacktrackingIterator<Page> getPageIterator(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public BacktrackingIterator<Record> getBlockIterator(String tableName,
            Page[] block) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public BacktrackingIterator<Record> getBlockIterator(String tableName,
            BacktrackingIterator<Page> block) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public BacktrackingIterator<Record> getBlockIterator(String tableName, Iterator<Page> block,
            int maxPages) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void runUpdateRecordWhere(String tableName, String targetColumnName,
                                     UnaryOperator<DataBox> targetValue,
                                     String predColumnName, PredicateOperator predOperator, DataBox predValue) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void runDeleteRecordWhere(String tableName, String predColumnName,
                                     PredicateOperator predOperator, DataBox predValue) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public TableStats getStats(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public int getNumDataPages(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public int getNumEntriesPerPage(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public byte[] readPageHeader(String tableName, Page p) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public int getPageHeaderSize(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public int getEntrySize(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public long getNumRecords(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public int getTreeOrder(String tableName, String columnName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public int getTreeHeight(String tableName, String columnName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Schema getSchema(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Schema getFullyQualifiedSchema(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public Table getTable(String tableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    public void deleteTempTable(String tempTableName) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void updateIndexMetadata(BPlusTreeMetadata metadata) {
        throw new UnsupportedOperationException("dummy transaction cannot do this");
    }

    @Override
    public void block() {
        lockManager.emit("block " + tNum);
        super.block();
    }

    @Override
    public void unblock() {
        lockManager.emit("unblock " + tNum);
        super.unblock();
        Thread.yield();
    }

    @Override
    public void close() {}

    @Override
    public String toString() {
        return "Dummy Transaction #" + tNum;
    }
}

