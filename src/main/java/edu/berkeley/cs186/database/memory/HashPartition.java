package edu.berkeley.cs186.database.memory;

import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class HashPartition {
    private String tempLeftTableName;
    private String tempRightTableName;
    private TransactionContext transaction;

    /**
     * A class representing a partition used in hashing operations for grace hash
     * join. This partition supports adding and iterating over records for two
     * different relations.
     *
     * @param transaction the transaction context this buffer will be used in
     * @param leftSchema the schema for the type of records in the left table to be added to this partition
     * @param rightSchema the schema for the type of records in the right table to be added to this partition
     */
    public HashPartition(TransactionContext transaction, Schema leftSchema, Schema rightSchema) {
        this.tempLeftTableName = transaction.createTempTable(leftSchema);
        this.tempRightTableName = transaction.createTempTable(rightSchema);
        this.transaction = transaction;
    }

    /**
     * Adds a record from the left relation to this partition
     * @param leftRecord the record to be added
     */
    public void addLeftRecord(Record leftRecord) {
        this.addLeftRecord(leftRecord.getValues());
    }

    /**
     * Adds a record from the right relation to this partition
     * @param rightRecord the record to be added
     */
    public void addRightRecord(Record rightRecord) {
        this.addRightRecord(rightRecord.getValues());
    }

    /**
     * Returns the number of pages used to store records from the left relation
     * in this partition
     * @return the number of pages used to store records from the left relation
     */
    public int getNumLeftPages() {
        return this.transaction.getTable(this.tempLeftTableName).getNumDataPages();
    }

    /**
     * Returns the number of pages used to store records from the right relation
     * in this partition
     * @return the number of pages used to store records from the right relation
     */
    public int getNumRightPages() {
        return this.transaction.getTable(this.tempRightTableName).getNumDataPages();
    }

    /**
     * Adds a record from the left relation consisting of the values in the input
     * @param values a list of values representing a record to be added
     */
    public void addLeftRecord(List<DataBox> leftValues) {
        transaction.addRecord(this.tempLeftTableName, leftValues);
    }

    /**
     * Adds a record from the right relation consisting of the values in the input
     * @param values a list of values representing a record to be added
     */
    public void addRightRecord(List<DataBox> rightValues) {
        transaction.addRecord(this.tempRightTableName, rightValues);
    }

    /**
     * Add a list of records from the left relation to the partition
     * @param records records to add to the partition
     */
    public void addLeftRecords(List<Record> records) {
        for (Record record : records) {
            this.addLeftRecord(record);
        }
    }

    /**
     * Add a list of records from the right relation to the partition
     * @param records records to add to the partition
     */
    public void addRightRecords(List<Record> records) {
        for (Record record : records) {
            this.addRightRecord(record);
        }
    }

    /**
     * Returns an iterator over all of the records from the left relation
     * that were written to this partition
     * @return an iterator over all the records in this partition
     */
    public Iterator<Record> getLeftIterator() {
        return this.transaction.getRecordIterator(this.tempLeftTableName);
    }

    /**
     * Returns an iterator over all of the records from the right relation
     * that were written to this partition
     * @return an iterator over all the records in this partition
     */
    public Iterator<Record> getRightIterator() {
        return this.transaction.getRecordIterator(this.tempRightTableName);
    }
}