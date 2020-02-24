package edu.berkeley.cs186.database.memory;

import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class NaiveHashPartition {
    private String tempTableName;
    private TransactionContext transaction;

    /**
     * A class representing a partition used in naive hashing operations. This
     * partition can only hold records from a single relation, as opposed to
     * HashPartition which can hold records from two different relations.
     *
     * @param transaction the transaction context this buffer will be used in
     * @param schema the schema for the type of records to be added to this partition
     */
    public NaiveHashPartition(TransactionContext transaction, Schema schema) {
        this.tempTableName = transaction.createTempTable(schema); // Partitions are backed as a tempTable
        this.transaction = transaction;
    }

    /**
     * Returns the number of pages used to store the records in this partition
     * @return the number of pages used to restore the records in this partition
     */
    public int getNumPages() {
        return this.transaction.getTable(this.tempTableName).getNumDataPages();
    }

    /**
     * Adds a record to this partition
     * @param record the record to be added
     */
    public void addRecord(Record record) {
        this.addRecord(record.getValues());
    }

    /**
     * Adds a record consisting of the values in the input
     * @param values a list of values representing a record to be added
     */
    public void addRecord(List<DataBox> values) {
        transaction.addRecord(this.tempTableName, values);
    }

    /**
     * Add a list of records to the partition
     * @param records records to add to the partition
     */
    public void addRecords(List<Record> records) {
        for (Record record : records) {
            this.addRecord(record);
        }
    }
    /**
     * Returns an iterator over all of the records that were written to this partition
     * @return an iterator over all the records in this partition
     */
    public Iterator<Record> getIterator() {
        return this.transaction.getRecordIterator(this.tempTableName);
    }
}