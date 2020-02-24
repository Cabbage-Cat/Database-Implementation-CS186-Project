package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.NaiveHashPartition;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.*;

public class NaiveHashJoin {
    private Iterator<Record> leftRelationIterator;
    private BacktrackingIterator<Record> rightRelationIterator;
    private int numBuffers;
    private int leftColumnIndex;
    private int rightColumnIndex;
    private TransactionContext transactionContext;
    private Schema leftSchema;

    /**
     * This class represents a naive hash join on two relations with records
     * contained in leftRelationIterator and rightRelationIterator. To join the
     * two relations the class will attempt to partition the left records
     * and then probe with the right records. It will fail if any of the
     * partitions are larger than the B-2 pages of memory needed to construct
     * the in memory hash table by throwing an InvalidArgumentException.
     */
    public NaiveHashJoin(Iterator<Record> leftRelationIterator,
                         BacktrackingIterator<Record> rightRelationIterator, int leftColumnIndex, int rightColumnIndex,
                         TransactionContext transactionContext, Schema leftSchema) {
        this.leftRelationIterator = leftRelationIterator;
        this.rightRelationIterator = rightRelationIterator;
        this.numBuffers = transactionContext.getWorkMemSize();
        this.leftColumnIndex = leftColumnIndex;
        this.rightColumnIndex = rightColumnIndex;
        this.transactionContext = transactionContext;
        this.leftSchema = leftSchema;

        // We'll be traversing the right relation multiple times
        rightRelationIterator.next();
        rightRelationIterator.markPrev();
        rightRelationIterator.reset();
    }

    /**
     * Partition stage. For every record in the left record iterator, hashes the
     * value we are joining on and adds that record to the correct partition.
     */
    private void partition(NaiveHashPartition[] partitions, Iterator<Record> leftRecords) {
        while (leftRecords.hasNext()) {
            // Partition left records on the chosen column
            Record record = leftRecords.next();
            DataBox columnValue = record.getValues().get(this.getLeftColumnIndex());
            int hash = columnValue.hashCode();
            int partitionNum = (hash % partitions.length); // Modulo to get which partition to use
            if (partitionNum < 0) {
                partitionNum += partitions.length;    // Hash might be negative
            }
            partitions[partitionNum].addRecord(record);
        }
    }

    /**
     * Builds the hash table using leftRecords and probes it with the records
     * in rightRecords. Joins the matching records and returns them as the
     * joinedRecords list.
     *
     * @param partition a partition
     * @param rightRecords An iterator of records from the right relation
     */
    private List<Record> buildAndProbe(NaiveHashPartition partition, Iterator<Record> rightRecords) {
        if (partition.getNumPages() > this.numBuffers - 2) {
            throw new IllegalArgumentException(
                "The records in this partition cannot fit in B-2 pages of memory."
            );
        }
        Iterator<Record> leftRecords = partition.getIterator();

        // Accumulate all the records from probing this partition here
        ArrayList<Record> joinedRecords = new ArrayList<>();

        // Our hash table to build on. The list contains all the records in the left records that hash to the same key
        Map<DataBox, List<Record>> hashTable = new HashMap<>();

        // Building stage
        while (leftRecords.hasNext()) {
            Record leftRecord = leftRecords.next();
            DataBox leftJoinValue = leftRecord.getValues().get(this.getLeftColumnIndex());

            if (!hashTable.containsKey(leftJoinValue)) {
                hashTable.put(leftJoinValue, new ArrayList<>());
            }
            hashTable.get(leftJoinValue).add(leftRecord);
        }

        // Probing stage
        while (rightRecords.hasNext()) {
            Record rightRecord = rightRecords.next();
            DataBox rightJoinValue = rightRecord.getValues().get(getRightColumnIndex());

            if (hashTable.containsKey(rightJoinValue)) {
                // We have to join the right record with EACH left record that matched the key
                for (Record lRecord : hashTable.get(rightJoinValue)) {
                    Record joinedRecord = joinRecords(lRecord, rightRecord);
                    joinedRecords.add(joinedRecord);
                }
            }
        }
        return joinedRecords;
    }

    /**
     * Runs the Hash Join algorithm! First, run the partitioning stage to create an
     * array of Hash Partitions Then, build and probe with each hash partitions
     * records Finally, return our list of joined records.
     *
     * @return A list of joined records
     */
    public List<Record> run() {
        ArrayList<Record> joinedRecords = new ArrayList<Record>();
        NaiveHashPartition partitions[] = createPartitions();
        this.partition(partitions, this.leftRelationIterator);
        for (NaiveHashPartition partition : partitions) {
            joinedRecords.addAll(buildAndProbe(partition, this.rightRelationIterator));
            this.rightRelationIterator.reset(); // We need to reset this every time!
        }
        return joinedRecords;
    }

    /**
     * Create an appropriate number of NaiveHashPartitions and return them
     * as an array.
     * @return an array of NaiveHashPartitions
     */
    private NaiveHashPartition[] createPartitions() {
        int usableBuffers = this.numBuffers - 1;
        NaiveHashPartition partitions[] = new NaiveHashPartition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            partitions[i] = createPartition();
        }
        return partitions;
    }

    //////////////////////////////////////// HELPER METHODS //////////////////////////////////////////////////////////////
    /**
     * Helper method to create a joined record from a record of the left relation
     * and a record of the right relation.
     *
     * @param leftRecord  Record from the left relation
     * @param rightRecord Record from the right relation
     * @return joined record
     */
    private Record joinRecords(Record leftRecord, Record rightRecord) {
        List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
        leftValues.addAll(rightValues);
        return new Record(leftValues);
    }

    /**
     * Creates a hash partition
     *
     * @return a new hash partition
     */
    private NaiveHashPartition createPartition() {
        return new NaiveHashPartition(transactionContext, this.leftSchema);
    }

    /**
     * Get left column index we are joining on
     *
     * @return left column index
     */
    public int getLeftColumnIndex() {
        return this.leftColumnIndex;
    }

    /**
     * Get right column index we are joining on
     *
     * @return right column index
     */
    public int getRightColumnIndex() {
        return this.rightColumnIndex;
    }
}
