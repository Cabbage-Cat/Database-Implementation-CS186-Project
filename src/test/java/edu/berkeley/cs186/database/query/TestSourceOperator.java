package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class TestSourceOperator extends QueryOperator {
    private List<Record> recordList;
    private Schema setSchema;
    private int numRecords;

    public TestSourceOperator() {
        super(OperatorType.SEQSCAN, null);
        this.recordList = null;
        this.setSchema = null;
        this.numRecords = 100;

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public TestSourceOperator(List<Record> recordIterator, Schema schema) {
        super(OperatorType.SEQSCAN);

        this.recordList = recordIterator;
        this.setOutputSchema(schema);
        this.setSchema = schema;
        this.numRecords = 100;

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public boolean isSequentialScan() {
        return false;
    }

    public TestSourceOperator(int numRecords) {
        super(OperatorType.SEQSCAN, null);
        this.recordList = null;
        this.setSchema = null;
        this.numRecords = numRecords;

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> execute() {
        if (this.recordList == null) {
            ArrayList<Record> recordList = new ArrayList<Record>();
            for (int i = 0; i < this.numRecords; i++) {
                recordList.add(TestUtils.createRecordWithAllTypes());
            }

            return recordList.iterator();
        }
        return this.recordList.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return this.execute();
    }

    @Override
    protected Schema computeSchema() {
        if (this.setSchema == null) {
            return TestUtils.createSchemaWithAllTypes();
        }
        return this.setSchema;
    }

    @Override
    public TableStats estimateStats() {
        Schema schema = this.computeSchema();
        return new TableStats(schema, Table.computeNumRecordsPerPage(BufferManager.EFFECTIVE_PAGE_SIZE,
                              schema));
    }

    @Override
    public int estimateIOCost() {
        return 1;
    }
}
