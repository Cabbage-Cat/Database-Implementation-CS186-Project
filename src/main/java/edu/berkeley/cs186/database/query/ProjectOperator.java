package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.databox.TypeId;
import edu.berkeley.cs186.database.table.MarkerRecord;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

class ProjectOperator extends QueryOperator {
    private List<String> columns;
    private List<Integer> indices;
    private boolean hasCount;
    private int averageColumnIndex;
    private int sumColumnIndex;
    private boolean hasAggregate;
    private int countValue;
    private double sumValue;
    private double averageSumValue;
    private int averageCountValue;
    private String sumColumn;
    private String averageColumn;
    private boolean sumIsFloat;

    /**
     * Creates a new ProjectOperator that reads tuples from source and filters out columns. Optionally
     * computers an aggregate if it is specified.
     *
     * @param source
     * @param columns
     * @param count
     * @param averageColumn
     * @param sumColumn
     */
    ProjectOperator(QueryOperator source,
                    List<String> columns,
                    boolean count,
                    String averageColumn,
                    String sumColumn) {
        super(OperatorType.PROJECT);
        this.columns = columns;
        this.indices = new ArrayList<>();
        this.countValue = 0;
        this.sumValue = 0;
        this.averageCountValue = 0;
        this.averageSumValue = 0;
        this.averageColumnIndex = -1;
        this.sumColumnIndex = -1;
        this.sumColumn = sumColumn;
        this.averageColumn = averageColumn;
        this.hasCount = count;
        this.hasAggregate = this.hasCount || averageColumn != null || sumColumn != null;

        // NOTE: Don't need to explicitly set the output schema because setting the source recomputes
        // the schema for the query optimization case.
        this.setSource(source);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public boolean isProject() {
        return true;
    }

    @Override
    protected Schema computeSchema() {
        // check to make sure that the source operator is giving us columns that we project
        Schema sourceSchema = this.getSource().getOutputSchema();
        List<String> sourceColumnNames = new ArrayList<>(sourceSchema.getFieldNames());
        List<Type> sourceColumnTypes = new ArrayList<>(sourceSchema.getFieldTypes());
        List<Type> columnTypes = new ArrayList<>();
        for (String columnName : this.columns) {
            columnName = this.checkSchemaForColumn(sourceSchema, columnName);
            int sourceColumnIndex = sourceColumnNames.indexOf(columnName);
            columnTypes.add(sourceColumnTypes.get(sourceColumnIndex));
            this.indices.add(sourceColumnIndex);
        }
        if (this.sumColumn != null) {
            this.sumColumn = this.checkSchemaForColumn(sourceSchema, this.sumColumn);
            this.sumColumnIndex = sourceColumnNames.indexOf(this.sumColumn);
            if (!(sourceColumnTypes.get(this.sumColumnIndex).getTypeId() == TypeId.INT) &&
                    !(sourceColumnTypes.get(this.sumColumnIndex).getTypeId() == TypeId.FLOAT)) {
                throw new QueryPlanException("Cannot compute sum over a non-integer column: " + this.sumColumn +
                                             ".");
            }
        }
        if (this.averageColumn != null) {
            this.averageColumn = this.checkSchemaForColumn(sourceSchema, this.averageColumn);
            this.averageColumnIndex = sourceColumnNames.indexOf(this.averageColumn);
            if (!(sourceColumnTypes.get(this.averageColumnIndex).getTypeId() == TypeId.INT) &&
                    !(sourceColumnTypes.get(this.sumColumnIndex).getTypeId() == TypeId.FLOAT)) {
                throw new QueryPlanException("Cannot compute sum over a non-integer column: " + this.averageColumn +
                                             ".");
            }
        }

        // make sure we add the correct columns to the output schema if we have aggregates in the
        // projection
        if (this.hasAggregate) {
            if (this.hasCount) {
                this.columns.add("countAgg");
                columnTypes.add(Type.intType());
            }
            if (this.sumColumn != null) {
                this.columns.add("sumAgg");
                if (sourceColumnTypes.get(this.sumColumnIndex).getTypeId() == TypeId.INT) {
                    columnTypes.add(Type.intType());
                    this.sumIsFloat = false;
                } else {
                    columnTypes.add(Type.floatType());
                    this.sumIsFloat = true;
                }
            }
            if (this.averageColumn != null) {
                this.columns.add("averageAgg");
                columnTypes.add(Type.floatType());
            }
        }
        return new Schema(this.columns, columnTypes);
    }

    @Override
    public Iterator<Record> iterator() { return new ProjectIterator(); }

    private void addToCount() {
        this.countValue++;
    }

    private int getAndResetCount() {
        int result = this.countValue;
        this.countValue = 0;
        return result;
    }

    private void addToSum(Record record) {
        if (this.sumIsFloat) {
            this.sumValue += record.getValues().get(this.sumColumnIndex).getFloat();
        } else {
            this.sumValue += record.getValues().get(this.sumColumnIndex).getInt();
        }
    }

    private double getAndResetSum() {
        double result = this.sumValue;
        this.sumValue = 0;
        return result;
    }

    private void addToAverage(Record record) {
        this.averageCountValue++;
        this.averageSumValue += record.getValues().get(this.averageColumnIndex).getInt();
    }

    private double getAndResetAverage() {
        if (this.averageCountValue == 0) {
            return 0f;
        }

        double result =  this.averageSumValue / this.averageCountValue;
        this.averageSumValue = 0;
        this.averageCountValue = 0;
        return result;
    }

    @Override
    public String str() {
        return "type: " + this.getType() +
               "\ncolumns: " + this.columns;
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
     */
    private class ProjectIterator implements Iterator<Record> {
        private Iterator<Record> sourceIterator;
        private MarkerRecord markerRecord;
        private boolean prevWasMarker;
        private List<DataBox> baseValues;

        private ProjectIterator() {
            this.sourceIterator = ProjectOperator.this.getSource().iterator();
            this.markerRecord = MarkerRecord.getMarker();
            this.prevWasMarker = true;
            this.baseValues = new ArrayList<>();
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.sourceIterator.hasNext();
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
                if (ProjectOperator.this.hasAggregate) {
                    while (this.sourceIterator.hasNext()) {
                        Record r = this.sourceIterator.next();
                        List<DataBox> recordValues = r.getValues();

                        // if the record is a MarkerRecord, that means we reached the end of a group... we reset
                        // the aggregates and add the appropriate new record to the new Records
                        if (r == this.markerRecord) {
                            if (ProjectOperator.this.hasCount) {
                                int count = ProjectOperator.this.getAndResetCount();
                                this.baseValues.add(new IntDataBox(count));
                            }
                            if (ProjectOperator.this.sumColumnIndex != -1) {
                                double sum = ProjectOperator.this.getAndResetSum();

                                if (ProjectOperator.this.sumIsFloat) {
                                    this.baseValues.add(new FloatDataBox((float) sum));
                                } else {
                                    this.baseValues.add(new IntDataBox((int) sum));
                                }
                            }
                            if (ProjectOperator.this.averageColumnIndex != -1) {
                                double average = (float) ProjectOperator.this.getAndResetAverage();
                                this.baseValues.add(new FloatDataBox((float) average));
                            }
                            // record that we just saw a marker record
                            this.prevWasMarker = true;
                            return new Record(this.baseValues);
                        } else {
                            // if the previous record was a marker (or for the first record) we have to get the relevant
                            // fields out of the record
                            if (this.prevWasMarker) {
                                this.baseValues = new ArrayList<>();
                                for (int index : ProjectOperator.this.indices) {
                                    this.baseValues.add(recordValues.get(index));
                                }
                                this.prevWasMarker = false;
                            }
                            if (ProjectOperator.this.hasCount) {
                                ProjectOperator.this.addToCount();
                            }
                            if (ProjectOperator.this.sumColumnIndex != -1) {
                                ProjectOperator.this.addToSum(r);
                            }
                            if (ProjectOperator.this.averageColumnIndex != -1) {
                                ProjectOperator.this.addToAverage(r);
                            }
                        }
                    }

                    // at the very end, we need to make sure we add all the aggregated records to the result
                    // either because there was no group by or to add the last group we saw
                    if (ProjectOperator.this.hasCount) {
                        int count = ProjectOperator.this.getAndResetCount();
                        this.baseValues.add(new IntDataBox(count));
                    }
                    if (ProjectOperator.this.sumColumnIndex != -1) {
                        double sum = ProjectOperator.this.getAndResetSum();

                        if (ProjectOperator.this.sumIsFloat) {
                            this.baseValues.add(new FloatDataBox((float) sum));
                        } else {
                            this.baseValues.add(new IntDataBox((int) sum));
                        }
                    }
                    if (ProjectOperator.this.averageColumnIndex != -1) {
                        double average = ProjectOperator.this.getAndResetAverage();
                        this.baseValues.add(new FloatDataBox((float) average));
                    }
                    return new Record(this.baseValues);
                } else {
                    Record r = this.sourceIterator.next();
                    List<DataBox> recordValues = r.getValues();
                    List<DataBox> newValues = new ArrayList<>();

                    // if there is a marker record (in the case we're projecting from a group by), we simply
                    // leave the marker records in
                    if (r == this.markerRecord) {
                        return markerRecord;
                    } else {
                        for (int index : ProjectOperator.this.indices) {
                            newValues.add(recordValues.get(index));
                        }
                        return new Record(newValues);
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
