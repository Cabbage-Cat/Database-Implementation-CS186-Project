package edu.berkeley.cs186.database.query;

import java.util.Iterator;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.table.Record;

class MaterializeOperator extends SequentialScanOperator {
    /**
     * Operator that materializes the source operator into a temporary table immediately,
     * and then acts as a sequential scan operator over the temporary table.
     * @param source source operator to be materialized
     * @param transaction current running transaction
     */
    MaterializeOperator(QueryOperator source,
                        TransactionContext transaction) {
        super(OperatorType.MATERIALIZE, transaction, materialize(source, transaction));
    }

    private static String materialize(QueryOperator source, TransactionContext transaction) {
        String materializedTableName = transaction.createTempTable(source.getOutputSchema());
        for (Record record : source) {
            transaction.addRecord(materializedTableName, record.getValues());
        }
        return materializedTableName;
    }

    @Override
    public String str() {
        return "type: " + this.getType();
    }

}
