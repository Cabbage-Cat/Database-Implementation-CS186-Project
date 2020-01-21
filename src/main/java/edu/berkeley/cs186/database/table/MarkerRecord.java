package edu.berkeley.cs186.database.table;

import java.util.ArrayList;

/**
 * An empty record used to delineate groups in the GroupByOperator (see
 * comments in GroupByOperator).
 */
public class MarkerRecord extends Record {
    private static final MarkerRecord record = new MarkerRecord();

    private MarkerRecord() {
        super(new ArrayList<>());
    }

    public static MarkerRecord getMarker() {
        return MarkerRecord.record;
    }
}
