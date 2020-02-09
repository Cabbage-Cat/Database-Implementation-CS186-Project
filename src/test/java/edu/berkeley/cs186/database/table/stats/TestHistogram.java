package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.table.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.berkeley.cs186.database.TestUtils;

import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

@Category({Proj3Tests.class, Proj3Part2Tests.class})
public class TestHistogram {
    private static final String TABLENAME = "testtable";
    private Table table;

    // 400ms max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                400 * TimeoutScaling.factor)));

    @Before
    public void beforeEach() {
        Schema schema = TestUtils.createSchemaWithAllTypes();
        HeapFile heapFile = new MemoryHeapFile();
        this.table = new Table(TABLENAME, schema, heapFile, new DummyLockContext());
    }

    //creates a record with all specified types
    private static Record createRecordWithAllTypes(boolean a1, int a2, String a3, float a4) {
        Record r = TestUtils.createRecordWithAllTypes();
        r.getValues().set(0, new BoolDataBox(a1));
        r.getValues().set(1, new IntDataBox(a2));
        r.getValues().set(2, new StringDataBox(a3, 1));
        r.getValues().set(3, new FloatDataBox(a4));
        return r;
    }

    @Test
    @Category(PublicTests.class)
    public void testBuildHistogramBasic() {
        //creates a 101 records int 0 to 100
        for (int i = 0; i < 101; ++i) {
            Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
            table.addRecord(r.getValues());
        }

        //creates a histogram of 10 buckets
        Histogram h = new Histogram(10);
        h.buildHistogram(table, 1); //build on the integer col

        assertEquals(101, h.getCount()); //count updated properly

        assertEquals(101, h.getNumDistinct()); //distinct count updated properly

        for (int i = 0; i < 9; i++) {
            assertEquals(10, h.get(i).getCount());
        }

        assertEquals(11, h.get(9).getCount());
    }

    @Test
    @Category(PublicTests.class)
    public void testBuildHistogramString() {
        //creates a 101 records int 0 to 100
        for (int i = 0; i < 101; ++i) {
            Record r = createRecordWithAllTypes(false, 0, "" + (char) i, 0.0f);
            table.addRecord(r.getValues());
        }

        //creates a histogram of 10 buckets
        Histogram h = new Histogram(10);
        h.buildHistogram(table, 2); //build on the integer col

        assertEquals(101, h.getCount()); //count updated properly

        assertEquals(101, h.getNumDistinct()); //distinct count updated properly
    }

    @Test
    @Category(PublicTests.class)
    public void testBuildHistogramEdge() {
        //creates a 101 records int 0 to 100
        for (int i = 0; i < 101; ++i) {
            Record r = createRecordWithAllTypes(false, 0, "!", 0.0f);
            table.addRecord(r.getValues());
        }

        //creates a histogram of 10 buckets
        Histogram h = new Histogram(10);
        h.buildHistogram(table, 1); //build on the integer col

        assertEquals(101, h.getCount()); //count updated properly

        assertEquals(1, h.getNumDistinct()); //distinct count updated properly

        for (int i = 0; i < 9; i++) {
            assertEquals(0, h.get(i).getCount());
        }

        assertEquals(101, h.get(9).getCount());
    }

    @Test
    @Category(PublicTests.class)
    public void testEquality() {
        //creates a 100 records int 0 to 99
        for (int i = 0; i < 100; ++i) {
            Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
            table.addRecord(r.getValues());
        }

        //creates a histogram of 10 buckets
        Histogram h = new Histogram(10);
        h.buildHistogram(table, 1); //build on the integer col

        //Should return [0.1,0,0,0,0,0,0,0,0,0,0]
        float [] result = h.filter(PredicateOperator.EQUALS, new IntDataBox(5));
        assertEquals(0.1, result[0], 0.00001);
        for (int i = 1; i < 10; i++) {
            assertEquals(0.0, result[i], 0.00001);
        }

        //Should return [0.9,1,1,1,1,1,1,1,1,1,1]
        result = h.filter(PredicateOperator.NOT_EQUALS, new IntDataBox(5));
        assertEquals(0.9, result[0], 0.00001);
        for (int i = 1; i < 10; i++) {
            assertEquals(1.0, result[i], 0.00001);
        }

        //Should return [0,0,0,0,0,0,0,0,0,0,0.1]
        result = h.filter(PredicateOperator.EQUALS, new IntDataBox(99));
        for (int i = 0; i < 9; i++) {
            assertEquals(0.0, result[i], 0.00001);
        }
        assertEquals(0.1, result[9], 0.00001);

        //Should return [0,0,0,0,0,0,0,0,0,0,0.0]
        result = h.filter(PredicateOperator.EQUALS, new IntDataBox(100));
        for (int i = 0; i < 10; i++) {
            assertEquals(0.0, result[i], 0.00001);
        }

        //Should return [0,0,0,0,0,0,0,0,0,0,0.0]
        result = h.filter(PredicateOperator.EQUALS, new IntDataBox(-1));
        for (int i = 0; i < 10; i++) {
            assertEquals(0.0, result[i], 0.00001);
        }

        //Should return [1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]
        result = h.filter(PredicateOperator.NOT_EQUALS, new IntDataBox(-1));
        for (int i = 0; i < 10; i++) {
            assertEquals(1.0, result[i], 0.00001);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testGreaterThan() {
        //creates a 101 records int 0 to 100
        for (int i = 0; i <= 100; ++i) {
            Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
            table.addRecord(r.getValues());
        }

        //creates a histogram of 10 buckets
        Histogram h = new Histogram(10);
        h.buildHistogram(table, 1); //build on the integery col

        //Should return [0.1,1,1,1,1,1,1,1,1,1,1]
        float [] result = h.filter(PredicateOperator.GREATER_THAN, new IntDataBox(9));
        assertEquals(0.1, result[0], 0.00001);
        for (int i = 1; i < 10; i++) {
            assertEquals(1.0, result[i], 0.00001);
        }

        //Should return [0.0,1,1,1,1,1,1,1,1,1,1]
        result = h.filter(PredicateOperator.GREATER_THAN, new IntDataBox(10));
        assertEquals(0.0, result[0], 0.00001);
        for (int i = 1; i < 10; i++) {
            assertEquals(1.0, result[i], 0.00001);
        }

        //Should return [1,1,1,1,1,1,1,1,1,1,1]
        result = h.filter(PredicateOperator.GREATER_THAN, new IntDataBox(-1));
        for (int i = 0; i < 10; i++) {
            assertEquals(1.0, result[i], 0.00001);
        }

        //Should return [0,0,0,0,0,0,0,0,0,0,0.0]
        result = h.filter(PredicateOperator.GREATER_THAN, new IntDataBox(101));
        for (int i = 0; i < 10; i++) {
            assertEquals(0.0, result[i], 0.00001);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testLessThan() {
        //creates a 101 records int 0 to 100
        for (int i = 0; i <= 100; ++i) {
            Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
            table.addRecord(r.getValues());
        }

        //creates a histogram of 10 buckets
        Histogram h = new Histogram(10);
        h.buildHistogram(table, 1); //build on the integery col

        //Should return [0.9,0,0,0,0,0,0,0,0,0,0]
        float [] result = h.filter(PredicateOperator.LESS_THAN, new IntDataBox(9));
        assertEquals(0.9, result[0], 0.00001);
        for (int i = 1; i < 10; i++) {
            assertEquals(0.0, result[i], 0.00001);
        }

        //Should return [1.0,0,0,0,0,0,0,0,0,0,0]
        result = h.filter(PredicateOperator.LESS_THAN, new IntDataBox(10));
        assertEquals(1.0, result[0], 0.00001);
        for (int i = 1; i < 10; i++) {
            assertEquals(0.0, result[i], 0.00001);
        }

        //Should return [1,1,1,1,1,1,1,1,1,1,1]
        result = h.filter(PredicateOperator.LESS_THAN, new IntDataBox(101));
        for (int i = 0; i < 10; i++) {
            assertEquals(1.0, result[i], 0.00001);
        }

        //Should return [0,0,0,0,0,0,0,0,0,0,0.0]
        result = h.filter(PredicateOperator.LESS_THAN, new IntDataBox(-1));
        for (int i = 0; i < 10; i++) {
            assertEquals(0.0, result[i], 0.00001);
        }
    }

}
