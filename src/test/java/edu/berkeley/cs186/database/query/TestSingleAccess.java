package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.PredicateOperator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

import edu.berkeley.cs186.database.table.Schema;

import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.BoolDataBox;

import edu.berkeley.cs186.database.TimeoutScaling;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;

@Category({Proj3Tests.class, Proj3Part2Tests.class})
public class TestSingleAccess {
    private static final String TABLENAME = "T";

    private static final String TestDir = "testDatabase";
    private Database db;

    //Before every test you create a temporary table, after every test you close it
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 2 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                2000 * TimeoutScaling.factor)));

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        String filename = testDir.getAbsolutePath();
        this.db = new Database(filename, 32);
        this.db.setWorkMem(5); // B=5
        this.db.waitSetupFinished();

        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();

            Schema schema = TestUtils.createSchemaWithAllTypes();

            t.createTable(schema, TABLENAME);

            t.createTable(schema, TABLENAME + "I");
            t.createIndex(TABLENAME + "I", "int", false);
            t.createTable(schema, TABLENAME + "MI");
            t.createIndex(TABLENAME + "MI", "int", false);
            t.createIndex(TABLENAME + "MI", "float", false);

            t.createTable(TestUtils.createSchemaWithAllTypes("one_"), TABLENAME + "o1");
            t.createTable(TestUtils.createSchemaWithAllTypes("two_"), TABLENAME + "o2");
            t.createTable(TestUtils.createSchemaWithAllTypes("three_"), TABLENAME + "o3");
            t.createTable(TestUtils.createSchemaWithAllTypes("four_"), TABLENAME + "o4");
        }
        this.db.waitAllTransactions();
    }

    @After
    public void afterEach() {
        this.db.waitAllTransactions();
        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
        }
        this.db.close();
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
    public void testSequentialScanSelection() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME, r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME).buildStatistics(10);

            QueryPlan query = transaction.query(TABLENAME, "t1");

            QueryOperator op = query.minCostSingleAccess("t1");

            assertTrue(op.isSequentialScan());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleIndexScanSelection() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME + "I", r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME + "I").buildStatistics(10);

            QueryPlan query = transaction.query(TABLENAME + "I", "t1");
            query.select("int", PredicateOperator.EQUALS, new IntDataBox(9));

            QueryOperator op = query.minCostSingleAccess("t1");

            assertTrue(op.isIndexScan());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testPushDownSelects() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME, r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME).buildStatistics(10);

            QueryPlan query = transaction.query(TABLENAME, "t1");
            query.select("int", PredicateOperator.EQUALS, new IntDataBox(9));

            QueryOperator op = query.minCostSingleAccess("t1");

            assertTrue(op.isSelect());
            assertTrue(op.getSource().isSequentialScan());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testPushDownMultipleSelects() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME, r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME).buildStatistics(10);

            QueryPlan query = transaction.query(TABLENAME, "t1");
            query.select("int", PredicateOperator.EQUALS, new IntDataBox(9));
            query.select("bool", PredicateOperator.EQUALS, new BoolDataBox(false));

            QueryOperator op = query.minCostSingleAccess("t1");

            assertTrue(op.isSelect());
            assertTrue(op.getSource().isSelect());
            assertTrue(op.getSource().getSource().isSequentialScan());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testNoValidIndices() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", i);
                transaction.insert(TABLENAME + "MI", r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME + "MI").buildStatistics(10);

            QueryPlan query = transaction.query(TABLENAME + "MI", "t1");

            QueryOperator op = query.minCostSingleAccess("t1");

            assertTrue(op.isSequentialScan());
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testIndexSelectionAndPushDown() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", i);
                transaction.insert(TABLENAME + "MI", r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME + "MI").buildStatistics(10);
            QueryPlan query = transaction.query(TABLENAME + "MI", "t1");
            query.select("int", PredicateOperator.EQUALS, new IntDataBox(9));
            query.select("bool", PredicateOperator.EQUALS, new BoolDataBox(false));

            QueryOperator op = query.minCostSingleAccess("t1");

            assertTrue(op.isSelect());
            assertTrue(op.getSource().isIndexScan());
        }
    }
}
