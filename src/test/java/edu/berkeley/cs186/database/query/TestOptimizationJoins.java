package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.PredicateOperator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import edu.berkeley.cs186.database.table.Schema;

import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.BoolDataBox;

import org.junit.After;

import edu.berkeley.cs186.database.TimeoutScaling;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({HW3Tests.class, HW3Part2Tests.class})
public class TestOptimizationJoins {
    private static final String TABLENAME = "T";

    private static final String TestDir = "testDatabase";
    private Database db;

    //Before every test you create a temporary table, after every test you close it
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 10 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                10000 * TimeoutScaling.factor)));

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
    public void testJoinTypeA() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME, r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME).buildStatistics(10);

            // add a join and a select to the QueryPlan
            QueryPlan query = transaction.query("T", "t1");
            query.join("T", "t2", "t1.int", "t2.int");

            //query.select("int", PredicateOperator.EQUALS, new IntDataBox(10));

            // execute the query
            query.execute();

            QueryOperator finalOperator = query.getFinalOperator();
            assertTrue(finalOperator.toString().contains("BNLJ"));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testJoinTypeB() {
        try(Transaction transaction = this.db.beginTransaction()) {
            for (int i = 0; i < 10; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME + "I", r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME + "I").buildStatistics(10);

            // add a join and a select to the QueryPlan
            QueryPlan query = transaction.query("TI", "t1");
            query.join("TI", "t2", "t1.int", "t2.int");
            query.select("int", PredicateOperator.EQUALS, new IntDataBox(9));

            // execute the query
            query.execute();

            QueryOperator finalOperator = query.getFinalOperator();

            assertTrue(finalOperator.toString().contains("\tvalue: 9"));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testJoinTypeC() {
        try(Transaction transaction = db.beginTransaction()) {
            for (int i = 0; i < 2000; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME + "I", r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME + "I").buildStatistics(10);

            // add a join and a select to the QueryPlan
            QueryPlan query = transaction.query("TI", "t1");
            query.join("TI", "t2", "t1.int", "t2.int");
            query.select("int", PredicateOperator.EQUALS, new IntDataBox(9));

            // execute the query
            query.execute();

            QueryOperator finalOperator = query.getFinalOperator();

            assertTrue(finalOperator.toString().contains("INDEXSCAN"));

            assertTrue(finalOperator.toString().contains("SNLJ"));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testJoinOrderA() {
        try(Transaction transaction = db.beginTransaction()) {
            for (int i = 0; i < 10; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME + "o1", r.getValues());
            }

            for (int i = 0; i < 100; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME + "o2", r.getValues());
            }

            for (int i = 0; i < 2000; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME + "o3", r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME + "o1").buildStatistics(10);
            transaction.getTransactionContext().getTable(TABLENAME + "o2").buildStatistics(10);
            transaction.getTransactionContext().getTable(TABLENAME + "o3").buildStatistics(10);

            // add a join and a select to the QueryPlan
            QueryPlan query = transaction.query("To1");
            query.join("To2", "To1.one_int", "To2.two_int");
            query.join("To3", "To2.two_int", "To3.three_int");

            // execute the query
            query.execute();

            QueryOperator finalOperator = query.getFinalOperator();
            //inner most joins are the largest tables
            assertTrue(finalOperator.toString().contains("\t\ttable: To2"));
            assertTrue(finalOperator.toString().contains("\t\ttable: To3"));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testJoinOrderB() {
        try(Transaction transaction = db.beginTransaction()) {
            for (int i = 0; i < 10; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME + "o1", r.getValues());
            }

            for (int i = 0; i < 100; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME + "o2", r.getValues());
            }

            for (int i = 0; i < 2000; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME + "o3", r.getValues());
                transaction.insert(TABLENAME + "o4", r.getValues());
            }

            transaction.getTransactionContext().getTable(TABLENAME + "o1").buildStatistics(10);
            transaction.getTransactionContext().getTable(TABLENAME + "o2").buildStatistics(10);
            transaction.getTransactionContext().getTable(TABLENAME + "o3").buildStatistics(10);
            transaction.getTransactionContext().getTable(TABLENAME + "o4").buildStatistics(10);

            // add a join and a select to the QueryPlan
            QueryPlan query = transaction.query("To1");
            query.join("To2", "To1.one_int", "To2.two_int");
            query.join("To3", "To2.two_int", "To3.three_int");
            query.join("To4", "To1.one_string", "To4.four_string");

            // execute the query
            query.execute();

            QueryOperator finalOperator = query.getFinalOperator();

            //smallest to largest order
            assertTrue(finalOperator.toString().contains("\t\t\ttable: To2"));
            assertTrue(finalOperator.toString().contains("\t\t\ttable: To3"));
            assertTrue(finalOperator.toString().contains("\t\ttable: To1"));
            assertTrue(finalOperator.toString().contains("\ttable: To4"));
        }
    }

}
