package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;

import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.BoolDataBox;

import static org.junit.Assert.*;
import org.junit.After;

import edu.berkeley.cs186.database.TimeoutScaling;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

@Category({Proj3Tests.class, Proj3Part2Tests.class})
public class TestBasicQuery {
    private static final String TABLENAME = "T";

    private static final String TestDir = "testDatabase";
    private Database db;

    //Before every test you create a temporary table, after every test you close it
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 1 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

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

            t.createTable(schema, TABLENAME + "2");
            t.createIndex(TABLENAME + "2", "int", false);
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
    public void testProject() {
        try(Transaction transaction = this.db.beginTransaction()) {
            //creates a 10 records int 0 to 9
            for (int i = 0; i < 10; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME, r.getValues());
            }

            //build the statistics on the table
            transaction.getTransactionContext().getTable(TABLENAME).buildStatistics(10);

            // add a project to the QueryPlan
            QueryPlan query = transaction.query("T");
            query.project(Collections.singletonList("int"));

            // execute the query and get the output
            Iterator<Record> queryOutput = query.execute();

            query.getFinalOperator();

            //tests to see if projects are applied properly
            int count = 0;
            while (queryOutput.hasNext()) {
                Record r = queryOutput.next();
                assertEquals(r.getValues().get(0), new IntDataBox(count));
                count++;
            }
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSelect() {
        try(Transaction transaction = db.beginTransaction()) {
            //creates a 10 records int 0 to 9
            for (int i = 0; i < 10; ++i) {
                Record r = createRecordWithAllTypes(false, i, "!", 0.0f);
                transaction.insert(TABLENAME, r.getValues());
            }

            //build the statistics on the table
            transaction.getTransactionContext().getTable(TABLENAME).buildStatistics(10);

            // add a select to the QueryPlan
            QueryPlan query = transaction.query("T");
            query.select("int", PredicateOperator.EQUALS, new IntDataBox(9));

            // execute the query and get the output
            Iterator<Record> queryOutput = query.execute();

            query.getFinalOperator();

            //tests to see if projects are applied properly
            assert (queryOutput.hasNext());

            Record r = queryOutput.next();
            assertEquals(r.getValues().get(1), new IntDataBox(9));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testGroupBy() {
        try(Transaction transaction = db.beginTransaction()) {
            //creates a 100 records int 0 to 9
            for (int i = 0; i < 100; ++i) {
                Record r = createRecordWithAllTypes(false, i % 10, "!", 0.0f);
                transaction.insert(TABLENAME, r.getValues());
            }

            //build the statistics on the table
            transaction.getTransactionContext().getTable(TABLENAME).buildStatistics(10);

            // add a project and a groupby to the QueryPlan
            QueryPlan query = transaction.query("T");
            query.groupBy("T.int");
            query.count();

            // execute the query and get the output
            Iterator<Record> queryOutput = query.execute();

            query.getFinalOperator();

            //tests to see if projects/group by are applied properly
            int count = 0;
            while (queryOutput.hasNext()) {
                Record r = queryOutput.next();
                assertEquals(r.getValues().get(0).getInt(), 10);
                count++;
            }

            assertEquals(count, 10);
        }
    }
}
