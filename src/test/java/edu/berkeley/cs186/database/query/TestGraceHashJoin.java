package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.ArrayBacktrackingIterator;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;

import edu.berkeley.cs186.database.table.Record;

import static org.junit.Assert.*;
import org.junit.After;

@Category({Proj3Tests.class, Proj3Part1Tests.class})
public class TestGraceHashJoin {
    private Database d;
    private Map<Long, Page> pinnedPages = new HashMap<>();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        d = new Database(tempDir.getAbsolutePath(), 256);
        d.setWorkMem(6); // B = 6
        d.waitAllTransactions();
    }

    @After
    public void cleanup() {
        for (Page p : pinnedPages.values()) {
            p.unpin();
        }
        d.close();
    }

    /**
     * Sanity Test to make sure NHJ works. You should pass this without having touched GHJ.
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleNHJ() {
        try(Transaction transaction = d.beginTransaction()) {
            Schema schema = TestUtils.createSchemaWithAllTypes();

            List<Record> leftRecords = new ArrayList<>();
            Record[] rightRecords = new Record[10];
            List<Record> expectedOutput = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                leftRecords.add(r);
            }

            Iterator<Record> leftIter = leftRecords.iterator();

            for (int i = 5; i < 15; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                rightRecords[i - 5] = r;
            }

            BacktrackingIterator<Record> rightIter = new ArrayBacktrackingIterator<>(rightRecords);

            for (int i = 5; i < 10; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                expectedOutput.add(joinRecords(r, r));
            }

            NaiveHashJoin nhj = new NaiveHashJoin(leftIter, rightIter, 1, 1,
                                                  transaction.getTransactionContext(), schema);

            List<Record> output = nhj.run();

            assertEquals(5, output.size());
            assertEquals(expectedOutput, output);
        }
    }

    /**
     * Sanity test on a simple set of inputs. GHJ should behave similarly to NHJ for this one.
     */
    @Test
    @Category(PublicTests.class)
    public void testSimpleGHJ() {
        try(Transaction transaction = d.beginTransaction()) {
            List<Record> leftRecords = new ArrayList<>();
            List<Record> rightRecords = new ArrayList<>();
            List<Record> expectedOutput = new ArrayList<>();

            Schema schema = TestUtils.createSchemaWithAllTypes();

            for (int i = 0; i < 10; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                leftRecords.add(r);
            }

            Iterator<Record> leftIter = leftRecords.iterator();

            for (int i = 5; i < 15; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                rightRecords.add(r);
            }

            Iterator<Record> rightIter = rightRecords.iterator();

            for (int i = 5; i < 10; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                expectedOutput.add(joinRecords(r, r));
            }

            GraceHashJoin ghj = new GraceHashJoin(leftIter, rightIter, 1, 1,
                                                  transaction.getTransactionContext(), schema, schema, true);

            List<Record> output = ghj.begin();

            assertEquals(5, output.size());
            assertEquals(expectedOutput, output);
        }
    }

    /**
     * Tests GHJ with records of different schemas on some join column.
     */
    @Test
    @Category(PublicTests.class)
    public void testGHJDifferentSchemas() {
        try(Transaction transaction = d.beginTransaction()) {
            Schema schema1 = TestUtils.createSchemaOfIntAndString(10);
            Schema schema2 = TestUtils.createSchemaWithAllTypes();

            List<Record> leftRecords = new ArrayList<>();
            List<Record> rightRecords = new ArrayList<>();
            Set<Record> expectedOutput = new HashSet<>();

            d.setWorkMem(3);

            for (int i = 0; i < 1860; i++) {
                Record r = TestUtils.createRecordWithIntAndStringWithValue(i, "I love 186", 10);
                leftRecords.add(r);
            }

            Iterator<Record> leftIter = leftRecords.iterator();

            for (int i = 186; i < 9300; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                rightRecords.add(r);
            }

            Iterator<Record> rightIter = rightRecords.iterator();

            for (int i = 186; i < 1860; i++) {
                Record r1 = TestUtils.createRecordWithIntAndStringWithValue(i, "I love 186", 10);
                Record r2 = TestUtils.createRecordWithAllTypesWithValue(i);

                // Unsure the order of join so we join both ways and check that the outputted join is at least one of them
                expectedOutput.add(joinRecords(r1, r2));
                expectedOutput.add(joinRecords(r2, r1));
            }

            GraceHashJoin ghj = new GraceHashJoin(leftIter, rightIter, 0, 1,
                                                  transaction.getTransactionContext(), schema1, schema2, true);

            List<Record> output = ghj.begin();

            assertEquals(1674, output.size());

            for (Record r : output) {
                assertTrue("Output incorrect", expectedOutput.contains(r));
            }
        }
    }

    /**
     * Tests student's input and checks whether NHJ fails but GHJ passes.
     */
    @Test
    @Category(PublicTests.class)
    public void testBreakNHJButPassGHJ() {
        try(Transaction transaction = d.beginTransaction()) {
            Schema schema = TestUtils.createSchemaOfInt();

            Pair<List<Record>, List<Record>> inputs = GraceHashJoin.getBreakNHJInputs();

            List<Record> leftRecords = inputs.getFirst();
            Iterator<Record> leftIter = leftRecords.iterator();

            Record[] rightRecords = inputs.getSecond().toArray(new Record[inputs.getSecond().size()]);
            BacktrackingIterator<Record> rightIter = new ArrayBacktrackingIterator<>(rightRecords);

            NaiveHashJoin nhj = new NaiveHashJoin(leftIter, rightIter, 0, 0,
                                                  transaction.getTransactionContext(), schema);

            try {
                nhj.run();
                fail("Naive Hash Join did not fail!");
            } catch (Exception e) {
                assertEquals("Naive Hash failed for the wrong reason!",
                             "The records in this partition cannot fit in B-2 pages of memory.", e.getMessage());
            }

            leftIter = leftRecords.iterator();
            rightIter = new ArrayBacktrackingIterator<>(rightRecords);

            GraceHashJoin ghj = new GraceHashJoin(leftIter, rightIter, 0, 0,
                                                  transaction.getTransactionContext(), schema, schema, false);

            try {
                ghj.begin();
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Tests student input such that GHJ breaks!
     */
    @Test
    @Category(PublicTests.class)
    public void testGHJBreak() {
        try(Transaction transaction = d.beginTransaction()) {
            Schema schema = TestUtils.createSchemaOfInt();

            Pair<List<Record>, List<Record>> inputs = GraceHashJoin.getBreakGHJInputs();

            List<Record> leftRecords = inputs.getFirst();
            List<Record> rightRecords = inputs.getSecond();

            Iterator<Record> leftIter = leftRecords.iterator();
            Iterator<Record> rightIter = rightRecords.iterator();

            GraceHashJoin ghj = new GraceHashJoin(leftIter, rightIter, 0, 0,
                                                  transaction.getTransactionContext(), schema, schema, false);

            try {
                List<Record> out = ghj.begin();
                fail("GHJ Worked! It shouldn't have...");
            } catch (Exception e) {
                assertEquals("GHJ Failed for the wrong reason...",
                             "Reached the max number of passes cap", e.getMessage());
            }
        }
    }

    /**
     * Helper method to create a joined record from a record of the left relation
     * and a record of the right relation.
     * @param leftRecord Record from the left relation
     * @param rightRecord Record from the right relation
     * @return joined record
     */
    private Record joinRecords(Record leftRecord, Record rightRecord) {
        List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
        leftValues.addAll(rightValues);
        return new Record(leftValues);
    }
}
