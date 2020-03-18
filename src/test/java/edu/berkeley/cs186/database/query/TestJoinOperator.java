package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.Page;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.table.Record;

import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

@Category({Proj3Tests.class, Proj3Part1Tests.class})
public class TestJoinOperator {
    private Database d;
    private long numIOs;
    private QueryOperator leftSourceOperator;
    private QueryOperator rightSourceOperator;
    private Map<Long, Page> pinnedPages = new HashMap<>();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("joinTest");
        d = new Database(tempDir.getAbsolutePath(), 256);
        d.setWorkMem(5); // B=5
        d.waitAllTransactions();
    }

    @After
    public void cleanup() {
        for (Page p : pinnedPages.values()) {
            p.unpin();
        }
        d.close();
    }

    // 4 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                4000 * TimeoutScaling.factor)));

    private void startCountIOs() {
        d.getBufferManager().evictAll();
        numIOs = d.getBufferManager().getNumIOs();
    }

    private void checkIOs(String message, long minIOs, long maxIOs) {
        if (message == null) {
            message = "";
        } else {
            message = "(" + message + ")";
        }

        long newIOs = d.getBufferManager().getNumIOs();
        long IOs = newIOs - numIOs;

        assertTrue(IOs + " I/Os not between " + minIOs + " and " + maxIOs + message,
                   minIOs <= IOs && IOs <= maxIOs);
        numIOs = newIOs;
    }

    private void checkIOs(String message, long numIOs) {
        checkIOs(message, numIOs, numIOs);
    }

    private void checkIOs(long minIOs, long maxIOs) {
        checkIOs(null, minIOs, maxIOs);
    }
    private void checkIOs(long numIOs) {
        checkIOs(null, numIOs, numIOs);
    }

    private void setSourceOperators(TestSourceOperator leftSourceOperator,
                                    TestSourceOperator rightSourceOperator, Transaction transaction) {
        setSourceOperators(
            new MaterializeOperator(leftSourceOperator, transaction.getTransactionContext()),
            new MaterializeOperator(rightSourceOperator, transaction.getTransactionContext())
        );
    }

    private void pinPage(int partNum, int pageNum) {
        long pnum = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        Page page = d.getBufferManager().fetchPage(new DummyLockContext(), pnum, false);
        this.pinnedPages.put(pnum, page);
    }

    private void unpinPage(int partNum, int pageNum) {
        long pnum = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        this.pinnedPages.remove(pnum).unpin();
    }

    private void evictPage(int partNum, int pageNum) {
        long pnum = DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        this.d.getBufferManager().evict(pnum);
        numIOs = d.getBufferManager().getNumIOs();
    }

    private void setSourceOperators(QueryOperator leftSourceOperator,
                                    QueryOperator rightSourceOperator) {
        assert (this.leftSourceOperator == null && this.rightSourceOperator == null);

        this.leftSourceOperator = leftSourceOperator;
        this.rightSourceOperator = rightSourceOperator;

        // hard-coded mess, but works as long as the first two tables created are the source operators
        pinPage(1, 0); // information_schema.tables header page
        pinPage(1, 3); // information_schema.tables entry for left source
        pinPage(1, 4); // information_schema.tables entry for right source
        pinPage(3, 0); // left source header page
        pinPage(4, 0); // right source header page
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleJoinPNLJ() {
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                new TestSourceOperator(),
                new TestSourceOperator(),
                transaction
            );

            startCountIOs();

            JoinOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            int numRecords = 0;
            List<DataBox> expectedRecordValues = new ArrayList<>();
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            Record expectedRecord = new Record(expectedRecordValues);

            while (outputIterator.hasNext() && numRecords < 100 * 100) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
            }
            checkIOs(0);

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 100 * 100, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleJoinBNLJ() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                new TestSourceOperator(),
                new TestSourceOperator(),
                transaction
            );

            startCountIOs();

            JoinOperator joinOperator = new BNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            int numRecords = 0;
            List<DataBox> expectedRecordValues = new ArrayList<>();
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            Record expectedRecord = new Record(expectedRecordValues);

            while (outputIterator.hasNext() && numRecords < 100 * 100) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
            }
            checkIOs(0);

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 100 * 100, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePNLJOutputOrder() {
        try(Transaction transaction = d.beginTransaction()) {
            Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
            List<DataBox> r1Vals = r1.getValues();
            Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
            List<DataBox> r2Vals = r2.getValues();

            List<DataBox> expectedRecordValues1 = new ArrayList<>();
            List<DataBox> expectedRecordValues2 = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                expectedRecordValues1.addAll(r1Vals);
                expectedRecordValues2.addAll(r2Vals);
            }

            Record expectedRecord1 = new Record(expectedRecordValues1);
            Record expectedRecord2 = new Record(expectedRecordValues2);
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");

            for (int i = 0; i < 400; i++) {
                List<DataBox> vals;
                if (i < 200) {
                    vals = r1Vals;
                } else {
                    vals = r2Vals;
                }
                transaction.getTransactionContext().addRecord("leftTable", vals);
                transaction.getTransactionContext().addRecord("rightTable", vals);
            }

            for (int i = 0; i < 400; i++) {
                if (i < 200) {
                    transaction.getTransactionContext().addRecord("leftTable", r2Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r1Vals);
                } else {
                    transaction.getTransactionContext().addRecord("leftTable", r1Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r2Vals);
                }
            }

            setSourceOperators(
                new SequentialScanOperator(transaction.getTransactionContext(), "leftTable"),
                new SequentialScanOperator(transaction.getTransactionContext(), "rightTable")
            );

            startCountIOs();

            QueryOperator joinOperator = new PNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            int count = 0;
            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2);

            while (outputIterator.hasNext() && count < 400 * 400 * 2) {
                if (count < 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                } else if (count < 200 * 200 * 2) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else if (count < 200 * 200 * 3) {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                } else if (count < 200 * 200 * 4) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else if (count < 200 * 200 * 5) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else if (count < 200 * 200 * 6) {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                } else if (count < 200 * 200 * 7) {
                    assertEquals("mismatch at record " + count, expectedRecord2, outputIterator.next());
                } else {
                    assertEquals("mismatch at record " + count, expectedRecord1, outputIterator.next());
                }
                count++;

                if (count == 200 * 200 * 2 || count == 200 * 200 * 6) {
                    checkIOs("at record " + count, 1);
                    evictPage(4, 1);
                } else if (count == 200 * 200 * 4) {
                    checkIOs("at record " + count, 2);
                    evictPage(4, 2);
                    evictPage(3, 1);
                } else {
                    checkIOs("at record " + count, 0);
                }
            }

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 400 * 400 * 2, count);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleSortMergeJoin() {
        d.setWorkMem(5); // B=5
        try(Transaction transaction = d.beginTransaction()) {
            setSourceOperators(
                new TestSourceOperator(),
                new TestSourceOperator(),
                transaction
            );

            startCountIOs();

            JoinOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "int",
                    "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2 * (1 + (1 + TestSortOperator.NEW_TABLE_IOS)));

            int numRecords = 0;
            List<DataBox> expectedRecordValues = new ArrayList<>();
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            expectedRecordValues.add(new BoolDataBox(true));
            expectedRecordValues.add(new IntDataBox(1));
            expectedRecordValues.add(new StringDataBox("a", 1));
            expectedRecordValues.add(new FloatDataBox(1.2f));
            Record expectedRecord = new Record(expectedRecordValues);

            while (outputIterator.hasNext() && numRecords < 100 * 100) {
                assertEquals("mismatch at record " + numRecords, expectedRecord, outputIterator.next());
                numRecords++;
            }
            checkIOs(0);

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 100 * 100, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSortMergeJoinUnsortedInputs()  {
        d.setWorkMem(3); // B=3
        try(Transaction transaction = d.beginTransaction()) {
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
            Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
            List<DataBox> r1Vals = r1.getValues();
            Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
            List<DataBox> r2Vals = r2.getValues();
            Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
            List<DataBox> r3Vals = r3.getValues();
            Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
            List<DataBox> r4Vals = r4.getValues();
            List<DataBox> expectedRecordValues1 = new ArrayList<>();
            List<DataBox> expectedRecordValues2 = new ArrayList<>();
            List<DataBox> expectedRecordValues3 = new ArrayList<>();
            List<DataBox> expectedRecordValues4 = new ArrayList<>();

            for (int i = 0; i < 2; i++) {
                expectedRecordValues1.addAll(r1Vals);
                expectedRecordValues2.addAll(r2Vals);
                expectedRecordValues3.addAll(r3Vals);
                expectedRecordValues4.addAll(r4Vals);
            }
            Record expectedRecord1 = new Record(expectedRecordValues1);
            Record expectedRecord2 = new Record(expectedRecordValues2);
            Record expectedRecord3 = new Record(expectedRecordValues3);
            Record expectedRecord4 = new Record(expectedRecordValues4);
            List<Record> leftTableRecords = new ArrayList<>();
            List<Record> rightTableRecords = new ArrayList<>();
            for (int i = 0; i < 400 * 2; i++) {
                Record r;
                if (i % 4 == 0) {
                    r = r1;
                } else if (i % 4 == 1) {
                    r = r2;
                } else if (i % 4 == 2) {
                    r = r3;
                } else {
                    r = r4;
                }
                leftTableRecords.add(r);
                rightTableRecords.add(r);
            }
            Collections.shuffle(leftTableRecords, new Random(10));
            Collections.shuffle(rightTableRecords, new Random(20));
            for (int i = 0; i < 400 * 2; i++) {
                transaction.getTransactionContext().addRecord("leftTable", leftTableRecords.get(i).getValues());
                transaction.getTransactionContext().addRecord("rightTable", rightTableRecords.get(i).getValues());
            }

            setSourceOperators(
                new SequentialScanOperator(transaction.getTransactionContext(), "leftTable"),
                new SequentialScanOperator(transaction.getTransactionContext(), "rightTable")
            );

            startCountIOs();

            JoinOperator joinOperator = new SortMergeOperator(leftSourceOperator, rightSourceOperator, "int",
                    "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(2 * (2 + (2 + TestSortOperator.NEW_TABLE_IOS)));

            int numRecords = 0;
            Record expectedRecord;

            while (outputIterator.hasNext() && numRecords < 400 * 400) {
                if (numRecords < (400 * 400 / 4)) {
                    expectedRecord = expectedRecord1;
                } else if (numRecords < (400 * 400 / 2)) {
                    expectedRecord = expectedRecord2;
                } else if (numRecords < 400 * 400 - (400 * 400 / 4)) {
                    expectedRecord = expectedRecord3;
                } else {
                    expectedRecord = expectedRecord4;
                }
                Record r = outputIterator.next();
                assertEquals("mismatch at record " + numRecords, expectedRecord, r);
                numRecords++;
            }
            checkIOs(0);

            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 400 * 400, numRecords);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testBNLJDiffOutPutThanPNLJ() {
        d.setWorkMem(4); // B=4
        try(Transaction transaction = d.beginTransaction()) {
            Record r1 = TestUtils.createRecordWithAllTypesWithValue(1);
            List<DataBox> r1Vals = r1.getValues();
            Record r2 = TestUtils.createRecordWithAllTypesWithValue(2);
            List<DataBox> r2Vals = r2.getValues();
            Record r3 = TestUtils.createRecordWithAllTypesWithValue(3);
            List<DataBox> r3Vals = r3.getValues();
            Record r4 = TestUtils.createRecordWithAllTypesWithValue(4);
            List<DataBox> r4Vals = r4.getValues();
            List<DataBox> expectedRecordValues1 = new ArrayList<>();
            List<DataBox> expectedRecordValues2 = new ArrayList<>();
            List<DataBox> expectedRecordValues3 = new ArrayList<>();
            List<DataBox> expectedRecordValues4 = new ArrayList<>();

            for (int i = 0; i < 2; i++) {
                expectedRecordValues1.addAll(r1Vals);
                expectedRecordValues2.addAll(r2Vals);
                expectedRecordValues3.addAll(r3Vals);
                expectedRecordValues4.addAll(r4Vals);
            }
            Record expectedRecord1 = new Record(expectedRecordValues1);
            Record expectedRecord2 = new Record(expectedRecordValues2);
            Record expectedRecord3 = new Record(expectedRecordValues3);
            Record expectedRecord4 = new Record(expectedRecordValues4);
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "leftTable");
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "rightTable");
            for (int i = 0; i < 2 * 400; i++) {
                if (i < 200) {
                    transaction.getTransactionContext().addRecord("leftTable", r1Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r3Vals);
                } else if (i < 400) {
                    transaction.getTransactionContext().addRecord("leftTable", r2Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r4Vals);
                } else if (i < 600) {
                    transaction.getTransactionContext().addRecord("leftTable", r3Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r1Vals);
                } else {
                    transaction.getTransactionContext().addRecord("leftTable", r4Vals);
                    transaction.getTransactionContext().addRecord("rightTable", r2Vals);
                }
            }

            setSourceOperators(
                new SequentialScanOperator(transaction.getTransactionContext(), "leftTable"),
                new SequentialScanOperator(transaction.getTransactionContext(), "rightTable")
            );

            startCountIOs();

            QueryOperator joinOperator = new BNLJOperator(leftSourceOperator, rightSourceOperator, "int", "int",
                    transaction.getTransactionContext());
            checkIOs(0);

            Iterator<Record> outputIterator = joinOperator.iterator();
            checkIOs(3);

            int count = 0;
            while (outputIterator.hasNext() && count < 4 * 200 * 200) {
                Record r = outputIterator.next();
                if (count < 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord3, r);
                } else if (count < 2 * 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord4, r);
                } else if (count < 3 * 200 * 200) {
                    assertEquals("mismatch at record " + count, expectedRecord1, r);
                } else {
                    assertEquals("mismatch at record " + count, expectedRecord2, r);
                }

                count++;

                if (count == 200 * 200 * 2) {
                    checkIOs("at record " + count, 1);
                } else {
                    checkIOs("at record " + count, 0);
                }
            }
            assertFalse("too many records", outputIterator.hasNext());
            assertEquals("too few records", 4 * 200 * 200, count);
        }
    }
}
