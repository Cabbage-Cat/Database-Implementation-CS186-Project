package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.Page;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

import edu.berkeley.cs186.database.table.Record;

import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import static org.junit.Assert.*;

@Category({Proj3Tests.class, Proj3Part1Tests.class})
public class TestSortOperator {
    private Database d;
    private long numIOs;
    private Map<Long, Page> pinnedPages = new HashMap<>();

    public static long FIRST_ACCESS_IOS = 1; // 1 I/O on first access to a table (after evictAll)
    public static long NEW_TABLE_IOS = 2; // 2 I/Os to create a new table

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 2 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                5000 * TimeoutScaling.factor)));

    @Ignore
    public static class SortRecordComparator implements Comparator<Record> {
        private int columnIndex;

        private SortRecordComparator(int columnIndex) {
            this.columnIndex = columnIndex;
        }
        @Override
        public int compare(Record o1, Record o2) {
            return o1.getValues().get(this.columnIndex).compareTo(
                       o2.getValues().get(this.columnIndex));
        }
    }

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("sortTest");
        d = new Database(tempDir.getAbsolutePath(), 256);
        d.setWorkMem(3); // B=3
        d.waitAllTransactions();
    }

    @After
    public void cleanup() {
        d.waitAllTransactions();
        for (Page p : pinnedPages.values()) {
            p.unpin();
        }
        d.close();
    }

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

    private void pinMetadata() {
        // hard-coded mess, but works as long as the first two tables created are the source operators
        pinPage(1, 0); // information_schema.tables header page
        pinPage(1, 3); // information_schema.tables entry for source
    }

    @Test
    @Category(PublicTests.class)
    public void testSortRun() {
        try(Transaction transaction = d.beginTransaction()) {
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "table");
            List<Record> records = new ArrayList<>();
            List<Record> recordsToShuffle = new ArrayList<>();
            for (int i = 0; i < 400 * 3; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                records.add(r);
                recordsToShuffle.add(r);
            }
            Collections.shuffle(recordsToShuffle, new Random(42));

            pinMetadata();
            startCountIOs();

            SortOperator s = new SortOperator(transaction.getTransactionContext(), "table",
                                              new SortRecordComparator(1));
            checkIOs(0);

            SortOperator.Run r = s.createRun();
            checkIOs(NEW_TABLE_IOS); // information_schema.tables row + header page of table

            r.addRecords(recordsToShuffle);
            checkIOs(3);

            startCountIOs();
            SortOperator.Run sortedRun = s.sortRun(r);
            checkIOs((3 + FIRST_ACCESS_IOS) + (3 + NEW_TABLE_IOS));

            Iterator<Record> iter = sortedRun.iterator();
            int i = 0;
            while (iter.hasNext() && i < 400 * 3) {
                assertEquals("mismatch at record " + i, records.get(i), iter.next());
                i++;
            }
            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", 400 * 3, i);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testMergeSortedRuns() {
        try(Transaction transaction = d.beginTransaction()) {
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "table");
            List<Record> records = new ArrayList<>();

            pinMetadata();
            startCountIOs();

            SortOperator s = new SortOperator(transaction.getTransactionContext(), "table",
                                              new SortRecordComparator(1));
            checkIOs(0);
            SortOperator.Run r1 = s.createRun();
            SortOperator.Run r2 = s.createRun();
            checkIOs(2 * NEW_TABLE_IOS);

            for (int i = 0; i < 400 * 3; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                records.add(r);
                if (i % 2 == 0) {
                    r1.addRecord(r.getValues());
                } else {
                    r2.addRecord(r.getValues());
                }
            }
            List<SortOperator.Run> runs = new ArrayList<>();
            runs.add(r1);
            runs.add(r2);
            checkIOs(2 * 2);

            startCountIOs();
            SortOperator.Run mergedSortedRuns = s.mergeSortedRuns(runs);
            checkIOs((2 * (2 + FIRST_ACCESS_IOS)) + (3 + NEW_TABLE_IOS));

            Iterator<Record> iter = mergedSortedRuns.iterator();
            int i = 0;
            while (iter.hasNext() && i < 400 * 3) {
                assertEquals("mismatch at record " + i, records.get(i), iter.next());
                i++;
            }
            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", 400 * 3, i);
            checkIOs(0);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testMergePass() {
        try(Transaction transaction = d.beginTransaction()) {
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "table");
            List<Record> records1 = new ArrayList<>();
            List<Record> records2 = new ArrayList<>();

            pinMetadata();
            startCountIOs();

            SortOperator s = new SortOperator(transaction.getTransactionContext(), "table",
                                              new SortRecordComparator(1));
            checkIOs(0);

            SortOperator.Run r1 = s.createRun();
            SortOperator.Run r2 = s.createRun();
            SortOperator.Run r3 = s.createRun();
            SortOperator.Run r4 = s.createRun();
            checkIOs(4 * NEW_TABLE_IOS);

            for (int i = 0; i < 400 * 4; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                if (i % 4 == 0) {
                    r1.addRecord(r.getValues());
                    records2.add(r);
                } else if (i % 4 == 1) {
                    r2.addRecord(r.getValues());
                    records1.add(r);
                } else if (i % 4 == 2) {
                    r3.addRecord(r.getValues());
                    records1.add(r);
                } else {
                    r4.addRecord(r.getValues());
                    records2.add(r);
                }
            }
            checkIOs(4 * 1);

            List<SortOperator.Run> runs = new ArrayList<>();
            runs.add(r3);
            runs.add(r2);
            runs.add(r1);
            runs.add(r4);

            startCountIOs();
            List<SortOperator.Run> result = s.mergePass(runs);
            assertEquals("wrong number of runs", 2, result.size());
            checkIOs((4 * (1 + FIRST_ACCESS_IOS)) + (2 * (2 + NEW_TABLE_IOS)));

            Iterator<Record> iter1 = result.get(0).iterator();
            Iterator<Record> iter2 = result.get(1).iterator();
            int i = 0;
            while (iter1.hasNext() && i < 400 * 2) {
                assertEquals("mismatch at record " + i, records1.get(i), iter1.next());
                i++;
            }
            assertFalse("too many records", iter1.hasNext());
            assertEquals("too few records", 400 * 2, i);
            i = 0;
            while (iter2.hasNext() && i < 400 * 2) {
                assertEquals("mismatch at record " + i, records2.get(i), iter2.next());
                i++;
            }
            assertFalse("too many records", iter2.hasNext());
            assertEquals("too few records", 400 * 2, i);
            checkIOs(0);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSortNoChange() {
        try(Transaction transaction = d.beginTransaction()) {
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "table");
            Record[] records = new Record[400 * 3];
            for (int i = 0; i < 400 * 3; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                records[i] = r;
                transaction.getTransactionContext().addRecord("table", r.getValues());
            }

            pinMetadata();
            startCountIOs();

            SortOperator s = new SortOperator(transaction.getTransactionContext(), "table",
                                              new SortRecordComparator(1));
            checkIOs(0);

            String sortedTableName = s.sort();
            checkIOs((3 + FIRST_ACCESS_IOS) + (3 + NEW_TABLE_IOS));

            Iterator<Record> iter = transaction.getTransactionContext().getRecordIterator(sortedTableName);
            int i = 0;
            while (iter.hasNext() && i < 400 * 3) {
                assertEquals("mismatch at record " + i, records[i], iter.next());
                i++;
            }
            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", 400 * 3, i);
            checkIOs(0);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSortBackwards() {
        try(Transaction transaction = d.beginTransaction()) {
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "table");
            Record[] records = new Record[400 * 3];
            for (int i = 400 * 3; i > 0; i--) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                records[i - 1] = r;
                transaction.getTransactionContext().addRecord("table", r.getValues());
            }

            pinMetadata();
            startCountIOs();

            SortOperator s = new SortOperator(transaction.getTransactionContext(), "table",
                                              new SortRecordComparator(1));
            checkIOs(0);

            String sortedTableName = s.sort();
            checkIOs((3 + FIRST_ACCESS_IOS) + (3 + NEW_TABLE_IOS));

            Iterator<Record> iter = transaction.getTransactionContext().getRecordIterator(sortedTableName);
            int i = 0;
            while (iter.hasNext() && i < 400 * 3) {
                assertEquals("mismatch at record " + i, records[i], iter.next());
                i++;
            }
            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", 400 * 3, i);
            checkIOs(0);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSortRandomOrder() {
        try(Transaction transaction = d.beginTransaction()) {
            transaction.createTable(TestUtils.createSchemaWithAllTypes(), "table");
            List<Record> records = new ArrayList<>();
            List<Record> recordsToShuffle = new ArrayList<>();
            for (int i = 0; i < 400 * 3; i++) {
                Record r = TestUtils.createRecordWithAllTypesWithValue(i);
                records.add(r);
                recordsToShuffle.add(r);
            }
            Collections.shuffle(recordsToShuffle, new Random(42));
            for (Record r : recordsToShuffle) {
                transaction.getTransactionContext().addRecord("table", r.getValues());
            }

            pinMetadata();
            startCountIOs();

            SortOperator s = new SortOperator(transaction.getTransactionContext(), "table",
                                              new SortRecordComparator(1));
            checkIOs(0);

            String sortedTableName = s.sort();
            checkIOs((3 + FIRST_ACCESS_IOS) + (3 + NEW_TABLE_IOS));

            Iterator<Record> iter = transaction.getTransactionContext().getRecordIterator(sortedTableName);
            int i = 0;
            while (iter.hasNext() && i < 400 * 3) {
                assertEquals("mismatch at record " + i, records.get(i), iter.next());
                i++;
            }
            assertFalse("too many records", iter.hasNext());
            assertEquals("too few records", 400 * 3, i);
            checkIOs(0);
        }
    }

}
