package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.concurrency.*;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordId;
import edu.berkeley.cs186.database.table.Schema;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.*;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@Category({HW4Tests.class, HW4Part2Tests.class})
public class TestDatabaseLocking {
    private static final String TestDir = "testDatabaseLocking";
    private static boolean passedPreCheck = false;
    private Database db;
    private LoggingLockManager lockManager;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    // 7 second max per method tested.
    public static long timeout = (long) (7000 * TimeoutScaling.factor);

    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis(timeout));

    private void reloadDatabase() {
        if (this.db != null) {
            while (TransactionContext.getTransaction() != null) {
                TransactionContext.unsetTransaction();
            }
            this.db.close();
        }
        if (this.lockManager != null && this.lockManager.isLogging()) {
            List<String> oldLog = this.lockManager.log;
            this.lockManager = new LoggingLockManager();
            this.lockManager.log = oldLog;
            this.lockManager.startLog();
        } else {
            this.lockManager = new LoggingLockManager();
        }
        this.db = new Database(this.filename, 128, this.lockManager);
        this.db.setWorkMem(32); // B=32
        // force initialization to finish before continuing
        this.db.waitSetupFinished();
        this.db.waitAllTransactions();
    }

    @ClassRule
    public static  TemporaryFolder checkFolder = new TemporaryFolder();

    @BeforeClass
    public static void beforeAll() {
        passedPreCheck = TestDatabaseDeadlockPrecheck.performCheck(checkFolder);
    }

    @Before
    public void beforeEach() throws Exception {
        assertTrue(passedPreCheck);

        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.reloadDatabase();
        try(Transaction t = this.beginTransaction()) {
            t.dropAllTables();
        } finally {
            this.db.waitAllTransactions();
        }
    }

    @After
    public void afterEach() {
        if (!passedPreCheck) {
            return;
        }

        this.lockManager.endLog();
        while (TransactionContext.getTransaction() != null) {
            TransactionContext.unsetTransaction();
        }
        this.db.close();
    }

    private Transaction beginTransaction() {
        // Database.Transaction ordinarily calls setTransaction/unsetTransaction around calls,
        // but we test directly with TransactionContext calls here, so we need to call setTransaction
        // manually
        Transaction t = db.beginTransaction();
        TransactionContext.setTransaction(t.getTransactionContext());
        return t;
    }

    private static <T extends Comparable<? super T>> void assertSameItems(List<T> expected,
            List<T> actual) {
        Collections.sort(expected);
        Collections.sort(actual);
        assertEquals(expected, actual);
    }

    private static <T> void assertSubsequence(List<T> expected, List<T> actual) {
        if (expected.size() == 0) {
            return;
        }
        Iterator<T> ei = expected.iterator();
        Iterator<T> ai = actual.iterator();
        while (ei.hasNext()) {
            T next = ei.next();
            boolean found = false;
            while (ai.hasNext()) {
                if (ai.next().equals(next)) {
                    found = true;
                    break;
                }
            }
            assertTrue(expected + " not subsequence of " + actual, found);
        }
    }

    private static <T> void assertContainsAll(List<T> expected, List<T> actual) {
        if (expected.size() == 0) {
            return;
        }
        for (T item : expected) {
            assertTrue(item + " not in " + actual, actual.contains(item));
        }
    }

    private static List<String> prepare(Long transNum, String ... expected) {
        return Arrays.stream(expected).map((String log) -> String.format(log,
                                           transNum)).collect(Collectors.toList());
    }

    private static List<String> removeMetadataLogs(List<String> log) {
        log = new ArrayList<>(log);
        // remove all information_schema lock log entries
        log.removeIf((String x) -> x.contains("information_schema"));
        // replace [acquire IS(database), promote IX(database), ...] with [acquire IX(database), ...]
        // (as if the information_schema locks never happened)
        if (log.size() >= 2 && log.get(0).endsWith("database IS") && log.get(1).endsWith("database IX")) {
            log.set(0, log.get(0).replace("IS", "IX"));
            log.remove(1);
        }
        return log;
    }

    private List<RecordId> createTable(String tableName, int pages) {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();
        List<DataBox> values = input.getValues();
        List<RecordId> rids = new ArrayList<>();
        try(Transaction t1 = beginTransaction()) {
            t1.createTable(s, tableName);
            int numRecords = pages * t1.getTransactionContext().getTable(tableName).getNumRecordsPerPage();
            for (int i = 0; i < numRecords; ++i) {
                rids.add(t1.getTransactionContext().addRecord(tableName, values));
            }
        } finally {
            this.db.waitAllTransactions();
        }

        return rids;
    }

    private List<RecordId> createTableWithIndices(String tableName, int pages,
            List<String> indexColumns) {
        Schema s = TestUtils.createSchemaWithTwoInts();
        List<RecordId> rids = new ArrayList<>();
        try(Transaction t1 = beginTransaction()) {
            t1.createTable(s, tableName);
            for (String col : indexColumns) {
                t1.createIndex(tableName, col, false);
            }
            int numRecords = pages * t1.getTransactionContext().getTable(tableName).getNumRecordsPerPage();
            for (int i = 0; i < numRecords; ++i) {
                rids.add(t1.getTransactionContext().addRecord(tableName, Arrays.asList(new IntDataBox(i),
                         new IntDataBox(i))));
            }
        } finally {
            this.db.waitAllTransactions();
        }

        return rids;
    }

    @Test
    @Category(PublicTests.class)
    public void testRecordRead() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);
        lockManager.startLog();

        try(Transaction t1 = beginTransaction()) {
            t1.getTransactionContext().getRecord(tableName, rids.get(0));
            t1.getTransactionContext().getRecord(tableName, rids.get(3 * rids.size() / 4 - 1));
            t1.getTransactionContext().getRecord(tableName, rids.get(rids.size() - 1));

            assertEquals(prepare(t1.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/tables.testTable1 IS",
                                 "acquire %s database/tables.testTable1/30000000001 S",
                                 "acquire %s database/tables.testTable1/30000000003 S",
                                 "acquire %s database/tables.testTable1/30000000004 S"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleTransactionCleanup() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        Transaction t1 = beginTransaction();
        try {
            t1.getTransactionContext().getRecord(tableName, rids.get(0));
            t1.getTransactionContext().getRecord(tableName, rids.get(3 * rids.size() / 4 - 1));
            t1.getTransactionContext().getRecord(tableName, rids.get(rids.size() - 1));

            assertTrue("did not acquire all required locks",
                       lockManager.getLocks(t1.getTransactionContext()).size() >= 5);

            lockManager.startLog();
        } finally {
            t1.commit();
            this.db.waitAllTransactions();
        }

        assertTrue("did not free all required locks",
                   lockManager.getLocks(t1.getTransactionContext()).isEmpty());
        assertSubsequence(prepare(t1.getTransNum(),
                                  "release %s database/tables.testTable1/30000000003",
                                  "release %s database"
                                 ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testRecordWrite() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);
        Record input = TestUtils.createRecordWithAllTypes();
        List<DataBox> values = input.getValues();

        try(Transaction t0 = beginTransaction()) {
            t0.getTransactionContext().deleteRecord(tableName, rids.get(rids.size() - 1));
        } finally {
            this.db.waitAllTransactions();
        }

        lockManager.startLog();

        try(Transaction t1 = beginTransaction()) {
            t1.getTransactionContext().addRecord(tableName, values);

            assertEquals(prepare(t1.getTransNum(),
                                 "acquire %s database IX",
                                 "acquire %s database/tables.testTable1 IX",
                                 "acquire %s database/tables.testTable1/30000000004 X"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testRecordUpdate() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);
        Record input = TestUtils.createRecordWithAllTypes();
        List<DataBox> values = input.getValues();

        lockManager.startLog();

        try(Transaction t1 = beginTransaction()) {
            t1.getTransactionContext().updateRecord(tableName, values, rids.get(rids.size() - 1));

            assertEquals(prepare(t1.getTransNum(),
                                 "acquire %s database IX",
                                 "acquire %s database/tables.testTable1 IX",
                                 "acquire %s database/tables.testTable1/30000000004 X"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testRecordDelete() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 4);

        lockManager.startLog();

        try(Transaction t1 = beginTransaction()) {
            t1.getTransactionContext().deleteRecord(tableName, rids.get(rids.size() - 1));

            assertEquals(prepare(t1.getTransNum(),
                                 "acquire %s database IX",
                                 "acquire %s database/tables.testTable1 IX",
                                 "acquire %s database/tables.testTable1/30000000004 X"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testTableScan() {
        String tableName = "testTable1";
        createTable(tableName, 4);

        lockManager.startLog();

        try(Transaction t1 = beginTransaction()) {
            Iterator<Record> r = t1.getTransactionContext().getRecordIterator(tableName);
            while (r.hasNext()) {
                r.next();
            }

            assertEquals(prepare(t1.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/tables.testTable1 S"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSortedScanNoIndexLocking() {
        String tableName = "testTable1";
        createTable(tableName, 1);

        lockManager.startLog();

        try(Transaction t1 = beginTransaction()) {
            Iterator<Record> r = t1.getTransactionContext().sortedScan(tableName, "int");
            while (r.hasNext()) {
                r.next();
            }

            assertEquals(prepare(t1.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/tables.testTable1 S"
                                ), removeMetadataLogs(lockManager.log).subList(0, 2));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testBPlusTreeRestrict() {
        String tableName = "testTable1";
        lockManager.startLog();
        createTableWithIndices(tableName, 0, Collections.singletonList("int1"));
        assertTrue(lockManager.log.contains("disable-children database/indices.testTable1,int1"));
    }

    @Test
    @Category(PublicTests.class)
    public void testSortedScanLocking() {
        String tableName = "testTable1";
        List<RecordId> rids = createTableWithIndices(tableName, 1, Arrays.asList("int1", "int2"));

        lockManager.startLog();
        try(Transaction t1 = beginTransaction()) {
            Iterator<Record> r = t1.getTransactionContext().sortedScan(tableName, "int1");
            while (r.hasNext()) {
                r.next();
            }
            List<String> log = removeMetadataLogs(lockManager.log);
            assertEquals(3, log.size());
            assertEquals(prepare(t1.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/tables.testTable1 S",
                                 "acquire %s database/indices.testTable1,int1 S"
                                ), log);
        } finally {
            this.db.waitAllTransactions();
        }

        lockManager.clearLog();
        try(Transaction t2 = beginTransaction()) {
            Iterator<Record> r = t2.getTransactionContext().sortedScanFrom(tableName, "int2",
                                 new IntDataBox(rids.size() / 2));
            while (r.hasNext()) {
                r.next();
            }
            List<String> log = removeMetadataLogs(lockManager.log);
            assertEquals(3, log.size());
            assertEquals(prepare(t2.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/tables.testTable1 S",
                                 "acquire %s database/indices.testTable1,int2 S"
                                ), log);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSearchOperationLocking() {
        String tableName = "testTable1";
        List<RecordId> rids = createTableWithIndices(tableName, 1, Arrays.asList("int1", "int2"));

        lockManager.startLog();
        try(Transaction t1 = beginTransaction()) {
            t1.getTransactionContext().lookupKey(tableName, "int1", new IntDataBox(rids.size() / 2));
            assertEquals(prepare(t1.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/indices.testTable1,int1 S"
                                ), removeMetadataLogs(lockManager.log));
        } finally {
            this.db.waitAllTransactions();
        }

        lockManager.clearLog();
        try(Transaction t2 = beginTransaction()) {
            t2.getTransactionContext().contains(tableName, "int2", new IntDataBox(rids.size() / 2 - 1));
            assertEquals(prepare(t2.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/indices.testTable1,int2 S"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testQueryWithIndex() {
        String tableName = "testTable1";
        createTableWithIndices(tableName, 6, Arrays.asList("int1", "int2"));

        try (Transaction ts = beginTransaction()) {
            ts.getTransactionContext().getTable("testTable1").buildStatistics(10);
        }

        db.waitAllTransactions();
        lockManager.startLog();

        try(Transaction t0 = beginTransaction()) {
            QueryPlan q = t0.query(tableName);
            q.select("int1", PredicateOperator.EQUALS, new IntDataBox(2));
            q.project(Collections.singletonList("int2"));
            q.execute();

            assertEquals(prepare(t0.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/indices.testTable1,int1 S"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testPageDirectoryCapacityLoad() {
        String tableName = "testTable1";
        createTable(tableName, 0);

        while (TransactionContext.getTransaction() != null) {
            TransactionContext.unsetTransaction();
        }
        db.close();
        db = null;

        this.lockManager = new LoggingLockManager();
        lockManager.startLog();

        this.reloadDatabase();
        assertTrue(lockManager.log.contains("set-capacity database/tables.testTable1 0"));
    }

    @Test
    @Category(PublicTests.class)
    public void testAutoEscalateS() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 18);

        lockManager.startLog();

        try(Transaction t0 = beginTransaction()) {
            t0.getTransactionContext().getRecord(tableName, rids.get(0));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() / 5));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() / 5 * 2));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() / 5 * 3));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() - 1));

            assertEquals(prepare(t0.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/tables.testTable1 IS",
                                 "acquire %s database/tables.testTable1/30000000001 S",
                                 "acquire %s database/tables.testTable1/30000000004 S",
                                 "acquire %s database/tables.testTable1/30000000008 S",
                                 "acquire %s database/tables.testTable1/30000000011 S",
                                 "acquire-and-release %s database/tables.testTable1 S [database/tables.testTable1, " +
                                 "database/tables.testTable1/30000000001, database/tables.testTable1/30000000004, " +
                                 "database/tables.testTable1/30000000008, database/tables.testTable1/30000000011]"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testAutoEscalateX() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 18);
        List<DataBox> values = TestUtils.createRecordWithAllTypes().getValues();

        lockManager.startLog();

        try(Transaction t0 = beginTransaction()) {
            t0.getTransactionContext().updateRecord(tableName, values, rids.get(0));
            t0.getTransactionContext().deleteRecord(tableName, rids.get(rids.size() / 5));
            t0.getTransactionContext().updateRecord(tableName, values, rids.get(rids.size() / 5 * 2));
            t0.getTransactionContext().deleteRecord(tableName, rids.get(rids.size() / 5 * 3));
            t0.getTransactionContext().updateRecord(tableName, values, rids.get(rids.size() - 1));

            assertEquals(prepare(t0.getTransNum(),
                                 "acquire %s database IX",
                                 "acquire %s database/tables.testTable1 IX",
                                 "acquire %s database/tables.testTable1/30000000001 X",
                                 "acquire %s database/tables.testTable1/30000000004 X",
                                 "acquire %s database/tables.testTable1/30000000008 X",
                                 "acquire %s database/tables.testTable1/30000000011 X",
                                 "acquire-and-release %s database/tables.testTable1 X [database/tables.testTable1, " +
                                 "database/tables.testTable1/30000000001, database/tables.testTable1/30000000004, " +
                                 "database/tables.testTable1/30000000008, database/tables.testTable1/30000000011]"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testAutoEscalateSIX() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 18);
        List<DataBox> values = TestUtils.createRecordWithAllTypes().getValues();

        lockManager.startLog();

        try(Transaction t0 = beginTransaction()) {
            t0.getTransactionContext().getRecord(tableName, rids.get(0));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() / 5));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() / 5 * 2));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() / 5 * 3));
            t0.getTransactionContext().updateRecord(tableName, values, rids.get(rids.size() - 1));

            assertEquals(prepare(t0.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/tables.testTable1 IS",
                                 "acquire %s database/tables.testTable1/30000000001 S",
                                 "acquire %s database/tables.testTable1/30000000004 S",
                                 "acquire %s database/tables.testTable1/30000000008 S",
                                 "acquire %s database/tables.testTable1/30000000011 S",
                                 "acquire-and-release %s database/tables.testTable1 S [database/tables.testTable1, " +
                                 "database/tables.testTable1/30000000001, database/tables.testTable1/30000000004, " +
                                 "database/tables.testTable1/30000000008, database/tables.testTable1/30000000011]",
                                 "promote %s database IX",
                                 "acquire-and-release %s database/tables.testTable1 SIX [database/tables.testTable1]",
                                 "acquire %s database/tables.testTable1/30000000018 X"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testAutoEscalateDisabled() {
        String tableName = "testTable1";
        List<RecordId> rids = createTable(tableName, 18);

        lockManager.startLog();

        try(Transaction t0 = beginTransaction()) {
            t0.getTransactionContext().getTable(tableName).disableAutoEscalate();

            t0.getTransactionContext().getRecord(tableName, rids.get(0));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() / 5));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() / 5 * 2));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() / 5 * 3));
            t0.getTransactionContext().getRecord(tableName, rids.get(rids.size() - 1));

            assertEquals(prepare(t0.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/tables.testTable1 IS",
                                 "acquire %s database/tables.testTable1/30000000001 S",
                                 "acquire %s database/tables.testTable1/30000000004 S",
                                 "acquire %s database/tables.testTable1/30000000008 S",
                                 "acquire %s database/tables.testTable1/30000000011 S",
                                 "acquire %s database/tables.testTable1/30000000018 S"
                                ), removeMetadataLogs(lockManager.log));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testLockTableMetadata() {
        createTable("testTable1", 4);

        lockManager.startLog();

        try(Transaction t = beginTransaction()) {
            TransactionContext.setTransaction(t.getTransactionContext());

            db.lockTableMetadata("tables.testTable1", LockType.S);

            assertEquals(prepare(t.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/information_schema.tables IS",
                                 "acquire %s database/information_schema.tables/10000000003 S"
                                ), lockManager.log);

            TransactionContext.unsetTransaction();
        }

        db.waitAllTransactions();
        lockManager.clearLog();
        lockManager.startLog();

        try(Transaction t = beginTransaction()) {
            TransactionContext.setTransaction(t.getTransactionContext());

            db.lockTableMetadata("tables.testTable1", LockType.X);

            assertEquals(prepare(t.getTransNum(),
                                 "acquire %s database IX",
                                 "acquire %s database/information_schema.tables IX",
                                 "acquire %s database/information_schema.tables/10000000003 X"
                                ), lockManager.log);

            TransactionContext.unsetTransaction();
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testLockIndexMetadata() {
        createTableWithIndices("testTable1", 4, Collections.singletonList("int1"));

        lockManager.startLog();

        try(Transaction t = beginTransaction()) {
            TransactionContext.setTransaction(t.getTransactionContext());

            db.lockIndexMetadata("testTable1,int1", LockType.S);

            assertEquals(prepare(t.getTransNum(),
                                 "acquire %s database IS",
                                 "acquire %s database/information_schema.indices IS",
                                 "acquire %s database/information_schema.indices/20000000001 S"
                                ), lockManager.log);

            TransactionContext.unsetTransaction();
        }

        db.waitAllTransactions();
        lockManager.clearLog();
        lockManager.startLog();

        try(Transaction t = beginTransaction()) {
            TransactionContext.setTransaction(t.getTransactionContext());

            db.lockIndexMetadata("testTable1,int1", LockType.X);

            assertEquals(prepare(t.getTransNum(),
                                 "acquire %s database IX",
                                 "acquire %s database/information_schema.indices IX",
                                 "acquire %s database/information_schema.indices/20000000001 X"
                                ), lockManager.log);

            TransactionContext.unsetTransaction();
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testTableMetadataLockOnUse() {
        lockManager.startLog();
        lockManager.suppressStatus(true);

        try(Transaction t = beginTransaction()) {
            try {
                t.getTransactionContext().getSchema("badTable");
            } catch (DatabaseException e) { /* do nothing */ }

            assertEquals(prepare(t.getTransNum(),
                                 "acquire %s database IX",
                                 "acquire %s database/information_schema.tables IX",
                                 "acquire %s database/information_schema.tables/10000000003 X"
                                ), lockManager.log);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testCreateTableSimple() {
        try(Transaction t = beginTransaction()) {
            try {
                t.getTransactionContext().getNumDataPages("testTable1");
            } catch (DatabaseException e) { /* do nothing */ }
        }
        db.waitAllTransactions();

        lockManager.startLog();
        createTable("testTable1", 4);

        try(Transaction t = beginTransaction()) {
            for (String x : lockManager.log) {
                System.out.println(x);
            }
            assertSubsequence(prepare(t.getTransNum() - 1,
                                      "acquire %s database IX",
                                      "acquire %s database/information_schema.tables IX",
                                      "acquire %s database/information_schema.tables/10000000003 X",
                                      "acquire %s database/tables.testTable1 X"
                                     ), lockManager.log);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testCreateIndexSimple() {
        createTableWithIndices("testTable1", 4, Collections.emptyList());

        try(Transaction t = beginTransaction()) {
            try {
                t.getTransactionContext().getTreeHeight("testTable1", "int1");
            } catch (DatabaseException e) { /* do nothing */ }
        }
        db.waitAllTransactions();

        lockManager.startLog();

        try(Transaction t = beginTransaction()) {
            t.createIndex("testTable1", "int1", false);
            assertSubsequence(prepare(t.getTransNum(),
                                      "acquire %s database/information_schema.tables IS",
                                      "acquire %s database/information_schema.tables/10000000003 S",
                                      "acquire %s database/information_schema.indices IX",
                                      "acquire %s database/information_schema.indices/20000000001 X",
                                      "acquire %s database/indices.testTable1,int1 X"
                                     ), lockManager.log);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testDropTableSimple() {
        String tableName = "testTable1";
        createTable(tableName, 0);
        lockManager.startLog();
        lockManager.suppressStatus(true);

        try(Transaction t0 = beginTransaction()) {
            t0.dropTable(tableName);

            assertEquals(prepare(t0.getTransNum(),
                                 "acquire %s database IX",
                                 "acquire %s database/information_schema.tables IX",
                                 "acquire %s database/information_schema.tables/10000000003 X",
                                 "acquire %s database/tables.testTable1 X"
                                ), lockManager.log);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testDropIndexSimple() {
        createTableWithIndices("testTable1", 4, Collections.singletonList("int1"));
        lockManager.startLog();
        lockManager.suppressStatus(true);

        try(Transaction t0 = beginTransaction()) {
            t0.dropIndex("testTable1", "int1");

            assertSubsequence(prepare(t0.getTransNum(),
                                      "acquire %s database IX",
                                      "acquire %s database/information_schema.indices IX",
                                      "acquire %s database/information_schema.indices/20000000001 X",
                                      "acquire %s database/indices.testTable1,int1 X"
                                     ), lockManager.log);
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testDropAllTables() {
        lockManager.startLog();

        try(Transaction t0 = beginTransaction()) {
            t0.dropAllTables();

            assertEquals(prepare(t0.getTransNum(),
                                 "acquire %s database X"
                                ), lockManager.log);
        }
    }
}
