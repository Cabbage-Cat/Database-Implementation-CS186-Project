package edu.berkeley.cs186.database.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.memory.Page;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.IntDataBox;

@Category({Proj99Tests.class, SystemTests.class})
public class TestTable {
    private static final String TABLENAME = "testtable";
    private MemoryHeapFile heapFile;
    private Table table;
    private Schema schema;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void beforeEach() {
        this.schema = TestUtils.createSchemaWithAllTypes();
        this.heapFile = new MemoryHeapFile();
        this.table = new Table(TABLENAME, schema, heapFile, new DummyLockContext());
    }

    @After
    public void cleanup() {
        this.heapFile.close();
    }

    private static Record createRecordWithAllTypes(int i) {
        Record r = TestUtils.createRecordWithAllTypes();
        r.getValues().set(1, new IntDataBox(i));
        return r;
    }

    @Test
    public void testGetNumRecordsPerPage() {
        assertEquals(10, schema.getSizeInBytes());
        assertEquals(4060, heapFile.getEffectivePageSize());
        // 50 + (400 * 10) = 4050
        // 51 + (408 * 10) = 4131
        assertEquals(400, table.getNumRecordsPerPage());
    }

    @Test
    public void testSingleInsertAndGet() {
        Record r = createRecordWithAllTypes(0);
        RecordId rid = table.addRecord(r.getValues());
        assertEquals(r, table.getRecord(rid));
    }

    @Test
    public void testThreePagesOfInserts() {
        List<RecordId> rids = new ArrayList<>();
        for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
            Record r = createRecordWithAllTypes(i);
            rids.add(table.addRecord(r.getValues()));
        }

        for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
            Record r = createRecordWithAllTypes(i);
            assertEquals(r, table.getRecord(rids.get(i)));
        }
    }

    @Test
    public void testSingleDelete() {
        Record r = createRecordWithAllTypes(0);
        RecordId rid = table.addRecord(r.getValues());
        assertEquals(r, table.deleteRecord(rid));
    }

    @Test
    public void testThreePagesOfDeletes() {
        List<RecordId> rids = new ArrayList<>();
        for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
            Record r = createRecordWithAllTypes(i);
            rids.add(table.addRecord(r.getValues()));
        }

        for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
            Record r = createRecordWithAllTypes(i);
            assertEquals(r, table.deleteRecord(rids.get(i)));
        }
    }

    @Test(expected = DatabaseException.class)
    public void testGetDeletedRecord() {
        Record r = createRecordWithAllTypes(0);
        RecordId rid = table.addRecord(r.getValues());
        table.deleteRecord(rid);
        table.getRecord(rid);
    }

    @Test
    public void testUpdateSingleRecord() {
        Record rOld = createRecordWithAllTypes(0);
        Record rNew = createRecordWithAllTypes(42);

        RecordId rid = table.addRecord(rOld.getValues());
        assertEquals(rOld, table.updateRecord(rNew.getValues(), rid));
        assertEquals(rNew, table.getRecord(rid));
    }

    @Test
    public void testThreePagesOfUpdates() {
        List<RecordId> rids = new ArrayList<>();
        for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
            Record r = createRecordWithAllTypes(i);
            rids.add(table.addRecord(r.getValues()));
        }

        for (int i = 0; i < table.getNumRecordsPerPage() * 3; ++i) {
            Record rOld = createRecordWithAllTypes(i);
            Record rNew = createRecordWithAllTypes(i * 10000);
            assertEquals(rOld, table.updateRecord(rNew.getValues(), rids.get(i)));
            assertEquals(rNew, table.getRecord(rids.get(i)));
        }
    }

    @Test
    public void testReloadTable()  {
        // We add 42 to make sure we have some incomplete pages.
        int numRecords = table.getNumRecordsPerPage() * 2 + 42;

        List<RecordId> rids = new ArrayList<>();
        for (int i = 0; i < numRecords; ++i) {
            Record r = createRecordWithAllTypes(i);
            rids.add(table.addRecord(r.getValues()));
        }

        table = new Table(table.getName(), table.getSchema(), heapFile, new DummyLockContext());
        for (int i = 0; i < numRecords; ++i) {
            Record r = createRecordWithAllTypes(i);
            assertEquals(r, table.getRecord(rids.get(i)));
        }
    }

    @Test
    public void testReloadTableThenWriteMoreRecords() {
        // We add 42 to make sure we have some incomplete pages.
        int numRecords = table.getNumRecordsPerPage() * 2 + 42;

        List<RecordId> rids = new ArrayList<>();
        for (int i = 0; i < numRecords; ++i) {
            Record r = createRecordWithAllTypes(i);
            rids.add(table.addRecord(r.getValues()));
        }

        table = new Table(table.getName(), table.getSchema(), heapFile, new DummyLockContext());
        for (int i = numRecords; i < 2 * numRecords; ++i) {
            Record r = createRecordWithAllTypes(i);
            rids.add(table.addRecord(r.getValues()));
        }

        for (int i = 0; i < 2 * numRecords; ++i) {
            Record r = createRecordWithAllTypes(i);
            assertEquals(r, table.getRecord(rids.get(i)));
        }
    }

    /**
     * Loads some number of pages of records. rids will be loaded with all the record IDs
     * of the new records, and the number of records will be returned.
     */
    private int setupIteratorTest(List<RecordId> rids, int pages) throws DatabaseException {
        int numRecords = table.getNumRecordsPerPage() * pages;

        // Write the records.
        for (int i = 0; i < numRecords; ++i) {
            Record r = createRecordWithAllTypes(i);
            RecordId rid = table.addRecord(r.getValues());
            rids.add(rid);
        }

        return numRecords;
    }

    /**
     * See above; this overload should be used when the list of record IDs is not
     * needed.
     */
    private int setupIteratorTest(int pages) throws DatabaseException {
        List<RecordId> rids = new ArrayList<>();
        return setupIteratorTest(rids, pages);
    }

    /**
     * Performs a simple loop checking (end - start)/incr records from iter, and
     * assuming values of recordWithAllTypes(i), where start <= i < end and
     * i increments by incr.
     */
    private void checkSequentialRecords(int start, int end, int incr,
                                        BacktrackingIterator<Record> iter) {
        for (int i = start; i < end; i += incr) {
            assertTrue(iter.hasNext());
            assertEquals(createRecordWithAllTypes(i), iter.next());
        }
    }

    /**
     * Basic test over a full page of records to check that next/hasNext work.
     */
    @Test
    public void testRIDPageIterator() throws DatabaseException {
        int numRecords = setupIteratorTest(1);
        Iterator<Page> pages = table.pageIterator();
        Page page = pages.next();

        BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDPageIterator(page));
        checkSequentialRecords(0, numRecords, 1, iter);
        assertFalse(iter.hasNext());
    }

    /**
     * Basic test over a half-full page of records, with a missing first/last
     * record and gaps between every record, to check that next/hasNext work.
     */
    @Test
    public void testRIDPageIteratorWithGaps() throws DatabaseException {
        List<RecordId> rids = new ArrayList<>();
        int numRecords = setupIteratorTest(rids, 1);

        // Delete every other record and the last record.
        for (int i = 0; i < numRecords - 1; i += 2) {
            table.deleteRecord(rids.get(i));
        }
        table.deleteRecord(rids.get(numRecords - 1));

        Iterator<Page> pages = table.pageIterator();
        Page page = pages.next();

        BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDPageIterator(page));
        checkSequentialRecords(1, numRecords - 1, 2, iter);
        assertFalse(iter.hasNext());
    }

    /**
     * Basic test making sure that RIDPageIterator handles mark/reset properly.
     */
    @Test
    public void testRIDPageIteratorMarkReset() throws DatabaseException {
        int numRecords = setupIteratorTest(1);
        Iterator<Page> pages = table.pageIterator();
        Page page = pages.next();

        BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDPageIterator(page));
        checkSequentialRecords(0, numRecords / 2, 1, iter);
        iter.markPrev();
        checkSequentialRecords(numRecords / 2, numRecords, 1, iter);
        assertFalse(iter.hasNext());
        iter.reset();
        // -1 because the record before the mark must also be returned
        checkSequentialRecords(numRecords / 2 - 1, numRecords, 1, iter);
        assertFalse(iter.hasNext());

        // resetting twice to the same mark should be fine.
        iter.reset();
        checkSequentialRecords(numRecords / 2 - 1, numRecords, 1, iter);
        assertFalse(iter.hasNext());
    }

    /**
     * Extra test making sure that RIDPageIterator handles mark/reset properly.
     */
    @Test
    public void testRIDPageIteratorMarkResetExtra() throws DatabaseException {
        int numRecords = setupIteratorTest(1);
        Iterator<Page> pages = table.pageIterator();
        Page page = pages.next();

        BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDPageIterator(page));
        // This should do nothing.
        iter.reset();
        checkSequentialRecords(0, numRecords, 1, iter);
        assertFalse(iter.hasNext());

        page.pin();
        iter = new RecordIterator(table, table.new RIDPageIterator(page));
        // This should also do nothing.
        iter.markPrev();
        iter.reset();
        checkSequentialRecords(0, numRecords, 1, iter);
        assertFalse(iter.hasNext());

        // No effective mark = no reset.
        iter.reset();
        assertFalse(iter.hasNext());

        // mark last record
        iter.markPrev();
        iter.reset();
        checkSequentialRecords(numRecords - 1, numRecords, 1, iter);
        assertFalse(iter.hasNext());
    }

    /**
     * Basic test making sure that RIDPageIterator handles mark/reset properly,
     * but with gaps between each record.
     */
    @Test
    public void testRIDPageIteratorMarkResetWithGaps() throws DatabaseException {
        List<RecordId> rids = new ArrayList<>();
        int numRecords = setupIteratorTest(rids, 1);

        // Delete every other record and the last record.
        for (int i = 0; i < numRecords - 1; i += 2) {
            table.deleteRecord(rids.get(i));
        }
        table.deleteRecord(rids.get(numRecords - 1));

        Iterator<Page> pages = table.pageIterator();
        Page page = pages.next();

        BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDPageIterator(page));

        int stop = numRecords / 2;
        if (stop % 2 == 0) {
            ++stop;
        }
        checkSequentialRecords(1, stop, 2, iter);
        iter.markPrev();
        checkSequentialRecords(stop, numRecords - 1, 2, iter);
        assertFalse(iter.hasNext());
        iter.reset();
        // -2 because the record before the mark must also be returned
        checkSequentialRecords(stop - 2, numRecords - 1, 2, iter);
        assertFalse(iter.hasNext());

        // resetting twice to the same mark should be fine.
        iter.reset();
        checkSequentialRecords(stop - 2, numRecords - 1, 2, iter);
        assertFalse(iter.hasNext());
    }

    /**
     * Extra test making sure that RIDPageIterator handles mark/reset properly,
     * but with gaps between each record.
     */
    @Test
    public void testRIDPageIteratorMarkResetWithGapsExtra() throws DatabaseException {
        List<RecordId> rids = new ArrayList<>();
        int numRecords = setupIteratorTest(rids, 1);

        // Delete every other record and the last record.
        for (int i = 0; i < numRecords - 1; i += 2) {
            table.deleteRecord(rids.get(i));
        }
        table.deleteRecord(rids.get(numRecords - 1));

        Iterator<Page> pages = table.pageIterator();
        Page page = pages.next();

        BacktrackingIterator<Record> iter = new RecordIterator(table, table.new RIDPageIterator(page));
        // This should do nothing.
        iter.reset();
        checkSequentialRecords(1, numRecords - 1, 2, iter);
        assertFalse(iter.hasNext());

        page.pin();
        iter = new RecordIterator(table, table.new RIDPageIterator(page));
        // This should also do nothing.
        iter.markPrev();
        iter.reset();
        checkSequentialRecords(1, numRecords - 1, 2, iter);
        assertFalse(iter.hasNext());

        // No effective mark = no reset.
        iter.reset();
        assertFalse(iter.hasNext());

        // mark last record
        iter.markPrev();
        iter.reset();
        // check last record
        checkSequentialRecords(numRecords - 3, numRecords - 1, 2, iter);
        assertFalse(iter.hasNext());
    }

    /**
     * Simple test of TableIterator over three pages of records with no gaps.
     */
    @Test
    public void testTableIterator() {
        // We add 42 to make sure we have some incomplete pages.
        int numRecords = table.getNumRecordsPerPage() * 2 + 42;

        // Write the records.
        for (int i = 0; i < numRecords; ++i) {
            Record r = createRecordWithAllTypes(i);
            table.addRecord(r.getValues());
        }

        // Iterate once.
        BacktrackingIterator<Record> iter = table.iterator();
        checkSequentialRecords(0, numRecords, 1, iter);
        assertFalse(iter.hasNext());

        // Iterate twice for good measure.
        iter = table.iterator();
        checkSequentialRecords(0, numRecords, 1, iter);
        assertFalse(iter.hasNext());
    }

    /**
     * Simple test of TableIterator over three pages of records with every other
     * record missing.
     */
    @Test
    public void testTableIteratorWithGaps() {
        // We add 42 to make sure we have some incomplete pages.
        int numRecords = table.getNumRecordsPerPage() * 2 + 42;

        // Write the records.
        List<RecordId> rids = new ArrayList<>();
        for (int i = 0; i < numRecords; ++i) {
            Record r = createRecordWithAllTypes(i);
            rids.add(table.addRecord(r.getValues()));
        }

        // Delete every other record.
        for (int i = 0; i < numRecords; i += 2) {
            table.deleteRecord(rids.get(i));
        }

        // Iterate.
        BacktrackingIterator<Record> iter = table.iterator();
        checkSequentialRecords(1, numRecords, 2, iter);
        assertFalse(iter.hasNext());
    }
}
