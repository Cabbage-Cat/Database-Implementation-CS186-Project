package edu.berkeley.cs186.database.table;

import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.*;
import edu.berkeley.cs186.database.common.Bits;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.stats.TableStats;

/**
 * # Overview
 * A Table represents a database table with which users can insert, get,
 * update, and delete records:
 *
 *   // Create a brand new table t(x: int, y: int) which is persisted in the
 *   // the heap file heapFile.
 *   List<String> fieldNames = Arrays.asList("x", "y");
 *   List<String> fieldTypes = Arrays.asList(Type.intType(), Type.intType());
 *   Schema schema = new Schema(fieldNames, fieldTypes);
 *   Table t = new Table("t", schema, heapFile, new DummyLockContext());
 *
 *   // Insert, get, update, and delete records.
 *   List<DataBox> a = Arrays.asList(new IntDataBox(1), new IntDataBox(2));
 *   List<DataBox> b = Arrays.asList(new IntDataBox(3), new IntDataBox(4));
 *   RecordId rid = t.addRecord(a);
 *   Record ra = t.getRecord(rid);
 *   t.updateRecord(b, rid);
 *   Record rb = t.getRecord(rid);
 *   t.deleteRecord(rid);
 *
 * # Persistence
 * Every table is persisted in its own HeapFile object (passed into the constructor),
 * which interfaces with the BufferManager and DiskSpaceManager to save it to disk.
 *
 * A table can be loaded again by simply constructing it with the same parameters.
 *
 * # Storage Format
 * Now, we discuss how tables serialize their data.
 *
 * All pages are data pages - there are no header pages, because all metadata is
 * stored elsewhere (as rows in the information_schema.tables table). Every data
 * page begins with a n-byte bitmap followed by m records. The bitmap indicates
 * which records in the page are valid. The values of n and m are set to maximize the
 * number of records per page (see computeDataPageNumbers for details).
 *
 * For example, here is a cartoon of what a table's file would look like if we
 * had 5-byte pages and 1-byte records:
 *
 *          +----------+----------+----------+----------+----------+ \
 *   Page 0 | 1001xxxx | 01111010 | xxxxxxxx | xxxxxxxx | 01100001 |  |
 *          +----------+----------+----------+----------+----------+  |
 *   Page 1 | 1101xxxx | 01110010 | 01100100 | xxxxxxxx | 01101111 |  |- data
 *          +----------+----------+----------+----------+----------+  |
 *   Page 2 | 0011xxxx | xxxxxxxx | xxxxxxxx | 01111010 | 00100001 |  |
 *          +----------+----------+----------+----------+----------+ /
 *           \________/ \________/ \________/ \________/ \________/
 *            bitmap     record 0   record 1   record 2   record 3
 *
 *  - The first page (Page 0) is a data page. The first byte of this data page
 *    is a bitmap, and the next four bytes are each records. The first and
 *    fourth bit are set indicating that record 0 and record 3 are valid.
 *    Record 1 and record 2 are invalid, so we ignore their contents.
 *    Similarly, the last four bits of the bitmap are unused, so we ignore
 *    their contents.
 *  - The second and third page (Page 1 and 2) are also data pages and are
 *    formatted similar to Page 0.
 *
 *  When we add a record to a table, we add it to the very first free slot in
 *  the table. See addRecord for more information.
 *
 * Some tables have large records. In order to efficiently handle tables with
 * large records (that still fit on a page), we format these tables a bit differently,
 * by giving each record a full page. Tables with full page records do not have a bitmap.
 * Instead, each allocated page is a single record, and we indicate that a page does
 * not contain a record by simply freeing the page.
 *
 * In some cases, this behavior may be desirable even for small records (our database
 * only supports locking at the page level, so in cases where tuple-level locks are
 * necessary even at the cost of an I/O per tuple, a full page record may be desirable),
 * and may be explicitly toggled on with the setFullPageRecords method.
 */
public class Table implements BacktrackingIterable<Record> {
    // The name of the table.
    private String name;

    // The schema of the table.
    private Schema schema;

    // The page directory persisting the table.
    private HeapFile heapFile;

    // The size (in bytes) of the bitmap found at the beginning of each data page.
    private int bitmapSizeInBytes;

    // The number of records on each data page.
    private int numRecordsPerPage;

    // Statistics about the contents of the database.
    private TableStats stats;

    // The number of records in the table.
    private long numRecords;

    // The lock context of the table.
    private LockContext lockContext;

    boolean autoEscalate = true;

    // Constructors //////////////////////////////////////////////////////////////
    /**
     * Load a table named `name` with schema `schema` from `heapFile`. `lockContext`
     * is the lock context of the table (use a DummyLockContext() to disable locking). A
     * new table will be created if none exists on the heapfile.
     */
    public Table(String name, Schema schema, HeapFile heapFile, LockContext lockContext) {
        // TODO(proj4_part3): table locking code
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);
        this.name = name;
        this.heapFile = heapFile;
        this.schema = schema;
        this.bitmapSizeInBytes = computeBitmapSizeInBytes(heapFile.getEffectivePageSize(), schema);
        this.numRecordsPerPage = computeNumRecordsPerPage(heapFile.getEffectivePageSize(), schema);
        // mark everything that is not used for records as metadata
        this.heapFile.setEmptyPageMetadataSize((short) (heapFile.getEffectivePageSize() - numRecordsPerPage
                                               * schema.getSizeInBytes()));

        this.stats = new TableStats(this.schema, this.numRecordsPerPage);
        this.numRecords = 0;

        Iterator<Page> iter = this.heapFile.iterator();
        while(iter.hasNext()) {
            Page page = iter.next();
            byte[] bitmap = getBitMap(page);

            for (short i = 0; i < numRecordsPerPage; ++i) {
                if (Bits.getBit(bitmap, i) == Bits.Bit.ONE) {
                    Record r = getRecord(new RecordId(page.getPageNum(), i));
                    stats.addRecord(r);
                    numRecords++;
                }
            }
            page.unpin();
        }

        this.lockContext = lockContext;
    }

    // Accessors /////////////////////////////////////////////////////////////////
    public String getName() {
        return name;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getNumRecordsPerPage() {
        return numRecordsPerPage;
    }

    public void setFullPageRecords() {
        numRecordsPerPage = 1;
        bitmapSizeInBytes = 0;
        heapFile.setEmptyPageMetadataSize((short) (heapFile.getEffectivePageSize() -
                                          schema.getSizeInBytes()));
    }

    public TableStats getStats() {
        return stats;
    }

    public long getNumRecords() {
        return numRecords;
    }

    public int getNumDataPages() {
        return this.heapFile.getNumDataPages();
    }

    public int getPartNum() {
        return heapFile.getPartNum();
    }

    private byte[] getBitMap(Page page) {
        if (bitmapSizeInBytes > 0) {
            byte[] bytes = new byte[bitmapSizeInBytes];
            page.getBuffer().get(bytes, 0, bitmapSizeInBytes);
            return bytes;
        } else {
            return new byte[] {(byte) 0xFF};
        }
    }

    private void writeBitMap(Page page, byte[] bitmap) {
        if (bitmapSizeInBytes > 0) {
            page.getBuffer().put(bitmap, 0, bitmapSizeInBytes);
        }
    }

    private static int computeBitmapSizeInBytes(int pageSize, Schema schema) {
        int unroundedRecords = computeUnroundedNumRecordsPerPage(pageSize, schema);
        if (unroundedRecords >= 8) {
            // Dividing by 8 simultaneously (a) rounds down the number of records to a
            // multiple of 8 and (b) converts bits to bytes.
            return unroundedRecords / 8;
        } else {
            // special case: full page records with no bitmap
            return 0;
        }
    }

    public static int computeNumRecordsPerPage(int pageSize, Schema schema) {
        int unroundedRecords = computeUnroundedNumRecordsPerPage(pageSize, schema);
        if (unroundedRecords >= 8) {
            // Dividing by 8 and then multiplying by 8 rounds down to the nearest
            // multiple of 8.
            return computeUnroundedNumRecordsPerPage(pageSize, schema) / 8 * 8;
        } else {
            // special case: full page records with no bitmap
            return 1;
        }
    }

    // Modifiers /////////////////////////////////////////////////////////////////
    /**
     * buildStatistics builds histograms on each of the columns of a table. Running
     * it multiple times refreshes the statistics
     */
    public void buildStatistics(int buckets) {
        this.stats.refreshHistograms(buckets, this);
    }

    // Modifiers /////////////////////////////////////////////////////////////////
    private synchronized void insertRecord(Page page, int entryNum, Record record) {
        if (autoEscalate) { autoEscalate(LockType.X); }
        int offset = bitmapSizeInBytes + (entryNum * schema.getSizeInBytes());
        page.getBuffer().position(offset).put(record.toBytes(schema));
    }

    /**
     * addRecord adds a record to this table and returns the record id of the
     * newly added record. stats, freePageNums, and numRecords are updated
     * accordingly. The record is added to the first free slot of the first free
     * page (if one exists, otherwise one is allocated). For example, if the
     * first free page has bitmap 0b11101000, then the record is inserted into
     * the page with index 3 and the bitmap is updated to 0b11111000.
     */
    public synchronized RecordId addRecord(List<DataBox> values) {
        Record record = schema.verify(values);
        if (autoEscalate) { autoEscalate(LockType.X); }
        Page page = heapFile.getPageWithSpace(schema.getSizeInBytes());
        try {
            // Find the first empty slot in the bitmap.
            // entry number of the first free slot and store it in entryNum; and (2) we
            // count the total number of entries on this page.
            byte[] bitmap = getBitMap(page);
            int entryNum = 0;
            for (; entryNum < numRecordsPerPage; ++entryNum) {
                if (Bits.getBit(bitmap, entryNum) == Bits.Bit.ZERO) {
                    break;
                }
            }
            if (numRecordsPerPage == 1) {
                entryNum = 0;
            }
            assert (entryNum < numRecordsPerPage);

            // Insert the record and update the bitmap.
            insertRecord(page, entryNum, record);
            Bits.setBit(bitmap, entryNum, Bits.Bit.ONE);
            writeBitMap(page, bitmap);

            // Update the metadata.
            stats.addRecord(record);
            numRecords++;

            return new RecordId(page.getPageNum(), (short) entryNum);
        } finally {
            page.unpin();
        }
    }

    /**
     * Retrieves a record from the table, throwing an exception if no such record
     * exists.
     */
    public synchronized Record getRecord(RecordId rid) {
        validateRecordId(rid);
        if (autoEscalate) { autoEscalate(LockType.S); }
        Page page = fetchPage(rid.getPageNum());
        try {
            byte[] bitmap = getBitMap(page);
            if (Bits.getBit(bitmap, rid.getEntryNum()) == Bits.Bit.ZERO) {
                String msg = String.format("Record %s does not exist.", rid);
                throw new DatabaseException(msg);
            }

            int offset = bitmapSizeInBytes + (rid.getEntryNum() * schema.getSizeInBytes());
            Buffer buf = page.getBuffer();
            buf.position(offset);
            return Record.fromBytes(buf, schema);
        } finally {
            page.unpin();
        }
    }

    /**
     * Overwrites an existing record with new values and returns the existing
     * record. stats is updated accordingly. An exception is thrown if rid does
     * not correspond to an existing record in the table.
     */
    public synchronized Record updateRecord(List<DataBox> values, RecordId rid) {
        // TODO(proj4_part3): modify for smarter locking
        if (autoEscalate) { autoEscalate(LockType.X); }
        LockContext context = lockContext.childContext(rid.getPageNum());
        LockUtil.ensureSufficientLockHeld(context, LockType.X);
        validateRecordId(rid);
        Page page = fetchPage(rid.getPageNum());

        Record newRecord = schema.verify(values);
        Record oldRecord = getRecord(rid);


        try {
            insertRecord(page, rid.getEntryNum(), newRecord);

            this.stats.removeRecord(oldRecord);
            this.stats.addRecord(newRecord);
            return oldRecord;
        } finally {
            page.unpin();
        }
    }

    /**
     * Deletes and returns the record specified by rid from the table and updates
     * stats, freePageNums, and numRecords as necessary. An exception is thrown
     * if rid does not correspond to an existing record in the table.
     */
    public synchronized Record deleteRecord(RecordId rid) {
        // TODO(proj4_part3): modify for smarter locking
        if (autoEscalate) { autoEscalate(LockType.X); }
        LockContext context = lockContext.childContext(rid.getPageNum());
        LockUtil.ensureSufficientLockHeld(context, LockType.X);
        validateRecordId(rid);

        Page page = fetchPage(rid.getPageNum());

        try {
            Record record = getRecord(rid);

            byte[] bitmap = getBitMap(page);
            Bits.setBit(bitmap, rid.getEntryNum(), Bits.Bit.ZERO);
            writeBitMap(page, bitmap);

            stats.removeRecord(record);
            int numRecords = numRecordsPerPage == 1 ? 0 : numRecordsOnPage(page);
            heapFile.updateFreeSpace(page,
                                     (short) ((numRecordsPerPage - numRecords) * schema.getSizeInBytes()));
            this.numRecords--;

            return record;
        } finally {
            page.unpin();
        }
    }

    @Override
    public String toString() {
        return "Table " + name;
    }

    // Helpers ///////////////////////////////////////////////////////////////////
    private Page fetchPage(long pageNum) {
        try {
            return heapFile.getPage(pageNum);
        } catch (PageException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Recall that every data page contains an m-byte bitmap followed by n
     * records. The following three functions computes m and n such that n is
     * maximized. To simplify things, we round n down to the nearest multiple of
     * 8 if necessary. m and n are stored in bitmapSizeInBytes and
     * numRecordsPerPage respectively.
     *
     * Some examples:
     *
     *   | Page Size | Record Size | bitmapSizeInBytes | numRecordsPerPage |
     *   | --------- | ----------- | ----------------- | ----------------- |
     *   | 9 bytes   | 1 byte      | 1                 | 8                 |
     *   | 10 bytes  | 1 byte      | 1                 | 8                 |
     *   ...
     *   | 17 bytes  | 1 byte      | 1                 | 8                 |
     *   | 18 bytes  | 2 byte      | 2                 | 16                |
     *   | 19 bytes  | 2 byte      | 2                 | 16                |
     */
    private static int computeUnroundedNumRecordsPerPage(int pageSize, Schema schema) {
        // Storing each record requires 1 bit for the bitmap and 8 *
        // schema.getSizeInBytes() bits for the record.
        int recordOverheadInBits = 1 + 8 * schema.getSizeInBytes();
        int pageSizeInBits = pageSize  * 8;
        return pageSizeInBits / recordOverheadInBits;
    }

    private int numRecordsOnPage(Page page) {
        byte[] bitmap = getBitMap(page);
        int numRecords = 0;
        for (int i = 0; i < numRecordsPerPage; ++i) {
            if (Bits.getBit(bitmap, i) == Bits.Bit.ONE) {
                numRecords++;
            }
        }
        return numRecords;
    }

    private void validateRecordId(RecordId rid) {
        long p = rid.getPageNum();
        int e = rid.getEntryNum();

        if (e < 0) {
            String msg = String.format("Invalid negative entry number %d.", e);
            throw new DatabaseException(msg);
        }

        if (e >= numRecordsPerPage) {
            String msg = String.format(
                             "There are only %d records per page, but record %d was requested.",
                             numRecordsPerPage, e);
            throw new DatabaseException(msg);
        }
    }

    // Locking ///////////////////////////////////////////////////////////////////

    /**
     * Enables auto-escalation. All future requests for pages of this table by transactions
     * that hold locks on at least 20% of the locks on the table's pages when this table
     * has at least 10 pages should escalate to a table-level lock before any locks are requested.
     */
    public void enableAutoEscalate() {
        // TODO(proj4_part3): implement
        autoEscalate = true;
    }

    /**
     * Disables auto-escalation. No future requests for pages of this table should result in
     * an automatic escalation to a table-level lock.
     */
    public void disableAutoEscalate() {
        // TODO(proj4_part3): implement
        autoEscalate = false;
    }

    private void autoEscalate(LockType lockType) {
        // LockType is S or X.
        if (lockContext != null) {
            int tableSize = lockContext.capacity();
            TransactionContext transaction = TransactionContext.getTransaction();
            double lockPercent = lockContext.saturation(transaction);
            if (tableSize >= 10 && lockPercent >= 0.2) {
                lockContext.escalate(transaction);
            }
        }
    }

    // Iterators /////////////////////////////////////////////////////////////////
    public BacktrackingIterator<RecordId> ridIterator() {
        // TODO(proj4_part3): reduce locking overhead for table scans
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);
        BacktrackingIterator<Page> iter = heapFile.iterator();
        return new ConcatBacktrackingIterator<>(new PageIterator(iter, false));
    }

    @Override
    public BacktrackingIterator<Record> iterator() {
        return new RecordIterator(this, ridIterator());
    }

    private BacktrackingIterator<RecordId> blockRidIterator(Iterator<Page> pageIter, int maxPages) {
        Page[] block = new Page[maxPages];
        int numPages;
        for (numPages = 0; numPages < maxPages && pageIter.hasNext(); ++numPages) {
            block[numPages] = pageIter.next();
            block[numPages].unpin();
        }
        if (numPages < maxPages) {
            Page[] temp = new Page[numPages];
            System.arraycopy(block, 0, temp, 0, numPages);
            block = temp;
        }
        return new ConcatBacktrackingIterator<>(new PageIterator(new ArrayBacktrackingIterator<>(block),
                                                true));
    }

    public BacktrackingIterator<Record> blockIterator(Iterator<Page> block,
            int maxPages) {
        return new RecordIterator(this, blockRidIterator(block, maxPages));
    }

    public BacktrackingIterator<Page> pageIterator() {
        return heapFile.iterator();
    }

    /**
     * RIDPageIterator is a BacktrackingIterator over the RecordIds of a single
     * page of the table.
     *
     * See comments on the BacktrackingIterator interface for how mark and reset
     * should function.
     */
    class RIDPageIterator extends IndexBacktrackingIterator<RecordId> {
        private Page page;
        private byte[] bitmap;

        RIDPageIterator(Page page) {
            super(numRecordsPerPage);
            this.page = page;
            this.bitmap = getBitMap(page);
            page.unpin();
        }

        @Override
        protected int getNextNonempty(int currentIndex) {
            for (int i = currentIndex + 1; i < numRecordsPerPage; ++i) {
                if (Bits.getBit(bitmap, i) == Bits.Bit.ONE) {
                    return i;
                }
            }
            return numRecordsPerPage;
        }

        @Override
        protected RecordId getValue(int index) {
            return new RecordId(page.getPageNum(), (short) index);
        }
    }

    private class PageIterator implements BacktrackingIterator<BacktrackingIterable<RecordId>> {
        private BacktrackingIterator<Page> sourceIterator;
        private boolean pinOnFetch;

        private PageIterator(BacktrackingIterator<Page> sourceIterator, boolean pinOnFetch) {
            this.sourceIterator = sourceIterator;
            this.pinOnFetch = pinOnFetch;
        }

        @Override
        public void markPrev() {
            sourceIterator.markPrev();
        }

        @Override
        public void markNext() {
            sourceIterator.markNext();
        }

        @Override
        public void reset() {
            sourceIterator.reset();
        }

        @Override
        public boolean hasNext() {
            return sourceIterator.hasNext();
        }

        @Override
        public BacktrackingIterable<RecordId> next() {
            return new InnerIterable(sourceIterator.next());
        }

        private class InnerIterable implements BacktrackingIterable<RecordId> {
            private Page baseObject;

            private InnerIterable(Page baseObject) {
                this.baseObject = baseObject;
                if (!pinOnFetch) {
                    baseObject.unpin();
                }
            }

            @Override
            public BacktrackingIterator<RecordId> iterator() {
                baseObject.pin();
                return new RIDPageIterator(baseObject);
            }
        }
    }
}

