package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.HW5Tests;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.DiskSpaceManagerImpl;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.BufferManagerImpl;
import edu.berkeley.cs186.database.memory.LRUEvictionPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.*;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

@Category(HW5Tests.class)
public class TestRecoveryManager {
    private String testDir;
    private RecoveryManager recoveryManager;
    private final Queue<Consumer<LogRecord>> redoMethods = new ArrayDeque<>();

    // 1 second per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        testDir = tempFolder.newFolder("test-dir").getAbsolutePath();
        recoveryManager = loadRecoveryManager(testDir);
        DummyTransaction.cleanupTransactions();
        LogRecord.onRedoHandler(t -> {});
    }

    @After
    public void cleanup() {
        recoveryManager.close();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleCommit() throws Exception {
        long pageNum = 10000000002L;
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after);
        long LSN1 = recoveryManager.commit(1L);

        assertEquals(LSN1, transactionTable.get(1L).lastLSN);
        assertEquals(Transaction.Status.COMMITTING, transactionTable.get(1L).transaction.getStatus());

        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        long LSN2 = recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, before, after);

        assertTrue(logManager.getFlushedLSN() + " is not greater than or equal to " + LSN1,
                   LSN1 <= logManager.getFlushedLSN());
        assertTrue(logManager.getFlushedLSN() + " is not less than " + LSN2,
                   LSN2 > logManager.getFlushedLSN());
    }

    @Test
    @Category(PublicTests.class)
    public void testAbort() throws Exception {
        long pageNum = 10000000002L;
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after);

        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, before, after);

        long LSN = recoveryManager.abort(1L);

        assertEquals(LSN, transactionTable.get(1L).lastLSN);
        assertEquals(Transaction.Status.ABORTING, transactionTable.get(1L).transaction.getStatus());
        assertEquals(Transaction.Status.RUNNING, transactionTable.get(2L).transaction.getStatus());
    }

    @Test
    @Category(PublicTests.class)
    public void testEnd() throws Exception {
        long pageNum = 10000000002L;
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);
        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        long[] LSNs = new long[] {
            recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after), // 0
            recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, before, after), // 1
            recoveryManager.logPageWrite(1L, pageNum, pageOffset, after, before), // 2
            recoveryManager.logPageWrite(2L, pageNum + 1, pageOffset, after, before), // 3
            recoveryManager.logAllocPart(2L, 2), // 4
            recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after), // 5
            recoveryManager.commit(2L), // 6
            recoveryManager.logPageWrite(1L, pageNum, pageOffset, before, after), // 7
            -1L,
            -1L,
        };

        assertEquals(LSNs[7], transactionTable.get(1L).lastLSN);
        assertEquals(LSNs[6], transactionTable.get(2L).lastLSN);
        assertEquals(LogManagerImpl.maxLSN(LogManagerImpl.getLSNPage(LSNs[6])), logManager.getFlushedLSN());
        assertEquals(Transaction.Status.COMMITTING, transactionTable.get(2L).transaction.getStatus());

        LSNs[8] = recoveryManager.end(2L);
        LSNs[9] = recoveryManager.abort(1L);

        assertEquals(LSNs[9], transactionTable.get(1L).lastLSN);
        assertEquals(Transaction.Status.ABORTING, transactionTable.get(1L).transaction.getStatus());

        recoveryManager.end(1L); // 4 CLRs + END

        Iterator<LogRecord> iter = logManager.iterator();

        // CLRs are written correctly
        int totalRecords = 0; // 18 - 3 (master + begin/end chkpt) + 10 (LSNs) + 4 (CLRs) + 1 (END)
        int abort = 0; // 1
        int commit = 0; // 1
        int end = 0; // 2
        int update = 0; // 4 + 2
        int allocPart = 0; // 1
        int undo = 0; // 4
        while (iter.hasNext()) {
            LogRecord record = iter.next();
            totalRecords++;
            switch (record.getType()) {
            case ABORT_TRANSACTION:
                abort++;
                break;
            case COMMIT_TRANSACTION:
                commit++;
                break;
            case END_TRANSACTION:
                end++;
                break;
            case UPDATE_PAGE:
                update++;
                break;
            case UNDO_UPDATE_PAGE:
                undo++;
                break;
            case ALLOC_PART:
                allocPart++;
                break;
            }
        }
        assertEquals(18, totalRecords);
        assertEquals(1, abort);
        assertEquals(1, commit);
        assertEquals(2, end);
        assertEquals(6, update);
        assertEquals(1, allocPart);
        assertEquals(4, undo);

        // Dirty page table
        assertEquals(LSNs[0], (long) dirtyPageTable.get(pageNum));
        assertEquals(LSNs[1], (long) dirtyPageTable.get(pageNum + 1));

        // Transaction table
        assertTrue(transactionTable.isEmpty());

        // Flushed log tail correct
        assertEquals(LogManagerImpl.maxLSN(LogManagerImpl.getLSNPage(LSNs[6])), logManager.getFlushedLSN());

        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
        assertEquals(Transaction.Status.COMPLETE, transaction2.getStatus());
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleLogPageWrite() throws Exception {
        short pageOffset = 20;
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        long LSN1 = recoveryManager.logPageWrite(transaction1.getTransNum(), 10000000002L, pageOffset,
                    before, after);

        // Check DPT, X-Act table updates
        assertTrue(transactionTable.containsKey(1L));
        assertEquals(LSN1, transactionTable.get(1L).lastLSN);
        assertTrue(transactionTable.get(1L).touchedPages.contains(10000000002L));
        assertTrue(dirtyPageTable.containsKey(10000000002L));
        assertEquals(LSN1, (long) dirtyPageTable.get(10000000002L));

        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);

        long LSN2 = recoveryManager.logPageWrite(transaction2.getTransNum(), 10000000003L, pageOffset,
                    before, after);

        assertTrue(transactionTable.containsKey(2L));
        assertEquals(LSN2, transactionTable.get(2L).lastLSN);
        assertTrue(transactionTable.get(2L).touchedPages.contains(10000000003L));
        assertEquals(LSN2, (long) dirtyPageTable.get(10000000003L));

        long LSN3 = recoveryManager.logPageWrite(transaction1.getTransNum(), 10000000002L, pageOffset,
                    before, after);
        assertEquals(LSN3, transactionTable.get(1L).lastLSN);
        assertEquals(LSN1, (long) dirtyPageTable.get(10000000002L));
    }

    @Test
    @Category(PublicTests.class)
    public void testTwoPartLogPageWrite() throws Exception {
        long pageNum = 10000000002L;
        byte[] before = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
        byte[] after = new byte[BufferManager.EFFECTIVE_PAGE_SIZE];
        for (int i = 0; i < BufferManager.EFFECTIVE_PAGE_SIZE; ++i) {
            after[i] = (byte) (i % 256);
        }

        LogManager logManager = getLogManager(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        long secondLSN = recoveryManager.logPageWrite(transaction1.getTransNum(), pageNum, (short) 0,
                         before, after);
        long firstLSN = secondLSN - 10000L; // previous log page, both at start of a log page

        LogRecord firstLogRecord = logManager.fetchLogRecord(firstLSN);
        LogRecord secondLogRecord = logManager.fetchLogRecord(secondLSN);

        assertTrue(firstLogRecord instanceof UpdatePageLogRecord);
        assertTrue(secondLogRecord instanceof UpdatePageLogRecord);
        assertArrayEquals(before, ((UpdatePageLogRecord) firstLogRecord).before);
        assertArrayEquals(new byte[0], ((UpdatePageLogRecord) secondLogRecord).before);
        assertArrayEquals(new byte[0], ((UpdatePageLogRecord) firstLogRecord).after);
        assertArrayEquals(after, ((UpdatePageLogRecord) secondLogRecord).after);

        assertTrue(transactionTable.containsKey(1L));
        assertTrue(transactionTable.get(1L).touchedPages.contains(pageNum));
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleSavepoint() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);
        recoveryManager.savepoint(1L, "savepoint 1");
        long LSN = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        recoveryManager.rollbackToSavepoint(1L, "savepoint 1");

        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        iter.next(); // page write record

        LogRecord clr = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, clr.getType());

        assertEquals((short) 0, ((UndoUpdatePageLogRecord) clr).offset);
        assertArrayEquals(before, ((UndoUpdatePageLogRecord) clr).after);
        assertEquals(Optional.of(1L), clr.getTransNum());
        assertEquals(Optional.of(10000000001L), clr.getPageNum());
        assertTrue(clr.getUndoNextLSN().orElseThrow(NoSuchElementException::new) < LSN);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleCheckpoint() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);

        long LSN1 = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        recoveryManager.checkpoint();

        recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, after, before);
        recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        Iterator<LogRecord> iter = logManager.scanFrom(LSN1);
        iter.next(); // page write (LSN 1)

        LogRecord beginCheckpoint = iter.next();
        LogRecord endCheckpoint = iter.next();
        assertEquals(LogType.BEGIN_CHECKPOINT, beginCheckpoint.getType());
        assertEquals(LogType.END_CHECKPOINT, endCheckpoint.getType());

        Map<Long, Pair<Transaction.Status, Long>> txnTable = endCheckpoint.getTransactionTable();
        Map<Long, Long> dpt = endCheckpoint.getDirtyPageTable();
        assertEquals(LSN1, (long) dpt.get(10000000001L));
        assertEquals(new Pair<>(Transaction.Status.RUNNING, LSN1), txnTable.get(1L));
    }

    @Test
    @Category(PublicTests.class)
    public void testRestartAnalysis() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(1L);
        DummyTransaction transaction2 = DummyTransaction.create(2L);
        DummyTransaction transaction3 = DummyTransaction.create(3L);

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before,
                                        after))); // 0
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 0,
                                        before, after))); // 1
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(3L, 10000000003L, 0L, (short) 0,
                                        before, after))); // 2
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(1L, LSNs.get(1)))); // 3
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(1L, LSNs.get(3)))); // 4
        LSNs.add(logManager.appendToLog(new FreePageLogRecord(2L, 10000000001L, 0L))); // 5
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(2L, LSNs.get(5)))); // 6
        LSNs.add(logManager.appendToLog(new BeginCheckpointLogRecord(9876543210L))); // 7

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // new recovery manager - tables/log manager/other state loaded with old manager are different
        // with the new recovery manager
        logManager = getLogManager(recoveryManager);
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);
        List<String> lockRequests = getLockRequests(recoveryManager);

        runAnalysis(recoveryManager);

        // Xact table
        assertFalse(transactionTable.containsKey(1L));
        assertTrue(transactionTable.containsKey(2L));
        assertEquals((long) LSNs.get(6), transactionTable.get(2L).lastLSN);
        assertEquals(new HashSet<>(Collections.singletonList(10000000001L)),
                     transactionTable.get(2L).touchedPages);
        assertTrue(transactionTable.containsKey(3L));
        assertTrue(transactionTable.get(3L).lastLSN > LSNs.get(7));
        assertEquals(new HashSet<>(Collections.singletonList(10000000003L)),
                     transactionTable.get(3L).touchedPages);

        // DPT
        assertFalse(dirtyPageTable.containsKey(10000000001L));
        assertTrue(dirtyPageTable.containsKey(10000000002L));
        assertEquals((long) LSNs.get(1), (long) dirtyPageTable.get(10000000002L));
        assertTrue(dirtyPageTable.containsKey(10000000003L));
        assertEquals((long) LSNs.get(2), (long) dirtyPageTable.get(10000000003L));

        // status/cleanup
        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
        assertTrue(transaction1.cleanedUp);
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transaction2.getStatus());
        assertFalse(transaction2.cleanedUp);
        assertEquals(Transaction.Status.RECOVERY_ABORTING, transaction3.getStatus());
        assertFalse(transaction2.cleanedUp);

        // lock requests made
        assertEquals(Arrays.asList(
                         "request 1 X(database/1/10000000001)",
                         "request 1 X(database/1/10000000002)",
                         "request 3 X(database/1/10000000003)",
                         "request 2 X(database/1/10000000001)"
                     ), lockRequests);

        // transaction counter - from begin checkpoint
        assertEquals(9876543210L, getTransactionCounter(recoveryManager));

        // FlushedLSN
        assertEquals(LogManagerImpl.maxLSN(LogManagerImpl.getLSNPage(LSNs.get(7))),
                     logManager.getFlushedLSN());
    }

    @Test
    @Category(PublicTests.class)
    public void testRestartRedo() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        DiskSpaceManager dsm = getDiskSpaceManager(recoveryManager);
        BufferManager bm = getBufferManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(1L);

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before,
                                        after))); // 0
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 1,
                                        before, after))); // 1
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(1), (short) 1,
                                        after, before))); // 2
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000003L, LSNs.get(2), (short) 2,
                                        before, after))); // 3
        LSNs.add(logManager.appendToLog(new AllocPartLogRecord(1L, 10, LSNs.get(3)))); // 4
        LSNs.add(logManager.appendToLog(new CommitTransactionLogRecord(1L, LSNs.get(4)))); // 5
        LSNs.add(logManager.appendToLog(new EndTransactionLogRecord(1L, LSNs.get(5)))); // 6

        // actually do the first and second write (and get it flushed to disk)
        logManager.fetchLogRecord(LSNs.get(0)).redo(dsm, bm);
        logManager.fetchLogRecord(LSNs.get(1)).redo(dsm, bm);

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up dirty page table - xact table is empty (transaction ended)
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        dirtyPageTable.put(10000000002L, LSNs.get(2));
        dirtyPageTable.put(10000000003L, LSNs.get(3));

        // set up checks for redo - these get called in sequence with each LogRecord#redo call
        setupRedoChecks(Arrays.asList(
                            (LogRecord record) -> assertEquals((long) LSNs.get(2), (long) record.LSN),
                            (LogRecord record) -> assertEquals((long) LSNs.get(3), (long) record.LSN),
                            (LogRecord record) -> assertEquals((long) LSNs.get(4), (long) record.LSN)
                        ));

        runRedo(recoveryManager);

        finishRedoChecks();
    }

    @Test
    @Category(PublicTests.class)
    public void testRestartUndo() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        LogManager logManager = getLogManager(recoveryManager);
        DiskSpaceManager dsm = getDiskSpaceManager(recoveryManager);
        BufferManager bm = getBufferManager(recoveryManager);

        DummyTransaction transaction1 = DummyTransaction.create(1L);

        List<Long> LSNs = new ArrayList<>();
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000001L, 0L, (short) 0, before,
                                        after))); // 0
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000002L, LSNs.get(0), (short) 1,
                                        before, after))); // 1
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000003L, LSNs.get(1), (short) 2,
                                        before, after))); // 2
        LSNs.add(logManager.appendToLog(new UpdatePageLogRecord(1L, 10000000004L, LSNs.get(2), (short) 3,
                                        before, after))); // 3
        LSNs.add(logManager.appendToLog(new AbortTransactionLogRecord(1L, LSNs.get(3)))); // 4

        // actually do the writes
        for (int i = 0; i < 4; ++i) {
            logManager.fetchLogRecord(LSNs.get(i)).redo(dsm, bm);
        }

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        // set up xact table - leaving DPT empty
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);
        TransactionTableEntry entry1 = new TransactionTableEntry(transaction1);
        entry1.lastLSN = LSNs.get(4);
        entry1.touchedPages = new HashSet<>(Arrays.asList(10000000001L, 10000000002L, 10000000003L,
                                            10000000004L));
        entry1.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        transactionTable.put(1L, entry1);

        // set up checks for undo - these get called in sequence with each LogRecord#redo call
        // (which should be called on CLRs)
        setupRedoChecks(Arrays.asList(
        (LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000004L), record.getPageNum());
        },
        (LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000003L), record.getPageNum());
        },
        (LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000002L), record.getPageNum());
        },
        (LogRecord record) -> {
            assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
            assertNotNull("log record not appended to log yet", record.LSN);
            assertEquals((long) record.LSN, transactionTable.get(1L).lastLSN);
            assertEquals(Optional.of(10000000001L), record.getPageNum());
        }
                        ));

        runUndo(recoveryManager);

        finishRedoChecks();

        assertEquals(Transaction.Status.COMPLETE, transaction1.getStatus());
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleRestart() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Transaction transaction1 = DummyTransaction.create(1L);

        recoveryManager.startTransaction(transaction1);
        long LSN = recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after);

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        LogManager logManager = getLogManager(recoveryManager);
        Map<Long, Long> dirtyPageTable = getDirtyPageTable(recoveryManager);
        Map<Long, TransactionTableEntry> transactionTable = getTransactionTable(recoveryManager);

        setupRedoChecks(Collections.singletonList(
        (LogRecord record) -> {
            assertEquals(LSN, record.getLSN());
            assertEquals(LogType.UPDATE_PAGE, record.getType());
        }
                        ));

        Runnable func = recoveryManager.restart(); // analysis + redo

        finishRedoChecks();

        assertTrue(transactionTable.containsKey(transaction1.getTransNum()));
        TransactionTableEntry entry = transactionTable.get(transaction1.getTransNum());
        assertEquals(Transaction.Status.RECOVERY_ABORTING, entry.transaction.getStatus());
        assertEquals(new HashSet<>(Collections.singletonList(10000000001L)), entry.touchedPages);
        assertEquals(LSN, (long) dirtyPageTable.get(10000000001L));

        func.run(); // undo

        Iterator<LogRecord> iter = logManager.scanFrom(LSN);

        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.ABORT_TRANSACTION, iter.next().getType());
        assertEquals(LogType.UNDO_UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.END_TRANSACTION, iter.next().getType());
        assertEquals(LogType.BEGIN_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.END_CHECKPOINT, iter.next().getType());
        assertFalse(iter.hasNext());
    }

    @Test
    @Category(PublicTests.class)
    public void testRestart() throws Exception {
        byte[] before = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00 };
        byte[] after = new byte[] { (byte) 0xBA, (byte) 0xAD, (byte) 0xF0, (byte) 0x0D };

        Transaction transaction1 = DummyTransaction.create(1L);
        recoveryManager.startTransaction(transaction1);
        Transaction transaction2 = DummyTransaction.create(2L);
        recoveryManager.startTransaction(transaction2);
        Transaction transaction3 = DummyTransaction.create(3L);
        recoveryManager.startTransaction(transaction3);

        long[] LSNs = new long[] {
            recoveryManager.logPageWrite(1L, 10000000001L, (short) 0, before, after), // 0
            recoveryManager.logPageWrite(2L, 10000000003L, (short) 0, before, after), // 1
            recoveryManager.commit(1L), // 2
            recoveryManager.logPageWrite(3L, 10000000004L, (short) 0, before, after), // 3
            recoveryManager.logPageWrite(2L, 10000000001L, (short) 0, after, before), // 4
            recoveryManager.end(1L), // 5
            recoveryManager.logPageWrite(3L, 10000000002L, (short) 0, before, after), // 6
            recoveryManager.abort(2), // 7
        };

        // flush everything - recovery tests should always start
        // with a clean load from disk, and here we want everything sent to disk first.
        // Note: this does not call RecoveryManager#close - it only closes the
        // buffer manager and disk space manager.
        shutdownRecoveryManager(recoveryManager);

        // load from disk again
        recoveryManager = loadRecoveryManager(testDir);

        LogManager logManager = getLogManager(recoveryManager);

        recoveryManager.restart().run(); // run everything in restart recovery

        Iterator<LogRecord> iter = logManager.iterator();
        assertEquals(LogType.MASTER, iter.next().getType());
        assertEquals(LogType.BEGIN_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.END_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.COMMIT_TRANSACTION, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.END_TRANSACTION, iter.next().getType());
        assertEquals(LogType.UPDATE_PAGE, iter.next().getType());
        assertEquals(LogType.ABORT_TRANSACTION, iter.next().getType());

        LogRecord record = iter.next();
        assertEquals(LogType.ABORT_TRANSACTION, record.getType());
        long LSN8 = record.LSN;

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
        assertEquals(LSN8, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LSNs[3], (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        long LSN9 = record.LSN;

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
        assertEquals(LSNs[7], (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LSNs[1], (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        long LSN10 = record.LSN;

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
        assertEquals(LSN9, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(0L, (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(LogType.END_TRANSACTION, iter.next().getType());

        record = iter.next();
        assertEquals(LogType.UNDO_UPDATE_PAGE, record.getType());
        assertEquals(LSN10, (long) record.getPrevLSN().orElseThrow(NoSuchElementException::new));
        assertEquals(0L, (long) record.getUndoNextLSN().orElseThrow(NoSuchElementException::new));

        assertEquals(LogType.END_TRANSACTION, iter.next().getType());
        assertEquals(LogType.BEGIN_CHECKPOINT, iter.next().getType());
        assertEquals(LogType.END_CHECKPOINT, iter.next().getType());
        assertFalse(iter.hasNext());
    }

    /*************************************************************************
     * Helpers - these are similar to the ones available in TestARIESStudent *
     *************************************************************************/

    /**
     * Helper to set up checks for redo. The first call to LogRecord.redo will
     * call the first method in METHODS, the second call to the second method in METHODS,
     * and so on. Call this method before the redo pass, and call finishRedoChecks
     * after the redo pass.
     */
    private void setupRedoChecks(Collection<Consumer<LogRecord>> methods) {
        for (final Consumer<LogRecord> method : methods) {
            redoMethods.add(record -> {
                method.accept(record);
                LogRecord.onRedoHandler(redoMethods.poll());
            });
        }
        redoMethods.add(record -> fail("LogRecord#redo() called too many times"));
        LogRecord.onRedoHandler(redoMethods.poll());
    }

    /**
     * Helper to finish checks for redo. Call this after the redo pass (or undo pass)-
     * if not enough redo calls were performed, an error is thrown.
     *
     * If setupRedoChecks is used for the redo pass, and this method is not called before
     * the undo pass, and the undo pass calls undo at least once, an error may be incorrectly thrown.
     */
    private void finishRedoChecks() {
        assertTrue("LogRecord#redo() not called enough times", redoMethods.isEmpty());
        LogRecord.onRedoHandler(record -> {});
    }

    /**
     * Loads the recovery manager from disk.
     * @param dir testDir
     * @return recovery manager, loaded from disk
     */
    protected RecoveryManager loadRecoveryManager(String dir) throws Exception {
        RecoveryManager recoveryManager = new ARIESRecoveryManagerNoLocking(
            new DummyLockContext(new Pair<>("database", 0L)),
            DummyTransaction::create
        );
        DiskSpaceManager diskSpaceManager = new DiskSpaceManagerImpl(dir, recoveryManager);
        BufferManager bufferManager = new BufferManagerImpl(diskSpaceManager, recoveryManager, 32,
                new LRUEvictionPolicy());
        boolean isLoaded = true;
        try {
            diskSpaceManager.allocPart(0);
            diskSpaceManager.allocPart(1);
            for (int i = 0; i < 10; ++i) {
                diskSpaceManager.allocPage(DiskSpaceManager.getVirtualPageNum(1, i));
            }
            isLoaded = false;
        } catch (IllegalStateException e) {
            // already loaded
        }
        recoveryManager.setManagers(diskSpaceManager, bufferManager);
        if (!isLoaded) {
            recoveryManager.initialize();
        }
        return recoveryManager;
    }

    /**
     * Flushes everything to disk, but does not call RecoveryManager#shutdown. Similar
     * to pulling the plug on the database at a time when no changes are in memory. You
     * can simulate a shutdown where certain changes _are_ in memory, by simply never
     * applying them (i.e. write a log record, but do not make the changes on the
     * buffer manager/disk space manager).
     */
    protected void shutdownRecoveryManager(RecoveryManager recoveryManager) throws Exception {
        ARIESRecoveryManager arm = (ARIESRecoveryManager) recoveryManager;
        arm.logManager.close();
        arm.bufferManager.evictAll();
        arm.bufferManager.close();
        arm.diskSpaceManager.close();
        DummyTransaction.cleanupTransactions();
    }

    protected BufferManager getBufferManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).bufferManager;
    }

    protected DiskSpaceManager getDiskSpaceManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).diskSpaceManager;
    }

    protected LogManager getLogManager(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).logManager;
    }

    protected List<String> getLockRequests(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).lockRequests;
    }

    protected long getTransactionCounter(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManagerNoLocking) recoveryManager).transactionCounter;
    }

    protected Map<Long, Long> getDirtyPageTable(RecoveryManager recoveryManager) throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).dirtyPageTable;
    }

    protected Map<Long, TransactionTableEntry> getTransactionTable(RecoveryManager recoveryManager)
    throws Exception {
        return ((ARIESRecoveryManager) recoveryManager).transactionTable;
    }

    protected void runAnalysis(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartAnalysis();
    }

    protected void runRedo(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartRedo();
    }

    protected void runUndo(RecoveryManager recoveryManager) throws Exception {
        ((ARIESRecoveryManager) recoveryManager).restartUndo();
    }
}
