package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.Proj5Tests;
import edu.berkeley.cs186.database.categories.StudentTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.DiskSpaceManagerImpl;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.BufferManagerImpl;
import edu.berkeley.cs186.database.memory.LRUEvictionPolicy;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.*;
import java.util.function.Consumer;

import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

/**
 * File for student tests for Project 5 (Recovery). Tests are run through
 * TestARIESStudentRunner for grading purposes.
 */
@Category({Proj5Tests.class, StudentTests.class})
public class TestARIESStudent {
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
    public void cleanup() throws Exception {}

    @Test
    public void testStudentAnalysis() throws Exception {
        // TODO(proj5): write your own test on restartAnalysis only
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager, and use runAnalysis(inner) instead of
        // inner.restartAnalysis() to call the analysis routine.
    }

    @Test
    public void testStudentRedo() throws Exception {
        // TODO(proj5): write your own test on restartRedo only
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager, and use runRedo(inner) instead of
        // inner.restartRedo() to call the analysis routine.
    }

    @Test
    public void testStudentUndo() throws Exception {
        // TODO(proj5): write your own test on restartUndo only
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager, and use runUndo(inner) instead of
        // inner.restartUndo() to call the analysis routine.
    }

    @Test
    public void testStudentIntegration() throws Exception {
        // TODO(proj5): write your own test on all of RecoveryManager
        // You should use loadRecoveryManager instead of new ARIESRecoveryManager(..) to
        // create the recovery manager.
    }

    // TODO(proj5): add as many (ungraded) tests as you want for testing!

    @Test
    public void testCase() throws Exception {
        // TODO(proj5): write your own test! (ungraded)
    }

    @Test
    public void anotherTestCase() throws Exception {
        // TODO(proj5): write your own test!!! (ungraded)
    }

    @Test
    public void yetAnotherTestCase() throws Exception {
        // TODO(proj5): write your own test!!!!! (ungraded)
    }

    /*************************************************************************
     * Helpers for writing tests.                                            *
     * Do not change the signature of any of the following methods.          *
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
        redoMethods.add(record -> {
            fail("LogRecord#redo() called too many times");
        });
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
