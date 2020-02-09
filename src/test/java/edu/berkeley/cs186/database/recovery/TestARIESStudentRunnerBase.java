package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.categories.Proj5Tests;
import edu.berkeley.cs186.database.categories.StudentTestRunner;
import edu.berkeley.cs186.database.categories.StudentTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.DiskSpaceManagerImpl;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.BufferManagerImpl;
import edu.berkeley.cs186.database.memory.LRUEvictionPolicy;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

@Category({Proj5Tests.class, StudentTestRunner.class})
public abstract class TestARIESStudentRunnerBase {
    private Class<? extends RecoveryManager> recoveryManagerClass;

    // 1 second per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

    abstract String getSuffix();

    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        assertNotNull("TestARIESStudent should have @Category({Proj5Tests.class, StudentTests.class})",
                      TestARIESStudent.class.getAnnotation(Category.class));
        assertArrayEquals("TestARIESStudent should have @Category({Proj5Tests.class, StudentTests.class})",
                          new Class[] { Proj5Tests.class, StudentTests.class }, TestARIESStudent.class.getAnnotation(
                              Category.class).value());
        for (Method m : TestARIESStudent.class.getMethods()) {
            assertNull("TestARIESStudent methods should not have @Category", m.getAnnotation(Category.class));
        }

        String className = ARIESRecoveryManager.class.getCanonicalName() + getSuffix();
        recoveryManagerClass = (Class<? extends RecoveryManager>) Class.forName(className);

        DummyTransaction.cleanupTransactions();
        LogRecord.onRedoHandler(t -> {});
    }

    private static class AnalysisInstance extends TestInstance {
        private AnalysisInstance(Class<? extends RecoveryManager> recoveryManagerClass) throws Exception {
            super(recoveryManagerClass, true);
        }

        @Override
        protected void runRedo(RecoveryManager recoveryManager) {
            throw new UnsupportedOperationException("not allowed in testStudentAnalysis");
        }

        @Override
        protected void runUndo(RecoveryManager recoveryManager) {
            throw new UnsupportedOperationException("not allowed in testStudentAnalysis");
        }
    }

    @Test
    public void testStudentAnalysis() throws Exception {
        TestInstance ti = new AnalysisInstance(recoveryManagerClass);
        try {
            ti.tempFolder.create();
            try {
                ti.setup();
                ti.testStudentAnalysis();
            } finally {
                ti.cleanup();
            }
        } finally {
            ti.tempFolder.delete();
        }
    }

    private static class RedoInstance extends TestInstance {
        private RedoInstance(Class<? extends RecoveryManager> recoveryManagerClass) throws Exception {
            super(recoveryManagerClass, true);
        }

        @Override
        protected void runAnalysis(RecoveryManager recoveryManager) {
            throw new UnsupportedOperationException("not allowed in testStudentRedo");
        }

        @Override
        protected void runUndo(RecoveryManager recoveryManager) {
            throw new UnsupportedOperationException("not allowed in testStudentRedo");
        }
    }

    @Test
    public void testStudentRedo() throws Exception {
        TestInstance ti = new RedoInstance(recoveryManagerClass);
        try {
            ti.tempFolder.create();
            try {
                ti.setup();
                ti.testStudentRedo();
            } finally {
                ti.cleanup();
            }
        } finally {
            ti.tempFolder.delete();
        }
    }

    private static class UndoInstance extends TestInstance {
        private UndoInstance(Class<? extends RecoveryManager> recoveryManagerClass) throws Exception {
            super(recoveryManagerClass, true);
        }

        @Override
        protected void runAnalysis(RecoveryManager recoveryManager) {
            throw new UnsupportedOperationException("not allowed in testStudentUndo");
        }

        @Override
        protected void runRedo(RecoveryManager recoveryManager) {
            throw new UnsupportedOperationException("not allowed in testStudentUndo");
        }
    }

    @Test
    public void testStudentUndo() throws Exception {
        TestInstance ti = new UndoInstance(recoveryManagerClass);
        try {
            ti.tempFolder.create();
            try {
                ti.setup();
                ti.testStudentUndo();
            } finally {
                ti.cleanup();
            }
        } finally {
            ti.tempFolder.delete();
        }
    }

    private static class IntegrationInstance extends TestInstance {
        private IntegrationInstance(Class<? extends RecoveryManager> recoveryManagerClass) throws
            Exception {
            super(recoveryManagerClass, false);
        }

        @Override
        protected void runAnalysis(RecoveryManager recoveryManager) {
            throw new UnsupportedOperationException("not allowed in testStudentIntegration");
        }

        @Override
        protected void runRedo(RecoveryManager recoveryManager) {
            throw new UnsupportedOperationException("not allowed in testStudentIntegration");
        }

        @Override
        protected void runUndo(RecoveryManager recoveryManager) {
            throw new UnsupportedOperationException("not allowed in testStudentIntegration");
        }
    }

    @Test
    public void testStudentIntegration() throws Exception {
        TestInstance ti = new IntegrationInstance(recoveryManagerClass);
        try {
            ti.tempFolder.create();
            try {
                ti.setup();
                ti.testStudentIntegration();
            } finally {
                ti.cleanup();
            }
        } finally {
            ti.tempFolder.delete();
        }
    }

    private static abstract class TestInstance extends TestARIESStudent {
        private final Class<? extends RecoveryManager> recoveryManagerClass;
        private final Constructor<? extends RecoveryManager> recoveryManagerConstructor;
        private final boolean limited;

        private TestInstance(Class<? extends RecoveryManager> recoveryManagerClass,
                             boolean limited) throws Exception {
            this.recoveryManagerClass = recoveryManagerClass;
            this.recoveryManagerConstructor = recoveryManagerClass.getDeclaredConstructor(
                                                  LockContext.class,
                                                  Function.class,
                                                  Consumer.class,
                                                  Supplier.class,
                                                  boolean.class
                                              );
            this.limited = limited;
        }

        @Override
        protected RecoveryManager loadRecoveryManager(String dir) throws Exception {
            RecoveryManager recoveryManager = new DelegatedRecoveryManager(recoveryManagerClass,
                    recoveryManagerConstructor);
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
            if (limited) {
                recoveryManager = new LimitedRecoveryManager((DelegatedRecoveryManager) recoveryManager);
            }
            return recoveryManager;
        }

        @Override
        protected void shutdownRecoveryManager(RecoveryManager recoveryManager) throws Exception {
            getLogManager(recoveryManager).close();
            getBufferManager(recoveryManager).evictAll();
            getBufferManager(recoveryManager).close();
            getDiskSpaceManager(recoveryManager).close();
            DummyTransaction.cleanupTransactions();
        }

        @Override
        protected BufferManager getBufferManager(RecoveryManager recoveryManager) throws Exception {
            DelegatedRecoveryManager drm = limited ? ((LimitedRecoveryManager) recoveryManager).inner : ((
                                               DelegatedRecoveryManager) recoveryManager);
            return (BufferManager) recoveryManagerClass.getDeclaredField("bufferManager").get(drm.inner);
        }

        @Override
        protected DiskSpaceManager getDiskSpaceManager(RecoveryManager recoveryManager) throws Exception {
            DelegatedRecoveryManager drm = limited ? ((LimitedRecoveryManager) recoveryManager).inner : ((
                                               DelegatedRecoveryManager) recoveryManager);
            return (DiskSpaceManager) recoveryManagerClass.getDeclaredField("diskSpaceManager").get(drm.inner);
        }

        @Override
        protected LogManager getLogManager(RecoveryManager recoveryManager) throws Exception {
            DelegatedRecoveryManager drm = limited ? ((LimitedRecoveryManager) recoveryManager).inner : ((
                                               DelegatedRecoveryManager) recoveryManager);
            return (LogManager) recoveryManagerClass.getDeclaredField("logManager").get(drm.inner);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected List<String> getLockRequests(RecoveryManager recoveryManager) throws Exception {
            DelegatedRecoveryManager drm = limited ? ((LimitedRecoveryManager) recoveryManager).inner : ((
                                               DelegatedRecoveryManager) recoveryManager);
            return (List<String>) recoveryManagerClass.getDeclaredField("lockRequests").get(drm.inner);
        }

        @Override
        protected long getTransactionCounter(RecoveryManager recoveryManager) throws Exception {
            DelegatedRecoveryManager drm = limited ? ((LimitedRecoveryManager) recoveryManager).inner : ((
                                               DelegatedRecoveryManager) recoveryManager);
            return drm.transactionCounter;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Map<Long, Long> getDirtyPageTable(RecoveryManager recoveryManager) throws Exception {
            DelegatedRecoveryManager drm = limited ? ((LimitedRecoveryManager) recoveryManager).inner : ((
                                               DelegatedRecoveryManager) recoveryManager);
            return (Map<Long, Long>) recoveryManagerClass.getDeclaredField("dirtyPageTable").get(drm.inner);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Map<Long, TransactionTableEntry> getTransactionTable(RecoveryManager recoveryManager)
        throws Exception {
            DelegatedRecoveryManager drm = limited ? ((LimitedRecoveryManager) recoveryManager).inner : ((
                                               DelegatedRecoveryManager) recoveryManager);
            return (Map<Long, TransactionTableEntry>)
                   recoveryManagerClass.getDeclaredField("transactionTable").get(drm.inner);
        }

        @Override
        protected void runAnalysis(RecoveryManager recoveryManager) throws Exception {
            DelegatedRecoveryManager drm = limited ? ((LimitedRecoveryManager) recoveryManager).inner : ((
                                               DelegatedRecoveryManager) recoveryManager);
            recoveryManagerClass.getDeclaredMethod("restartAnalysis").invoke(drm.inner);
        }

        @Override
        protected void runRedo(RecoveryManager recoveryManager) throws Exception {
            DelegatedRecoveryManager drm = limited ? ((LimitedRecoveryManager) recoveryManager).inner : ((
                                               DelegatedRecoveryManager) recoveryManager);
            recoveryManagerClass.getDeclaredMethod("restartRedo").invoke(drm.inner);
        }

        @Override
        protected void runUndo(RecoveryManager recoveryManager) throws Exception {
            DelegatedRecoveryManager drm = limited ? ((LimitedRecoveryManager) recoveryManager).inner : ((
                                               DelegatedRecoveryManager) recoveryManager);
            recoveryManagerClass.getDeclaredMethod("restartUndo").invoke(drm.inner);
        }
    }

    private static class LimitedRecoveryManager implements RecoveryManager {
        private DelegatedRecoveryManager inner;

        private LimitedRecoveryManager(DelegatedRecoveryManager inner) {
            this.inner = inner;
        }

        @Override
        public void initialize() {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public void startTransaction(Transaction transaction) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public long commit(long transNum) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public long abort(long transNum) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public long end(long transNum) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public void pageFlushHook(long pageLSN) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public void diskIOHook(long pageNum) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                                 byte[] after) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public long logAllocPart(long transNum, int partNum) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public long logFreePart(long transNum, int partNum) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public long logAllocPage(long transNum, long pageNum) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public long logFreePage(long transNum, long pageNum) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public void savepoint(long transNum, String name) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public void releaseSavepoint(long transNum, String name) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public void rollbackToSavepoint(long transNum, String name) {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public void checkpoint() {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public Runnable restart() {
            throw new UnsupportedOperationException("this method may not be used");
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("this method may not be used");
        }
    }

    private static class DelegatedRecoveryManager implements RecoveryManager {
        private final Class<? extends RecoveryManager> recoveryManagerClass;
        RecoveryManager inner;
        long transactionCounter = 0L;

        DelegatedRecoveryManager(Class<? extends RecoveryManager> recoveryManagerClass,
                                 Constructor<? extends RecoveryManager> recoveryManagerConstructor) throws Exception {
            this.recoveryManagerClass = recoveryManagerClass;
            this.inner = recoveryManagerConstructor.newInstance(
                             new DummyLockContext(new Pair<>("database", 0L)),
                             (Function<Long, Transaction>) DummyTransaction::create,
                             (Consumer<Long>) t -> transactionCounter = t,
                             (Supplier<Long>) () -> transactionCounter,
                             true
                         );
        }

        @Override
        public void initialize() {
            inner.initialize();
        }

        @Override
        public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
            inner.setManagers(diskSpaceManager, bufferManager);
        }

        @Override
        public void startTransaction(Transaction transaction) {
            inner.startTransaction(transaction);
        }

        @Override
        public long commit(long transNum) {
            return inner.commit(transNum);
        }

        @Override
        public long abort(long transNum) {
            return inner.abort(transNum);
        }

        @Override
        public long end(long transNum) {
            return inner.end(transNum);
        }

        @Override
        public void pageFlushHook(long pageLSN) {
            inner.pageFlushHook(pageLSN);
        }

        @Override
        public void diskIOHook(long pageNum) {
            inner.diskIOHook(pageNum);
        }

        @Override
        public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                                 byte[] after) {
            return inner.logPageWrite(transNum, pageNum, pageOffset, before, after);
        }

        @Override
        public long logAllocPart(long transNum, int partNum) {
            return inner.logAllocPart(transNum, partNum);
        }

        @Override
        public long logFreePart(long transNum, int partNum) {
            return inner.logFreePart(transNum, partNum);
        }

        @Override
        public long logAllocPage(long transNum, long pageNum) {
            return inner.logAllocPage(transNum, pageNum);
        }

        @Override
        public long logFreePage(long transNum, long pageNum) {
            long rv = inner.logFreePage(transNum, pageNum);
            try {
                Map<Long, TransactionTableEntry> transactionTable = (Map<Long, TransactionTableEntry>)
                        recoveryManagerClass.getDeclaredField("transactionTable").get(inner);
                Map<Long, Long> dirtyPageTable = (Map<Long, Long>)
                                                 recoveryManagerClass.getDeclaredField("dirtyPageTable").get(inner);
                transactionTable.get(transNum).touchedPages.add(pageNum);
                dirtyPageTable.remove(pageNum);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return rv;
        }

        @Override
        public void savepoint(long transNum, String name) {
            inner.savepoint(transNum, name);
        }

        @Override
        public void releaseSavepoint(long transNum, String name) {
            inner.releaseSavepoint(transNum, name);
        }

        @Override
        public void rollbackToSavepoint(long transNum, String name) {
            inner.rollbackToSavepoint(transNum, name);
        }

        @Override
        public void checkpoint() {
            inner.checkpoint();
        }

        @Override
        public Runnable restart() {
            return inner.restart();
        }

        @Override
        public void close() {
            inner.close();
        }
    }
}
