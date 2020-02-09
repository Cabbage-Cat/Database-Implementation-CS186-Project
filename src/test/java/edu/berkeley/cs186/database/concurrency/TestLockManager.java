package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.AbstractTransactionContext;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@Category({Proj4Tests.class, Proj4Part1Tests.class})
public class TestLockManager {
    private LoggingLockManager lockman;
    private TransactionContext[] transactions;
    private ResourceName dbResource;
    private ResourceName[] tables;

    // 2 seconds per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                2000 * TimeoutScaling.factor)));

    static boolean holds(LockManager lockman, TransactionContext transaction, ResourceName name,
                         LockType type) {
        List<Lock> locks = lockman.getLocks(transaction);
        if (locks == null) {
            return false;
        }
        for (Lock lock : locks) {
            if (lock.name == name && lock.lockType == type) {
                return true;
            }
        }
        return false;
    }

    @Before
    public void setUp() {
        lockman = new LoggingLockManager();
        transactions = new TransactionContext[8];
        dbResource = new ResourceName(new Pair<>("database", 0L));
        tables = new ResourceName[transactions.length];
        for (int i = 0; i < transactions.length; ++i) {
            transactions[i] = new DummyTransactionContext(lockman, i);
            tables[i] = new ResourceName(dbResource, new Pair<>("table" + i, (long) i));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireRelease() {
        DeterministicRunner runner = new DeterministicRunner(1);

        runner.run(0, () -> {
            lockman.acquireAndRelease(transactions[0], tables[0], LockType.S, Collections.emptyList());
            lockman.acquireAndRelease(transactions[0], tables[1], LockType.S, Collections.singletonList(tables[0]));
        });
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], tables[0]));
        assertEquals(Collections.emptyList(), lockman.getLocks(tables[0]));
        assertEquals(LockType.S, lockman.getLockType(transactions[0], tables[1]));
        assertEquals(Collections.singletonList(new Lock(tables[1], LockType.S, 0L)),
                     lockman.getLocks(tables[1]));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseQueue() {
        DeterministicRunner runner = new DeterministicRunner(2);

        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                   Collections.emptyList()));
        runner.run(1, () -> lockman.acquireAndRelease(transactions[1], tables[1], LockType.X,
                   Collections.emptyList()));
        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[1], LockType.X,
                   Collections.singletonList(tables[0])));
        assertEquals(LockType.X, lockman.getLockType(transactions[0], tables[0]));
        assertEquals(Collections.singletonList(new Lock(tables[0], LockType.X, 0L)),
                     lockman.getLocks(tables[0]));
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], tables[1]));
        assertEquals(Collections.singletonList(new Lock(tables[1], LockType.X, 1L)),
                     lockman.getLocks(tables[1]));
        assertTrue(transactions[0].getBlocked());

        runner.join(1);
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseDuplicateLock() {
        DeterministicRunner runner = new DeterministicRunner(1);

        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                   Collections.emptyList()));
        try {
            runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                       Collections.emptyList()));
            fail();
        } catch (DuplicateLockRequestException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseNotHeld() {
        DeterministicRunner runner = new DeterministicRunner(1);

        runner.run(0, () -> lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                   Collections.emptyList()));
        try {
            runner.run(0, () ->
                       lockman.acquireAndRelease(transactions[0], tables[2], LockType.X, Arrays.asList(tables[0],
                                                 tables[1])));
            fail();
        } catch (NoLockHeldException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testAcquireReleaseUpgrade() {
        DeterministicRunner runner = new DeterministicRunner(1);

        runner.run(0, () -> {
            lockman.acquireAndRelease(transactions[0], tables[0], LockType.S, Collections.emptyList());
            lockman.acquireAndRelease(transactions[0], tables[0], LockType.X,
                                      Collections.singletonList(tables[0]));
        });
        assertEquals(LockType.X, lockman.getLockType(transactions[0], tables[0]));
        assertEquals(Collections.singletonList(new Lock(tables[0], LockType.X, 0L)),
                     lockman.getLocks(tables[0]));
        assertFalse(transactions[0].getBlocked());

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireLock() {
        DeterministicRunner runner = new DeterministicRunner(2);

        runner.run(0, () -> lockman.acquire(transactions[0], tables[0], LockType.S));
        runner.run(1, () -> lockman.acquire(transactions[1], tables[1], LockType.X));
        assertEquals(LockType.S, lockman.getLockType(transactions[0], tables[0]));
        assertEquals(Collections.singletonList(new Lock(tables[0], LockType.S, 0L)),
                     lockman.getLocks(tables[0]));
        assertEquals(LockType.X, lockman.getLockType(transactions[1], tables[1]));
        assertEquals(Collections.singletonList(new Lock(tables[1], LockType.X, 1L)),
                     lockman.getLocks(tables[1]));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquireLockFail() {
        DeterministicRunner runner = new DeterministicRunner(1);

        TransactionContext t1 = transactions[0];
        ResourceName r1 = dbResource;

        runner.run(0, () -> lockman.acquire(t1, r1, LockType.X));
        try {
            runner.run(0, () -> lockman.acquire(t1, r1, LockType.X));
            fail();
        } catch (DuplicateLockRequestException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleReleaseLock() {
        DeterministicRunner runner = new DeterministicRunner(1);

        runner.run(0, () -> {
            lockman.acquire(transactions[0], dbResource, LockType.X);
            lockman.release(transactions[0], dbResource);
        });
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], dbResource));
        assertEquals(Collections.emptyList(), lockman.getLocks(dbResource));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleConflict() {
        DeterministicRunner runner = new DeterministicRunner(2);

        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.X));
        assertEquals(LockType.X, lockman.getLockType(transactions[0], dbResource));
        assertEquals(LockType.NL, lockman.getLockType(transactions[1], dbResource));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 0L)),
                     lockman.getLocks(dbResource));
        assertFalse(transactions[0].getBlocked());
        assertTrue(transactions[1].getBlocked());

        runner.run(0, () -> lockman.release(transactions[0], dbResource));
        assertEquals(LockType.NL, lockman.getLockType(transactions[0], dbResource));
        assertEquals(LockType.X, lockman.getLockType(transactions[1], dbResource));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 1L)),
                     lockman.getLocks(dbResource));
        assertFalse(transactions[0].getBlocked());
        assertFalse(transactions[1].getBlocked());

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSXS() {
        DeterministicRunner runner = new DeterministicRunner(3);
        List<Boolean> blocked_status = new ArrayList<>();

        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.S));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.X));
        runner.run(2, () -> lockman.acquire(transactions[2], dbResource, LockType.S));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 0L)),
                     lockman.getLocks(dbResource));
        for (int i = 0; i < 3; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, true, true), blocked_status);

        runner.run(0, () -> lockman.release(transactions[0], dbResource));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 1L)),
                     lockman.getLocks(dbResource));
        blocked_status.clear();
        for (int i = 0; i < 3; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, true), blocked_status);

        runner.run(1, () -> lockman.release(transactions[1], dbResource));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 2L)),
                     lockman.getLocks(dbResource));
        blocked_status.clear();
        for (int i = 0; i < 3; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, false), blocked_status);

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testXSXS() {
        DeterministicRunner runner = new DeterministicRunner(4);
        List<Boolean> blocked_status = new ArrayList<>();

        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.S));
        runner.run(2, () -> lockman.acquire(transactions[2], dbResource, LockType.X));
        runner.run(3, () -> lockman.acquire(transactions[3], dbResource, LockType.S));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 0L)),
                     lockman.getLocks(dbResource));
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, true, true, true), blocked_status);

        runner.run(0, () -> lockman.release(transactions[0], dbResource));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 1L)),
                     lockman.getLocks(dbResource));
        blocked_status.clear();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, true, true), blocked_status);

        runner.run(1, () -> lockman.release(transactions[1], dbResource));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 2L)),
                     lockman.getLocks(dbResource));
        blocked_status.clear();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, false, true), blocked_status);

        runner.run(2, () -> lockman.release(transactions[2], dbResource));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.S, 3L)),
                     lockman.getLocks(dbResource));
        blocked_status.clear();
        for (int i = 0; i < 4; ++i) { blocked_status.add(i, transactions[i].getBlocked()); }
        assertEquals(Arrays.asList(false, false, false, false), blocked_status);

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromoteLock() {
        DeterministicRunner runner = new DeterministicRunner(1);

        runner.run(0, () -> {
            lockman.acquire(transactions[0], dbResource, LockType.S);
            lockman.promote(transactions[0], dbResource, LockType.X);
        });
        assertEquals(LockType.X, lockman.getLockType(transactions[0], dbResource));
        assertEquals(Collections.singletonList(new Lock(dbResource, LockType.X, 0L)),
                     lockman.getLocks(dbResource));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromoteLockNotHeld() {
        DeterministicRunner runner = new DeterministicRunner(1);
        try {
            runner.run(0, () -> lockman.promote(transactions[0], dbResource, LockType.X));
            fail();
        } catch (NoLockHeldException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromoteLockAlreadyHeld() {
        DeterministicRunner runner = new DeterministicRunner(1);

        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        try {
            runner.run(0, () -> lockman.promote(transactions[0], dbResource, LockType.X));
            fail();
        } catch (DuplicateLockRequestException e) {
            // do nothing
        }

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testFIFOQueueLocks() {
        DeterministicRunner runner = new DeterministicRunner(3);

        runner.run(0, () -> lockman.acquire(transactions[0], dbResource, LockType.X));
        runner.run(1, () -> lockman.acquire(transactions[1], dbResource, LockType.X));
        runner.run(2, () -> lockman.acquire(transactions[2], dbResource, LockType.X));

        assertTrue(holds(lockman, transactions[0], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[1], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[2], dbResource, LockType.X));

        runner.run(0, () -> lockman.release(transactions[0], dbResource));

        assertFalse(holds(lockman, transactions[0], dbResource, LockType.X));
        assertTrue(holds(lockman, transactions[1], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[2], dbResource, LockType.X));

        runner.run(1, () -> lockman.release(transactions[1], dbResource));

        assertFalse(holds(lockman, transactions[0], dbResource, LockType.X));
        assertFalse(holds(lockman, transactions[1], dbResource, LockType.X));
        assertTrue(holds(lockman, transactions[2], dbResource, LockType.X));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testStatusUpdates() {
        DeterministicRunner runner = new DeterministicRunner(2);

        TransactionContext t1 = transactions[0];
        TransactionContext t2 = transactions[1];

        ResourceName r1 = dbResource;

        runner.run(0, () -> lockman.acquire(t1, r1, LockType.X));
        runner.run(1, () -> lockman.acquire(t2, r1, LockType.X));

        assertTrue(holds(lockman, t1, r1, LockType.X));
        assertFalse(holds(lockman, t2, r1, LockType.X));
        assertFalse(t1.getBlocked());
        assertTrue(t2.getBlocked());

        runner.run(0, () -> lockman.release(t1, r1));

        assertFalse(holds(lockman, t1, r1, LockType.X));
        assertTrue(holds(lockman, t2, r1, LockType.X));
        assertFalse(t1.getBlocked());
        assertFalse(t2.getBlocked());

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testTableEventualUpgrade() {
        DeterministicRunner runner = new DeterministicRunner(2);

        TransactionContext t1 = transactions[0];
        TransactionContext t2 = transactions[1];

        ResourceName r1 = dbResource;

        runner.run(0, () -> lockman.acquire(t1, r1, LockType.S));
        runner.run(1, () -> lockman.acquire(t2, r1, LockType.S));

        assertTrue(holds(lockman, t1, r1, LockType.S));
        assertTrue(holds(lockman, t2, r1, LockType.S));

        runner.run(0, () -> lockman.promote(t1, r1, LockType.X));

        assertTrue(holds(lockman, t1, r1, LockType.S));
        assertFalse(holds(lockman, t1, r1, LockType.X));
        assertTrue(holds(lockman, t2, r1, LockType.S));

        runner.run(1, () -> lockman.release(t2, r1));

        assertTrue(holds(lockman, t1, r1, LockType.X));
        assertFalse(holds(lockman, t2, r1, LockType.S));

        runner.run(0, () -> lockman.release(t1, r1));

        assertFalse(holds(lockman, t1, r1, LockType.X));
        assertFalse(holds(lockman, t2, r1, LockType.S));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testIntentBlockedAcquire() {
        DeterministicRunner runner = new DeterministicRunner(2);

        TransactionContext t1 = transactions[0];
        TransactionContext t2 = transactions[1];

        ResourceName r0 = dbResource;

        runner.run(0, () -> lockman.acquire(t1, r0, LockType.S));
        runner.run(1, () -> lockman.acquire(t2, r0, LockType.IX));

        assertTrue(holds(lockman, t1, r0, LockType.S));
        assertFalse(holds(lockman, t2, r0, LockType.IX));

        runner.run(0, () -> lockman.release(t1, r0));

        assertFalse(holds(lockman, t1, r0, LockType.S));
        assertTrue(holds(lockman, t2, r0, LockType.IX));

        runner.joinAll();
    }

    @Test
    @Category(PublicTests.class)
    public void testReleaseUnheldLock() {
        DeterministicRunner runner = new DeterministicRunner(1);

        TransactionContext t1 = transactions[0];
        try {
            runner.run(0, () -> lockman.release(t1, dbResource));
            fail();
        } catch (NoLockHeldException e) {
            // do nothing
        }

        runner.joinAll();
    }

}

