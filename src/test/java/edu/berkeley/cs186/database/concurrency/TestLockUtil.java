package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.Proj4Part2Tests;
import edu.berkeley.cs186.database.categories.Proj4Tests;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.PublicTests;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({Proj4Tests.class, Proj4Part2Tests.class})
public class TestLockUtil {
    private LoggingLockManager lockManager;
    private TransactionContext transaction;
    private LockContext dbContext;
    private LockContext tableContext;
    private LockContext[] pageContexts;

    // 1 second per test
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                1000 * TimeoutScaling.factor)));

    @Before
    public void setUp() {
        lockManager = new LoggingLockManager();
        transaction = new DummyTransactionContext(lockManager, 0);
        dbContext = lockManager.databaseContext();
        tableContext = dbContext.childContext("table1", 1);
        pageContexts = new LockContext[8];
        for (int i = 0; i < pageContexts.length; ++i) {
            pageContexts[i] = tableContext.childContext((long) i);
        }
        TransactionContext.setTransaction(transaction);
    }

    @Test
    @Category(PublicTests.class)
    public void testRequestNullTransaction() {
        lockManager.startLog();
        TransactionContext.setTransaction(null);
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Collections.emptyList(), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleAcquire() {
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Arrays.asList(
                         "acquire 0 database IS",
                         "acquire 0 database/table1 IS",
                         "acquire 0 database/table1/4 S"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimplePromote() {
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.X);
        assertEquals(Arrays.asList(
                         "promote 0 database IX",
                         "promote 0 database/table1 IX",
                         "promote 0 database/table1/4 X"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testIStoS() {
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        pageContexts[4].release(transaction);
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                         "acquire-and-release 0 database/table1 S [database/table1]"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleEscalate() {
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                         "acquire-and-release 0 database/table1 S [database/table1, database/table1/4]"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testIXBeforeIS() {
        LockUtil.ensureSufficientLockHeld(pageContexts[3], LockType.X);
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(pageContexts[4], LockType.S);
        assertEquals(Collections.singletonList(
                         "acquire 0 database/table1/4 S"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSIX1() {
        LockUtil.ensureSufficientLockHeld(pageContexts[3], LockType.X);
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                         "acquire-and-release 0 database/table1 SIX [database/table1]"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSIX2() {
        LockUtil.ensureSufficientLockHeld(pageContexts[1], LockType.S);
        LockUtil.ensureSufficientLockHeld(pageContexts[2], LockType.S);
        LockUtil.ensureSufficientLockHeld(pageContexts[3], LockType.X);
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
        assertEquals(Collections.singletonList(
                         "acquire-and-release 0 database/table1 SIX [database/table1, database/table1/1, database/table1/2]"
                     ), lockManager.log);
    }

    @Test
    @Category(PublicTests.class)
    public void testSimpleNL() {
        lockManager.startLog();
        LockUtil.ensureSufficientLockHeld(tableContext, LockType.NL);
        assertEquals(Collections.emptyList(), lockManager.log);
    }

}

