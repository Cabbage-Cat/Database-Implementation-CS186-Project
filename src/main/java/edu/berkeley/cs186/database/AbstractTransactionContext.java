package edu.berkeley.cs186.database;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This transaction context implementation assumes that exactly one transaction runs
 * on a thread at a time, and that, aside from the unblock() method, no methods
 * of the transaction are called from a different thread than the thread that the
 * transaction is associated with. This implementation blocks the thread when
 * block() is called.
 */
public abstract class AbstractTransactionContext implements TransactionContext {
    private boolean blocked = false;
    private boolean startBlock = false;
    private final ReentrantLock transactionLock = new ReentrantLock();
    private final Condition unblocked = transactionLock.newCondition();

    /**
     * prepareBlock acquires the lock backing the condition variable that the transaction
     * waits on. Must be called before block(), and is used to ensure that the unblock() call
     * corresponding to the following block() call cannot be run before the transaction blocks.
     */
    @Override
    public void prepareBlock() {
        if (this.startBlock) {
            throw new IllegalStateException("already preparing to block");
        }
        this.transactionLock.lock();
        this.startBlock = true;
    }

    /**
     * Blocks the transaction (and thread). prepareBlock() must be called first.
     */
    @Override
    public void block() {
        if (!this.startBlock) {
            throw new IllegalStateException("prepareBlock() must be called before block()");
        }
        try {
            this.blocked = true;
            while (this.blocked) {
                this.unblocked.awaitUninterruptibly();
            }
        } finally {
            this.startBlock = false;
            this.transactionLock.unlock();
        }
    }

    /**
     * Unblocks the transaction (and thread running the transaction).
     */
    @Override
    public void unblock() {
        this.transactionLock.lock();
        try {
            this.blocked = false;
            this.unblocked.signal();
        } finally {
            this.transactionLock.unlock();
        }
    }

    @Override
    public boolean getBlocked() {
        return this.blocked;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void rethrow(Throwable t) throws T {
        // rethrows checked exceptions as unchecked
        throw (T) t;
    }
}
