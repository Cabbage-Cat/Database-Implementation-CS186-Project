package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.Collections;
import java.util.List;

/**
 * Represents a lock request on the queue, for
 * TRANSACTION requesting LOCK and releasing everything in RELEASEDLOCKS.
 * LOCK should be granted and everything in RELEASEDLOCKS should be released
 * *before* the transaction is unblocked.
 */
class LockRequest {
    TransactionContext transaction;
    Lock lock;
    List<Lock> releasedLocks;

    // Lock request for LOCK, that is not releasing anything.
    LockRequest(TransactionContext transaction, Lock lock) {
        this.transaction = transaction;
        this.lock = lock;
        this.releasedLocks = Collections.emptyList();
    }

    // Lock request for LOCK, in exchange for all the locks in RELEASEDLOCKS.
    LockRequest(TransactionContext transaction, Lock lock, List<Lock> releasedLocks) {
        this.transaction = transaction;
        this.lock = lock;
        this.releasedLocks = releasedLocks;
    }

    @Override
    public String toString() {
        return "Request for " + lock.toString() + " (releasing " + releasedLocks.toString() + ")";
    }
}