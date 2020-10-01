package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null || lockType.equals(LockType.NL)) { return; }
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);
        if (explicitLockType.equals(LockType.NL)) {
            LockType ensureParentType = lockType.equals(LockType.S) ? LockType.IS : LockType.IX;
            ensureParent(transaction, lockContext.parent, ensureParentType);
            lockContext.acquire(transaction, lockType);
            return;
        }
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        if (LockType.substitutable(effectiveLockType, lockType)) { return; }

        if (lockType.equals(LockType.S)) {
            if (explicitLockType.equals(LockType.IS)) {
                lockContext.escalate(transaction);
            } else {
                ensureParent(transaction, lockContext.parent, LockType.IX);
                lockContext.promote(transaction, LockType.SIX);
            }
            //lockContext.promote(transaction, lockType);
        }
        else {
            ensureParent(transaction, lockContext.parent, LockType.IX);
            lockContext.escalate(transaction);
            if (!lockContext.getExplicitLockType(transaction).equals(LockType.X)) {
                lockContext.promote(transaction, LockType.X);
            }
        }

    }

    // TODO(proj4_part2): add helper methods as you see fit
    private static void ensureParent(TransactionContext transaction, LockContext parent, LockType lockType) {
        LockContext travel = parent;
        ArrayList<LockContext> lockContextArrayList = new ArrayList<>();
        while (travel != null) {
            lockContextArrayList.add(travel);
            travel = travel.parent;
        }
        Collections.reverse(lockContextArrayList);
        for (LockContext context : lockContextArrayList) {
            if (context.getExplicitLockType(transaction).equals(LockType.NL)) {
                context.acquire(transaction, lockType);
            }
            else if (!LockType.substitutable(context.getExplicitLockType(transaction), lockType)) {
                context.promote(transaction, lockType);
            }
        }
    }
}
