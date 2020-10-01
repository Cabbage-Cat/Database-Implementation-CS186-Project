package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held
    private static final boolean[][] lockTypeMatrix= {{true, true, true, true, false},
            {true, true, false, false, false},
            {true, false, true, false, false},
            {true, false, false, false, false},
            {false, false, false, false, false}
    };
    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */

    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (a == NL || b == NL) { return true; }
        return getCompatibleHelper(a, b);
    }
    private static boolean getCompatibleHelper(LockType a, LockType b) {
        int index1 = getLockTypeIndex(a);
        int index2 = getLockTypeIndex(b);
        return lockTypeMatrix[index1][index2];
    }
    private static int getLockTypeIndex(LockType a) {
        if (a == IS) { return 0; }
        if (a == IX) { return 1; }
        if (a == S) { return 2; }
        if (a == SIX) { return 3; }
        if (a == X) { return 4; }
        return -1;
    }
    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (parentLockType.equals(childLockType) || childLockType == NL) { return true; }
        if (parentLockType == NL) { return false; }
        if (childLockType == S || childLockType == IS) {
            return parentLockType == IS || parentLockType == IX;
        }
        if (childLockType == X || childLockType == IX || childLockType == SIX) {
            return parentLockType == IX || parentLockType == SIX;
        }
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (substitute == required || required == LockType.NL) { return true; }
        if (required == S) {
            return substitute == SIX || substitute == X;
        }
        if (required == IS) {
            return substitute == SIX || substitute == IX || substitute == S || substitute == X;
        }
        if (required == IX) {
            return substitute == SIX || substitute == X;
        }
        if (required == SIX) {
            return substitute == X;
        }
        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

