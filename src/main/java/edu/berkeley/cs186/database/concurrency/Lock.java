package edu.berkeley.cs186.database.concurrency;

/**
 * Represents a lock held by a transaction on a resource.
 */
public class Lock {
    public ResourceName name;
    public LockType lockType;
    public Long transactionNum;

    public Lock(ResourceName name, LockType lockType, long transactionNum) {
        this.name = name;
        this.lockType = lockType;
        this.transactionNum = transactionNum;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Lock) {
            Lock l = (Lock) other;
            return l.name.equals(name) && lockType == l.lockType && transactionNum.equals(l.transactionNum);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 37 * (37 * name.hashCode() + lockType.hashCode()) + transactionNum.hashCode();
    }

    @Override
    public String toString() {
        return "T" + transactionNum.toString() + ": " + lockType.toString() + "(" + name.toString() + ")";
    }
}
