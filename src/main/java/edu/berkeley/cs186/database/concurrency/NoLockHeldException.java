package edu.berkeley.cs186.database.concurrency;

public class NoLockHeldException extends RuntimeException {
    NoLockHeldException(String message) {
        super(message);
    }
}

