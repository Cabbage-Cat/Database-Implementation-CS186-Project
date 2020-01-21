package edu.berkeley.cs186.database.concurrency;

public class InvalidLockException extends RuntimeException {
    InvalidLockException(String message) {
        super(message);
    }
}

