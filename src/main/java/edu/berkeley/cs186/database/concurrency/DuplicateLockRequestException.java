package edu.berkeley.cs186.database.concurrency;

public class DuplicateLockRequestException extends RuntimeException {
    DuplicateLockRequestException(String message) {
        super(message);
    }
}

