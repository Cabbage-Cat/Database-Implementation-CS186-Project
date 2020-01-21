package edu.berkeley.cs186.database.io;

/**
 * Exception thrown for errors while paging.
 */
public class PageException extends RuntimeException {
    public PageException() {
        super();
    }

    public PageException(Exception e) {
        super(e);
    }

    public PageException(String message) {
        super(message);
    }
}

