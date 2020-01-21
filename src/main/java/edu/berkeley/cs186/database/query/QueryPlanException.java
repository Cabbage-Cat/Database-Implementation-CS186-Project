package edu.berkeley.cs186.database.query;

public class QueryPlanException extends RuntimeException {
    private String message;

    public QueryPlanException(String message) {
        this.message = message;
    }

    public QueryPlanException(Exception e) {
        this.message = e.getClass().toString() + ": " + e.getMessage();
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}

