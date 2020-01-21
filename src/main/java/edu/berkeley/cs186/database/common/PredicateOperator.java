package edu.berkeley.cs186.database.common;

public enum PredicateOperator {
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    GREATER_THAN,
    GREATER_THAN_EQUALS;

    public <T extends Comparable<T>> boolean evaluate(T a, T b) {
        switch (this) {
        case EQUALS:
            return a.compareTo(b) == 0;
        case NOT_EQUALS:
            return a.compareTo(b) != 0;
        case LESS_THAN:
            return a.compareTo(b) < 0;
        case LESS_THAN_EQUALS:
            return a.compareTo(b) <= 0;
        case GREATER_THAN:
            return a.compareTo(b) > 0;
        case GREATER_THAN_EQUALS:
            return a.compareTo(b) >= 0;
        }
        return false;
    }
}
