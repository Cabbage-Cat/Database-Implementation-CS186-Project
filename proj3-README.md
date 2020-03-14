# Project 3: Joins and Query Optimization

This project is divided into two parts.

Part 1 is due: **Friday, 3/13/2020, 11:59 PM**.

Part 2 is due: **Wednesday, 3/18/2020, 11:59 PM**.

Part 2 does not require Part 1 to be completed first, but you will need to make sure
any code you write for Part 1 does not throw an exception before starting Part 2. See
the Grading section at the bottom of this document for notes on how your score will
be computed.

**You will not be able to use slip minutes for the Part 1 deadline.** The standard late penalty
(33%/day, counted in days not minutes) will apply after the Part 1 deadline. Slip minutes
may be used for the Part 2 deadline, and the late penalty for the two parts are independent.

For example, if you submit Part 1 at 5:30 AM two days after it is due,
and Part 2 at 6:00 PM the day after it is due, you will recieve:
- 66% penalty on your Part 1 submission
- No penalty on your Part 2 submission
- A total of 1080 slip minutes consumed

## Overview

In this project, you will implement some join algorithms and a limited
version of the Selinger optimizer.

## Prerequisites

You should watch the Iterators and Joins lectures before working on Part 1 of
this project.

You should watch both the Query Optimization I: Plan Space and Query
Optimization II: Cost and Search lectures before working on Part 2 of this
project.

## Getting Started

The test cases for Part 1 are located in `src/test/java/edu/berkeley/cs186/database/query`
(`TestJoinOperator`, `TestSortOperator`, and `TestGraceHashJoin`).

The test cases for Part 2 are located in `src/test/java/edu/berkeley/cs186/database/query`
(`TestBasicQuery`, `TestOptimization2`, `TestOptimizationJoins`,
`TestSingleAccess`, and `TestSingleAccessJoins`).

The files you will be responsible for modifying can all be found in `src/main/java/edu/berkeley/cs186/database/query`. A full list of files you will be submitting can be found in the section "Submitting the Assignment".

To build and test your code in the container, run the following:

```bash
git pull origin master
mvn clean test -D proj=3
```

There should be 16 failures, 11 errors, and 32 tests run.

To run just Part 1 tests, run:

```bash
mvn clean test -D proj=3Part1
```

There should be 13 failures, 3 errors, and 17 tests run.

To run just Part 2 tests, run:

```bash
mvn clean test -D proj=3Part2
```

There should be 3 failures, 8 errors, and 15 tests run.

## Part 0: Understand the Skeleton Code

### common

The `common/iterator` directory  contains an interface called a `BacktrackingIterator`.
Iterators that implement this will be able to mark a point during iteration,
and reset back to that mark. For example, here we have a backtracking iterator
that just returns 1, 2, and 3, but can backtrack:

```java
BackTrackingIterator<Integer> iter = new BackTrackingIteratorImplementation();
iter.next(); //returns 1
iter.next(); //returns 2
iter.markPrev(); // marks the previously returned value, 2
iter.next(); //returns 3
iter.hasNext(); //returns false
iter.reset(); // reset to the marked value - next() will return the marked value (2)
iter.hasNext(); // returns true
iter.next(); //returns 2
iter.markNext(); // mark the value to be returned next, 3
iter.next(); // returns 3
iter.hasNext(); // returns false
iter.reset(); // reset to the marked value - next() will return the marked value (3)
iter.hasNext(); // returns true
iter.next(); // returns 3
```

`ArrayBacktrackingIterator` implements this interface. It takes in an array and
returns a backtracking iterator over the values in that array.

### query

The `query` directory contains what are called query operators. A single query
to the database may be expressed as a composition of these operators. The
sequential scan and index scan operators fetch data from a single table, while
the remaining operators take as input the output of one or two operators,
transform or combine the input (for example: projecting away columns, or joining
records from two input operators together), and return a collection of records.

This is the _volcano model_, where the operators are layered atop one
another, and each operator requests tuples from the input operator(s) as
it needs to generate its next output tuple. Note that each operator only fetches
tuples from its input operator(s) as needed, rather than all at once!

![image of volcano model](images/proj3-volcano-model.png)

A query plan is a composition of query operators, and it describes _how_ a query
is executed. Recall that SQL is a _declarative_ language - the user does not
specify _how_ a query is run, and only _what_ the query should return.
Therefore, there are often many possible query plans for a given query.

The various `*Operator` classes are the query operators. All operators extend the
`QueryOperator` class. `JoinOperator` in
particular is the base class of all the join operators, which various join
implementations extend. You should take a look at the methods that are
implemented for you in `JoinOperator`: it provides methods you may need to deal
with tables and the current transaction. You should not be dealing directly with `Table`
objects nor `TransactionContext` objects while implementing join algorithms in Part
1 (aside from passing them into methods that require them).

The `QueryPlan` class represents a query. Users of the database create queries
using the public methods (such as `join()`, `select()`, etc.) and then call
`execute` to generate a query plan for the query and get back an iterator over
the resulting data set (which is _not_ fully materialized: the iterator
generates each tuple as requested). The current implementation of `execute`
simply calls `executeNaive`, which joins tables in the order given; your task in
Part 2 will be to generate better query plans.

### Interface for querying

You should read through the [`Database.java`](README.md#databasejava) section
of the main README and browse through examples in
[`src/test/java/edu/berkeley/cs186/database/TestDatabase.java`](src/test/java/edu/berkeley/cs186/database/TestDatabase.java) to familiarize yourself with how queries are
written in our database.

After `execute()` has been called on a QueryPlan object, you can print the final query plan:
```java
Iterator<Record> result = query.execute();
QueryOperator finalOperator = query.getFinalOperator();
System.out.println(finalOperator.toString());
```
```
type: BNLJ
leftColumn: S.sid
rightColumn: E.sid
    (left)
    type: WHERE
    column: E.cid
    predicate: EQUALS
    value: CS 186
        type: SEQSCAN
        table: E

    (right)
    type: SEQSCAN
    table: S
```

## Part 1: Join Algorithms

[Part 1 README](proj3-part1-README.md)

## Part 2: Query Optimization

[Part 2 README](proj3-part2-README.md)

## Submitting the Assignment

See [the main readme](README.md#submitting-assignments) for submission instructions. The
assignment numbers for this project are proj3\_part1 and proj3\_part2.

You may **not** modify the signature of any methods or classes that we provide to youi, but you're free to
add helper methods.

You should make sure that all code you modify belongs to files with proj3 todo comments in them (e.g. don't add
helper methods to DataBox). A full list of files that you may modify follows:

- query/BNLJOperator.java
- query/SortOperator.java
- query/SortMergeOperator.java
- query/GraceHashJoin.java
- query/QueryPlan.java (Part 2 only)

Make sure that your code does *not* use any static (non-final) variables - this may cause odd behavior when
running with maven vs. in your IDE (tests run through the IDE often run with a new instance
of Java for each test, so the static variables get reset, but multiple tests per Java instance
may be run when using maven, where static variables *do not* get reset).

## Testing

We strongly encourage testing your code yourself. The given tests for this project are not comprehensive tests:
it is possible to write incorrect code that passes them all (but not get full score).

Things that you might consider testing for include: anything that we specify in the comments or in this document
that a method should do that you don't see a test already testing for, and any edge cases that you can think of.
Think of what valid inputs might break your code and cause it not to perform as intended, and add a test to make
sure things are working.

To help you get started, here is one case that is *not* in the given tests (and
will be included in the hidden tests): joining an empty table with another table
should result in an iterator that returns no records (`hasNext()` should return
false immediately).

To add a unit test, open up the appropriate test file and simply
add a new method to the file with a `@Test` annotation, for example:

```java
@Test
public void testEmptyBNLJ() {
    // your test code here
}
```

Many test classes have some setup code done for you already: take a look at other tests in the file for an idea
of how to write the test code.

## Grading
- **30% of your overall score will come from your submission for Part 1**. We will only be running released Part 1 tests (`database.query.*`) on your Part 1 submission.
- The rest of your score will come from your submission for Part 2 (testing both Part 1 and Part 2 code).
- 65% of your overall score will be made up of the tests released in this project (the tests that we provided
  in `database.query.*` and `database.table.stats.*`).
- 35% of your overall score will be made up of hidden, unreleased tests that we will run on your submission after the deadline.
- Part 1 is worth approximately 43.33% of your Project 3 grade and Part 2 is worth approximately 56.67% of your Project 3 grade.

