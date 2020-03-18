# Project 4: Locking

This project is divided into two parts.

Part 1 is due: **Thursday, 4/2/2020, 11:59 PM**.

Part 2 is due: **Wednesday, 4/8/2020, 11:59 PM**.

Part 3 is due: **Tuesday, 4/14/2020, 11:59 PM**.

Part 3 requires Part 2 to be completed first which requires Part 1 to be completed first. See the Grading section at the bottom of this document for notes on how your score will be computed.

**You will not be able to use slip minutes for the Part 1 and 2 deadline.** The standard late penalty
(33%/day, counted in days not minutes) will apply after each deadline. Slip minutes
may be used for the Part 3 deadline, and the late penalty for each part are independent.

## Overview

In this project, you will implement multigranularity locking and integrate
it into the codebase.

## Prerequisites

You should watch both the Transactions and Concurrency Part 1 and Transactions
and Concurrency Part 2 lectures before working on this project.

## Getting Started

The test cases for Part 1 are located in
`src/test/java/edu/berkeley/cs186/database/concurrency/TestLockType` and
`src/test/java/edu/berkeley/cs186/database/concurrency/TestLockManager`

The test cases for Part 2 are located in
`src/test/java/edu/berkeley/cs186/database/concurrency/TestLockContext` and
`src/test/java/edu/berkeley/cs186/database/concurrency/TestLockUtil.java`


The test cases for Part 3 are located in
`src/test/java/edu/berkeley/cs186/database/TestDatabaseDeadlockPrecheck.java` and
`src/test/java/edu/berkeley/cs186/database/TestDatabaseLocking.java`.

To build and test your code in the container, run:
```bash
mvn clean test -D proj=4
```

There should be 70 failures, 1 error, and 76 tests run.

To run just Part 1 tests, run:
```bash
mvn clean test -D proj=4Part1
```

There should be 22 failures and 23 tests run.

To run just Part 2 tests, run:
```bash
mvn clean test -D proj=4Part2
```

There should be 25 and 28 tests run. (These numbers may
change as you work through Part 1).

To run just Part 3 tests, run:
```bash
mvn clean test -D proj=4Part3
```

There should be 23, 1 error, and 25 tests run. (These numbers may
change as you work through Part 1 and 2).

## Part 0: Understand the Skeleton Code

Read through all of the code in the `concurrency` directory (including the classes that you are not touching -
they may contain useful methods or information pertinent to the project). Many comments contain
critical information on how you must implement certain functions. If you do not obey
the comments, you will lose points.

Try to understand how each class fits in: what is each class responsible for, what are
all the methods you have to implement, and how does each one manipulate the internal state.
Trying to code one method at a time without understanding how all the parts of the lock
manager work often results in having to rewrite significant amounts of code.

The skeleton code divides multigranularity locking into three layers.
- The `LockManager` object manages all the locks, treating each resource as independent
  (it doesn't consider the relationship between two resources: it does not understand that
  a table is at a lower level in the hierarchy than the database, and just treats locks on
  both in exactly the same way). This level is responsible for blocking/unblocking transactions
  as necessary, and is the single source of authority on whether a transaction has a certain lock. If
  the `LockManager` says T1 has X(database), then T1 has X(database).
- A collection of `LockContext` objects, which each represent a single lockable object
  (e.g. a page or a table) lies on top of the `LockManager`. The `LockContext` objects
  are connected according to the hierarchy (e.g. a `LockContext` for a table has the database
  context as its parent, and its pages' contexts as children). The `LockContext` objects
  all share a single `LockManager`, and enforces multigranularity constraints on all
  lock requests (e.g. an exception should be thrown if a transaction attempts to request
  X(table) without IX(database)).
- A declarative layer lies on top of the collection of `LockContext` objects, and is responsible
  for acquiring all the intent locks needed for each S or X request that the database uses
  (e.g. if S(page) is requested, this layer would be responsible for requesting IS(database), IS(table)
  if necessary).

![Layers of locking in Proj4](images/proj4-layers.png?raw=true "Layers")

In Part 1, you will be implementing the bottom layer (`LockManager`) and lock types.

In Part 2, you will be implementing the middle and top layer (`LockContext` and `LockUtil`)

In Part 3, you will be integrating everything into the codebase.

## Part 1: LockType and LockManager

[Part 1 README](proj4-part1-README.md)

## Part 2: LockContext and LockUtil

[Part 2 README](proj4-part2-README.md)

## Part 3: Integration

[Part 3 README](proj4-part3-README.md)

## Submitting the Assignment

See Piazza for submission instructions.

You may **not** modify the signature of any methods or classes that we
provide to you, but you're free to add helper methods.

You should make sure that all code you modify belongs to files with Proj4 todo comments in them
(e.g. don't add helper methods to PNLJOperator). A full list of files that you may modify follows:

- concurrency/LockType.java
- concurrency/LockManager.java
- concurrency/LockContext.java
- concurrency/LockUtil.java
- index/BPlusTree.java (Part 3 only)
- memory/Page.java (Part 3 only)
- table/PageDirectory.java (Part 3 only)
- table/Table.java (Part 3 only)
- Database.java (Part 3 only)

Make sure that your code does *not* use any static (non-final) variables - this may cause odd behavior when
running with maven vs. in your IDE (tests run through the IDE often run with a new instance
of Java for each test, so the static variables get reset, but multiple tests per Java instance
may be run when using maven, where static variables *do not* get reset).

For this project, we are being more strict on efficiency. If your code is timing out on edX, 
re-visit your code and try to optimize wherever you can.

## Testing

We strongly encourage testing your code yourself, especially after each part (rather than all at the end). The given
tests for this project (even more so than previous projects) are **not** comprehensive tests: it **is** possible to write
incorrect code that passes them all.

Things that you might consider testing for include: anything that we specify in the comments or in this document that a method should do
that you don't see a test already testing for, and any edge cases that you can think of. Think of what valid inputs
might break your code and cause it not to perform as intended, and add a test to make sure things are working.

To help you get started, here is one case that is **not** in the given tests (and will be included in the hidden tests):
if a transaction holds IX(database), IS(table), S(page) and promotes the
database lock to a SIX lock via `LockContext#promote`, `numChildLocks` should
be updated to be 0 for both the database and table contexts.

To add a unit test, open up the appropriate test file (all test files are located in `src/test/java/edu/berkeley/cs186/database`
or subdirectories of it), and simply add a new method to the test class, for example:

```
@Test
public void testPromoteSIXSaturation() {
    // your test code here
}
```

Many test classes have some setup code done for you already: take a look at other tests in the file for an idea of how to write the test code.

## Grading
- Part 1 is worth 35% of your Project 4 grade. 50% of that will come from public tests and 50% will come from hidden tests. We will only be running public tests on your Part 1 submission.
- Part 2 is worth 35% of your Project 4 grade. 50% of that will come from public tests and 50% will come from hidden tests. We will only be running public tests on your Part 2 submission.
- Part 3 is worth 30% of your Project 4 grade. 50% of that will come from public tests and 50% will come from hidden tests.

Your Part 3 submission will be used to run all hidden tests (and public Part 3 tests). If you do not have a submission for Part 3, you will receive a 0 on all hidden tests for Part 1, 2, and 3. You may go back and make changes to previous parts even after that part deadline has passed to submit to the final submission.

