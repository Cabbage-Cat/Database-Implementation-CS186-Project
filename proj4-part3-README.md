# Project 4: Locking (Part 3)

**A working implementation of Part 2 is required for Part 3. If you have not yet finished
[Part 2](proj4-part2-README.md), you should do so before continuing.**

## Overview

In this part, you will integrate locking into the rest of the codebase.

## Integrating the Lock Manager and Lock Context

At this point, you should have a working, standalone lock manager and context. In
this section, you will modify code throughout the codebase to use locking.

This section will be mostly concerned with code in `database.memory.*`, `database.table.*`, and with the Database
and Transaction classes. We strongly recommend understanding how the codebase
is structured (described below) before starting, and maybe drawing out a diagram
of the relevant classes - it will save you a lot of time understanding the tasks.

The `database.memory` package contains the code that controls what data is in memory at any given time
(it's the buffer management layer). It lies on top of the disk space manager, and exposes `Page` objects
representing a single page. The rest of the codebase uses these objects to manipulate data on pages.
- The `Page` class represents a single page of data (on disk, but possibly cached in memory). The
  `PageBuffer` inner class implements the `Buffer` interface, and is how someone with a `Page` object
  is able to read and write to it. All reads are funnelled through the `Page.PageBuffer#get` method, and
  all writes are funnelled through the `Page.PageBuffer#put` method.

The `database.table` package contains table-related code.
- The `PageDirectory` class is our page directory implementation of a heap file. It exposes methods for
  finding data pages with a certain amount of free space.
- The `Table` class represents a table. It uses a `HeapFile` object (of which `PageDirectory` is an
  implementation) to manage its data pages. The lock context for a table and its heap file are the
  same.

The `database.index` package contains index-related code.
- The `BPlusTree` class represents a B+ tree index. B+ trees do not use a heap file to store its pages
  (there's no need for header pages to keep track of the data pages, since all the pages in a
  B+ tree are connected to each other), and instead get new pages directly through the buffer manager
  (which then allocates and loads a new page using the disk space manager). All nodes in the tree
  have access to the tree's lock context (the `treeContext` field in `InnerNode` and `LeafNode`
  is the same context as the `lockContext` field in `BPlusTree`).

In the `database` package we have the `Database` class.
- The `Database` class is the object that users of our database library interact with. The `Database` class
  represents the entire database, and manages the tables and indices of the database (creating Table/BPlusTree
  objects as needed and passing the appropriate table/index-level lock context to them). It contains the
  `TransactionContextImpl` and `TransactionImpl` inner classes, which represent
  the internal and external interfaces of a single transaction, respectively.
  All queries and changes in our database are done
  through transactions (and this is where much of the integration work will take place).

  In the constructor of `Database`, transactions with transaction number 0 and 1 are created and given X locks on every table
  before the tables and indices are loaded. This transaction ends when all tables and indices are loaded, and therefore
  starts/finishes at the start of _every_ test in this section. If you see conflicts in this section with an IX(database)
  lock or with X locks on every table, you are likely not properly releasing locks at the end of a transaction.

Notes on the tests: the tests use a syntax that you might not familiar with: the try-with-resources statement.
In the tests, this looks like:

```java
try (Transaction transaction = ....) {
    // code here
}
```

This is similar to the following:

```java
Transaction transaction = ....
try {
    // code here
} finally {
    transaction.close();
}
```

where `transaction.close()` is defined to be the same thing as `transaction.commit()`.

You will at some point find that all the tests in `TestDatabaseLocking` are
failing in `beforeEach` on the line `assertTrue(passedPreCheck)`. To fix this,
you will need to complete Task 2, and pass the test in
`TestDatabaseDeadlockPrecheck` (this test is the same check run in the
`beforeAll` method of `TestDatabaseLocking`).

Note: most of these tasks only require writing a few lines of code. Errors and incorrect output
are very frequently the result of uncaught bugs in `LockUtil`.

#### Task 1: Page-level locking

The simplest scheme for locking is to simply lock pages as we need them. As all reads and
writes to pages are performed via the `Page.PageBuffer` class, it suffices to
change only that.

Modify the `get` and `put` methods of `Page.PageBuffer` to lock the page (and
acquire locks up the hierarchy as needed) with the least permissive lock
types possible (Hint: the declarative layer may come in handy).

Note: no more tests will pass after doing this, see the next task for why.

You should modify `database/memory/Page.java` for this task (`PageBuffer` is an
inner class of `Page`).

#### Task 2: Releasing Locks

At this point, transactions should be acquiring lots of locks needed to do their
queries, but no locks are ever released, which means that
our database won't be able to do much after someone requests an X lock...and we do
just that when initializing the database!

We will be using Strict Two-Phase Locking in our database, which means that lock
releases only happen when the transaction finishes, in the `cleanup` method.

Modify the `close` method of `Database.TransactionContextImpl` to release all locks the
transaction acquired.

You should only use `LockContext#release` and not `LockManager#release` - `LockManager`
will not verify multigranularity constraints, but other transactions at the same time
assume that these constraints are met, so you do want these constraints to be maintained.

Hint: you can't just release the locks in any order! Think about in what order you are allowed to release
the locks.

You should modify `database/Database.java` for this task (`TransactionContextImpl` is an inner class
of Database), and should pass `testRecordRead`, `testSimpleTransactionCleanup`,
and `TestDatabaseDeadlockPrecheck#testDeadlock` after implementing this task and task 1.

(`testAutoEscalateDisabled` will also pass, until you start implementing task 6).

#### Task 3: Writes

At this point, you have successfully added locking to the database\*! This task,
and the remaining integration tasks, are focused on locking in a more efficient manner.

The first operation that we perform in an insert/update/delete of
a record involves reading some metadata from the page. Following our logic of reactively acquiring locks
only as needed, we would request an S lock on the page,
then request a promotion to an X lock when attempting to write to the page.
Given that we know we will eventually need an X lock, we should request it to begin with.

The methods we are optimizing here are `addRecord`, `updateRecord`, and `deleteRecord`. However,
`addRecord` first requests a page with free space from the page directory, which reads from the page.

You should therefore modify the following methods to request the appropriate X lock upfront.
- `PageDirectory#getPageWithSpace`
- `Table#updateRecord`
- `Table#deleteRecord`

\* (for DML operations, see Task 7 for DDL)

You should modify `database/table/PageDirectory.java` and `database/table/Table.java` for this task, and should
pass `testRecordWrite`, `testRecordUpdate`, `testRecordDelete` after
implementing this task.

#### Task 4: Table-Level Operations

Our approach of locking every page can be inefficient:
a full table scan over a table of 3000 pages will require acquiring over 3000 locks. This and other
table-level operations (in particular: table construction) can be optimized by locking the
entire table preemptively.

Reduce the locking overhead when performing table scans and table construction, by locking the
entire table when any of `Table#ridIterator`, `Database.TransactionContextImpl#sortedScan`,
`Database.TransactionContextImpl#sortedScanFrom`, or the `Table` constructor is called.

A useful method for this task is: `Database#getTableContext`.

You should modify `database/table/Table.java` and `database/Database.java` for this task,
and should pass `testTableScan` and `testSortedScanNoIndexLocking` after implementing this task.

(`testCreateTableSimple` will also pass, until you start implementing task 7).

#### Task 5: Indices

Locking pages as we read or write from them works for simple queries. But this is rather
inefficient for indices: if you use an index in any way, locking page by page, you
acquire around `h` locks (where `h` is the height of the index), but effectively have locked
the entire B+ tree (think about why this is the case!), which is bad for concurrency.

There are many ways to lock B+ trees efficiently (one simple method is known as "crabbing",
see the textbook for details and other approaches), but for
simplicity, we are just going to lock the entire tree for both reads and writes (locking page
by page with strict 2PL already acts as a lock on the entire tree, so we may as well just
get a lock on the whole tree to begin with). This is no better for concurrency, but at least reduces the locking overhead.

First, modify the BPlusTree constructor to disable locking at lower granularities
(take a look at the methods in `LockContext` for this).

Then, preemptively acquire locks in the following
methods in `BPlusTree`: `scanEqual`, `scanAll`, `scanGreaterEqual`, `get`, `put`, `bulkLoad`, `remove`,
`toSexp`, `toDot`, and the constructor of `BPlusTree`.

You should modify `database/index/BPlusTree.java` for this task, and
should pass `testBPlusTreeRestrict`, `testSortedScanLocking`, `testSearchOperationLocking`, and `testQueryWithIndex`
after implementing this task.

(`testCreateIndexSimple` will also pass, until you start implementing task 7).

#### Task 6: Auto-escalation

Although we already optimized table scans to lock the entire table, we may still run into cases
where a single transaction holds locks on a significant portion of a table's pages.

To do anything about this problem, we must first have some idea of how many pages a table uses:
if a table had 1000 pages, and a transaction holds locks on 900 pages, it's a problem, but if a table
had 100000 pages, then a transaction holding 900 page locks is something we can't really optimize.
Begin by modifying methods of `PageDirectory` (in `database/table/PageDirectory.java`) to set the capacity of
the table-level lock contexts appropriately (to the number of data pages that the table has).

Once we have a measure of how many pages a table has, and how many pages of it a transaction holds locks on,
we can implement a simple policy to automatically escalate page-level locks into a table-level lock. You
should modify the codebase to escalate locks from page-level to table-level when both of two conditions are met:

1. The transaction holds at least 20% of the table's pages.
2. The table has at least 10 pages (to avoid locking the entire table unnecessarily when the table is very small).

Auto-escalation should only happen when a transaction needs a new lock on the table's pages, where both of the
above conditions are met *before* the new request is considered. You should escalate the lock first, before making any
lock requests (including requests for intent locks too!) then request the new lock only if the escalated lock is insufficient.

Finally, while auto-escalation is helpful, sometimes, we might want to turn it
off. For example, escalating a lock from the page level to the table level in
the special metadata tables (discussed in detail in Task 7) would bring the
entire database to a standstill. Thus, we also allow for auto-escalation to be
toggled on and off, via the `Table#enableAutoEscalate` and
`Table#disableAutoEscalate` methods, which you will need to implement. A table
with auto-escalation disabled should _never_ auto-escalate.

This task is more open-ended than the other tasks. You may modify any class with a proj4 todo in it (including
classes from Part 1 and 2) by either modifying existing methods or adding new public or private methods. Your code must still pass Part 1 and 2 tests,
so it is recommended that you backup your implementations of Part 1 and 2 if you opt to modify those classes.

Hint: when are we automatically escalating locks? Think about where you can place the auto-escalation code to ensure
that we always escalate when we need to.

A full list of files you can modify: `database/concurrency/LockType.java`, `database/concurrency/LockManager.java`,
`database/concurrency/LockContext.java`, `database/concurrency/LockUtil.java`, `database/index/BPlusTree.java`,
`database/memory/Page.java`, `database/table/PageDirectory.java`, `database/table/Table.java`, `database/Database.java`.

You should pass `testPageDirectoryCapacityLoad`, `testAutoEscalateS`, `testAutoEscalateX`,
`testAutoEscalateSIX`, and `testAutoEscalateDisabled` after
implementing this task.

#### Task 7: Transactional DDL

So far, we've been focusing on locking DML operations.
We'll now focus on locking DDL operations. Our database supports transactional DDL, which is to say,
we *allow* operations like table creation to be done during a transaction.

DML and DDL are terms you might remember from the first few lectures. DML is the Data Manipulation Language, which
encompasses operations that play with the data itself (e.g. inserts, queries), whereas DDL is the Data Definition
Language, which encompasses operations that touch the *structure* of the data (e.g. table creation, changes to the
schema).

In our database, we maintain two special metadata tables for storing metadata about
tables and indices: `information_schema.tables` and
`information_schema.indices`. These tables reside in partitions 1 and
2 respectively (partition 0 contains the write-ahead log, which is not relevant
for this project, and is the topic of the next project), have records the size
of a page, and are accessible from within `Database`, `TransactionContextImpl`,
and `TransactionImpl` as `tableInfo` and `indexInfo`.

For faster lookup, we also maintain two in-memory hash tables: `tableInfoLookup`
and `indexInfoLookup`, which maps table (index) names to the record ID of the
row of metadata in the two tables.

DDL operations are notably different from DML operations in that they affect
metadata. To ensure that multiple transactions do not modify the metadata of
a table or index at the same time, we require that the row in the specific
metadata table be locked whenever a DDL operation is underway.

Supporting transaction DDL also requires us to modify locking for DML
operations: we definitely do not want a table or index to be dropped while
another transaction is using it, so DML operations should make sure to acquire
a lock on the metadata row for a table or index before using it.

Finally, to maintain the property of isolation, we also need to make sure that
once a transaction has checked and realized that a table or index does _not_
exist, no other transaction can create the table/index until after the first
transaction finishes. To accomplish this, we need there to be a row in the metadata tables
whenever we check if a table/index exists, even if it doesn't. The row
creation logic is provided for you in `Database#lockTableMetadata` and
`Database#lockIndexMetadata`, but you will need to add locking logic to them.

You will need to modify the following methods for this task:
- `Database#lockTableMetadata` and `Database#lockIndexMetadata` should acquire
  the appropriate locks on the metadata tables (hint: every row in the metadata
  tables takes up a full page, so you can just lock the page the row is on to
  lock the row).
- `TransactionContextImpl#getTable` and
  `TransactionContextImpl#resolveIndexMetadataFromName` should ensure that the
  correct locks on rows in the metadata table are acquired (the previous couple
  of methods may be useful).
- `TransactionImpl#createTable`, `TransactionImpl#dropTable`,
  `TransactionImpl#createIndex`, `TransactionImpl#dropIndex`, and
  `TransactionImpl#dropAllTables` are the DDL
  operations, and need to be modified to add locking. Note that although
  `createTable` and `createIndex` request locks on the entire table/tree via the
  `Table` and `BPlusTree` constuctors, you will need to explicitly acquire
  similar locks in `dropTable` and `dropIndex`.

A method that might be useful for this task is: `Database#getIndexContext`.

You should modify `database/Database.java` for this task, and should pass
`testLockTableMetadata`, `testLockIndexMetadata`, `testTableMetadataLockOnUse`,
`testCreateTableSimple`, `testCreateIndexSimple`,
`testDropTableSimple`, `testDropIndexSimple`, and `testDropAllTables` after
implementing this task.

#### Additional Notes

At this point, you should pass all the test cases in `database.TestDatabaseLocking`
and `database.TestDatabaseDeadlockPrecheck`.

Note that you may **not** modify the signature of any methods or classes that we
provide to you, but you're free to add helper methods.

You should make sure that all code you modify belongs to files with Project 4 todo comments in them
(e.g. don't add helper methods to PNLJOperator). A full list of files that you may modify follows:

- concurrency/LockType.java
- concurrency/LockManager.java
- concurrency/LockContext.java
- concurrency/LockUtil.java
- index/BPlusTree.java
- memory/Page.java
- table/PageDirectory.java
- table/Table.java
- Database.java

Congratulations! You have finished Project 4!

