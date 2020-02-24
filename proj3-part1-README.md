# Project 3: Joins and Query Optimization (Part 1)

## Overview

In this part, you will implement some join algorithms - in particular, block
nested loop join (BNLJ), sort merge join, and grace hash join.

Aside from when the comments tell you that you can do something in memory, everything else should be **streamed**. You should not hold more pages in memory at once than the given algorithm says you are allowed to. Doing otherwise may result in no credit.

Remember the test cases we give you are not comprehensive, so you should write your own tests to further test your code and catch edge cases.

Small note on terminology: in lecture, we sometimes use both `block` and `page`
to describe the unit of transfer between memory and disk. In the context of join
algorithms, however, `page` refers to the unit of transfer between memory and
disk, and `block` refers to a set of one or more `page`s. All uses of the word
`block` in this part refer to this second definition (a set of pages).

All files that will need to be edited can be found under the `Query` directory in the codebase.

**Read through the all the docstrings and helper methods as they will be very helpful**

## Simple Nested Loop Join (SNLJ)

SNLJ has already been implemented for you, in `SNLJOperator`. You should take
a look at it to get a sense for how the pseudocode in lecture and section
translate to code.

You should not, however, copy it when writing your own join operators. Although
each join algorithm should return the same data, the order differs between each
join algorithm, as does the structure of the code. In particular, SNLJ does not
need to explicitly manage pages of data (it only ever needs the next record of
each table, and therefore can just use an iterator over all records in a table),
whereas all the algorithms you will be implementing in this part must explicitly
manage when pages of data are fetched from disk.

## Page Nested Loop Join and Block Nested Loop Join (PNLJ/BNLJ)

PNLJ has already been implemented for you, as a special case of BNLJ, with B=3.  Therefore, it will not function properly until BNLJ has been properly
implemented. The test cases for both PNLJ and BNLJ in `TestJoinOperator` depend
on a properly implemented BNLJ.

You should read through the given skeleton code in `BNLJOperator`. The `next`
and `hasNext` methods of the iterator have already been filled out for you, but
you will need to implement the `fetchNextRecord` method, which should do most of
the heavy lifting of the BNLJ algorithm. The `fetchNextRecord` method should, as its
name suggests, fetches the next record of the join output.

There are also two suggested helper methods: `fetchNextLeftBlock`, which should
fetch the next non-empty block of left table pages from `leftIterator`, and
`fetchNextRightPage`, which should fetch the next non-empty page of the right
table (from `rightIterator`).

We suggest breaking up the problem into smaller subproblems, and adding more
helper methods than the two suggested ones -- it will make debugging your code
much easier.

Once you have implemented `BNLJOperator`, all the PNLJ and BNLJ tests in
`TestJoinOperator` should pass.

**Note: We recommend you avoid using a `do-while` loop when implenting BNLJ as it has caused a lot of headache
for quite a few students in the past. Use a `while` loop instead.**

## External Sort

The first step in Sort Merge Join is to sort both input relations. Therefore,
before you can work on implementing Sort Merge Join, you must first implement an
external sorting algorithm.

Recall that a "run" in the context of external mergesort is just a sequence of
sorted records. This is represented in `SortOperator` by the `Run` inner class.
As runs in external mergesort can span many pages (and eventually span the
entirety of the table), the `Run` class does not keep all its data in memory.
Rather, it creates a temporary table and writes all of its data to the temporary
table (which is materialized to disk at the buffer manager's discretion).

You will need to implement the `sortRun`, `mergeSortedRuns`, `mergePass`, and
`sort` methods of `SortOperator`.

`sortRun(run)` should sort the passed in data using an in-memory sort (Pass 0 of
external mergesort).

`mergeSortedRuns(runs)` should return a new run given a list of sorted runs.

`mergePass(runs)` should perform a single merge pass of external mergesort,
given a list of all the sorted runs from the previous pass.

`sort()` should run external mergesort from start to finish, and return the name
of the temporary table with the sorted data.

Each of these methods may be tested independently, so you **must** implement
each one as described. You may add additional helper methods as you see fit.

Once you have implemented all four methods, all the tests in `TestSortOperator`
should pass.

## Sort Merge Join (SMJ)

Now that you have a working external sort, you can now implement SMJ.

For simplicity, your implementation of SMJ should *not* utilize the optimization
discussed in lecture in any case (where the final merge pass of sorting happens
at the same time as the join). Therefore, you should use `SortOperator` to sort
during the sort phase of SMJ.

You will need to implement the `SortMergeIterator` inner class of
`SortMergeOperator`.

Your implementation in `SortMergeOperator` and your implementation of
`SortOperator` may be tested independently. You **must not** use any method of
`SortOperator` in `SortMergeOperator`, aside from the public methods given in
the skeleton (in other words: don't add a new public method to `SortOperator`
and call it from `SortMergeOperator`).

Once you have implemented `SortMergeIterator`, all the remaining tests in
`TestJoinOperator` should pass.

## Grace Hash Join (GHJ)

In order to help you implement GHJ, we provided an implementation of Naive Hash Join (NHJ) which can be found in `NaiveHashJoin.java`. Read the code for NHJ carefully as GHJ will require a few modifications and will build ontop of NHJ.

Everything you will need to implement will be done in `GraceHashJoin.java`. You will need to implement the functions `partition`, `buildAndProbe`, `createPartitions`, and `run`. Additionally, you will have to provide some inputs in `getBreakNHJInputs` and `getBreakGHJInputs` which will be used to test that Naive Hash Join fails but Grace Hash Join passes (Tested in `testBreakNHJButPassGHJ`) and that GHJ breaks (tested in `testGHJBreak`) respectively.

Before coding, read through all the documentation and skeleton code to get a good understanding of the logic. 
Additionally, the file `HashPartition.java` in the `memory` directory will be useful when working with partitions. 
Read through the file and get a good idea what methods you can use.

Once you have implemented all the methods in `GraceHashJoin.java`, all tests in `TestGraceHashJoin.java` will pass.

There will be **no** hidden tests for Grace Hash Join. Your grade for Grace Hash Join will come from the released public tests. 

## Additional Notes

After this, you should pass all the tests we have provided to you in `database.query.TestJoinOperator`, `database.query.TestSortOperator`, and `database.query.TestGraceHashJoin`.

Note that you may **not** modify the signature of any methods or classes that we
provide to you, but you're free to add helper methods. Also, you should only modify
`BNLJOperator.java`, `SortOperator.java`, `SortMergeOperator.java`, and `GraceHashJoin.java` in this part.

