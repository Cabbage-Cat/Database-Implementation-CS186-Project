package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.PredicateOperator;
import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

@Category({Proj99Tests.class, SystemTests.class})
public class TestDatabase {
    private static final String TestDir = "testDatabase";
    private Database db;
    private String filename;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.db = new Database(filename, 32);
        this.db.setWorkMem(4);
        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
        }
    }

    @After
    public void afterEach() {
        try(Transaction t = this.db.beginTransaction()) {
            t.dropAllTables();
        }
        this.db.close();
    }

    @Test
    public void testTableCreate() {
        Schema s = TestUtils.createSchemaWithAllTypes();

        try(Transaction t = this.db.beginTransaction()) {
            t.createTable(s, "testTable1");
        }
    }

    @Test
    public void testTransactionBegin() {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        try(Transaction t1 = db.beginTransaction()) {
            t1.createTable(s, tableName);
            RecordId rid = t1.getTransactionContext().addRecord(tableName, input.getValues());
            t1.getTransactionContext().getRecord(tableName, rid);
        }
    }

    @Test
    public void testTransactionTempTable() {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        try(Transaction t1 = db.beginTransaction()) {
            t1.createTable(s, tableName);
            RecordId rid = t1.getTransactionContext().addRecord(tableName, input.getValues());
            Record rec = t1.getTransactionContext().getRecord(tableName, rid);
            assertEquals(input, rec);

            String tempTableName = t1.getTransactionContext().createTempTable(s);
            rid = t1.getTransactionContext().addRecord(tempTableName, input.getValues());
            rec = t1.getTransactionContext().getRecord(tempTableName, rid);
            assertEquals(input, rec);
        }
    }

    @Test(expected = DatabaseException.class)
    public void testTransactionTempTable2() {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        RecordId rid;
        Record rec;
        String tempTableName;
        try(Transaction t1 = db.beginTransaction()) {
            t1.createTable(s, tableName);
            rid = t1.getTransactionContext().addRecord(tableName, input.getValues());
            rec = t1.getTransactionContext().getRecord(tableName, rid);
            assertEquals(input, rec);

            tempTableName = t1.getTransactionContext().createTempTable(s);
            rid = t1.getTransactionContext().addRecord(tempTableName, input.getValues());
            rec = t1.getTransactionContext().getRecord(tempTableName, rid);
            assertEquals(input, rec);
        }

        try(Transaction t2 = db.beginTransaction()) {
            t2.getTransactionContext().addRecord(tempTableName, input.getValues());
        }
    }

    @Test
    public void testDatabaseDurablity() {
        Schema s = TestUtils.createSchemaWithAllTypes();
        Record input = TestUtils.createRecordWithAllTypes();

        String tableName = "testTable1";

        RecordId rid;
        Record rec;
        try(Transaction t1 = db.beginTransaction()) {
            t1.createTable(s, tableName);
            rid = t1.getTransactionContext().addRecord(tableName, input.getValues());
            rec = t1.getTransactionContext().getRecord(tableName, rid);

            assertEquals(input, rec);
        }

        db.close();
        db = new Database(this.filename, 32);

        try(Transaction t1 = db.beginTransaction()) {
            rec = t1.getTransactionContext().getRecord(tableName, rid);
            assertEquals(input, rec);
        }
    }

    @Test
    public void testREADMESample() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema(
                Arrays.asList("id", "firstName", "lastName"),
                Arrays.asList(Type.intType(), Type.stringType(10), Type.stringType(10))
            );
            t1.createTable(s, "table1");
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(1),
                          new StringDataBox("John", 10),
                          new StringDataBox("Doe", 10)
                      ));
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(2),
                          new StringDataBox("Jane", 10),
                          new StringDataBox("Doe", 10)
                      ));
            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            Iterator<Record> iter = t2.query("table1").execute();

            assertEquals(Arrays.asList(
                             new IntDataBox(1),
                             new StringDataBox("John", 10),
                             new StringDataBox("Doe", 10)
                         ), iter.next().getValues());

            assertEquals(Arrays.asList(
                             new IntDataBox(2),
                             new StringDataBox("Jane", 10),
                             new StringDataBox("Doe", 10)
                         ), iter.next().getValues());

            assertFalse(iter.hasNext());

            t2.commit();
        }
    }

    @Test
    public void testJoinQuery() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema(
                Arrays.asList("id", "firstName", "lastName"),
                Arrays.asList(Type.intType(), Type.stringType(10), Type.stringType(10))
            );
            t1.createTable(s, "table1");
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(1),
                          new StringDataBox("John", 10),
                          new StringDataBox("Doe", 10)
                      ));
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(2),
                          new StringDataBox("Jane", 10),
                          new StringDataBox("Doe", 10)
                      ));
            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            // FROM table1 AS t1
            QueryPlan queryPlan = t2.query("table1", "t1");
            // JOIN table1 AS t2 ON t1.lastName = t2.lastName
            queryPlan.join("table1", "t2", "t1.lastName", "t2.lastName");
            // WHERE t1.firstName = 'John'
            queryPlan.select("t1.firstName", PredicateOperator.EQUALS, new StringDataBox("John", 10));
            // .. AND t2.firstName = 'Jane'
            queryPlan.select("t2.firstName", PredicateOperator.EQUALS, new StringDataBox("Jane", 10));
            // SELECT t1.id, t2.id, t1.firstName, t2.firstName, t1.lastName
            queryPlan.project(Arrays.asList("t1.id", "t2.id", "t1.firstName", "t2.firstName", "t1.lastName"));

            // run the query
            Iterator<Record> iter = queryPlan.execute();

            assertEquals(Arrays.asList(
                             new IntDataBox(1),
                             new IntDataBox(2),
                             new StringDataBox("John", 10),
                             new StringDataBox("Jane", 10),
                             new StringDataBox("Doe", 10)
                         ), iter.next().getValues());

            assertFalse(iter.hasNext());

            t2.commit();
        }
    }

    @Test
    public void testAggQuery() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema(
                Arrays.asList("id", "firstName", "lastName"),
                Arrays.asList(Type.intType(), Type.stringType(10), Type.stringType(10))
            );
            t1.createTable(s, "table1");
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(1),
                          new StringDataBox("John", 10),
                          new StringDataBox("Doe", 10)
                      ));
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(2),
                          new StringDataBox("Jane", 10),
                          new StringDataBox("Doe", 10)
                      ));
            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            // FROM table1
            QueryPlan queryPlan = t2.query("table1");
            // SELECT COUNT(*)
            queryPlan.count();
            // .. SUM(id)
            queryPlan.sum("id");
            // .. AVERAGE(id)
            queryPlan.average("id");

            // run the query
            Iterator<Record> iter = queryPlan.execute();

            assertEquals(Arrays.asList(
                             new IntDataBox(2),
                             new IntDataBox(3),
                             new FloatDataBox(1.5f)
                         ), iter.next().getValues());

            assertFalse(iter.hasNext());

            t2.commit();
        }
    }

    @Test
    public void testGroupByQuery() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema(
                Arrays.asList("id", "firstName", "lastName"),
                Arrays.asList(Type.intType(), Type.stringType(10), Type.stringType(10))
            );
            t1.createTable(s, "table1");
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(1),
                          new StringDataBox("John", 10),
                          new StringDataBox("Doe", 10)
                      ));
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(2),
                          new StringDataBox("Jane", 10),
                          new StringDataBox("Doe", 10)
                      ));

            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            // FROM table1
            QueryPlan queryPlan = t2.query("table1");
            // GROUP BY lastName
            queryPlan.groupBy("lastName");
            // SELECT lastName
            queryPlan.project(Collections.singletonList("lastName"));
            // .. COUNT(*)
            queryPlan.count();

            // run the query
            Iterator<Record> iter = queryPlan.execute();

            assertEquals(Arrays.asList(
                             new StringDataBox("Doe", 10),
                             new IntDataBox(2)
                         ), iter.next().getValues());

            assertFalse(iter.hasNext());

            t2.commit();
        }
    }

    @Test
    public void testUpdateQuery() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema(
                Arrays.asList("id", "firstName", "lastName"),
                Arrays.asList(Type.intType(), Type.stringType(10), Type.stringType(10))
            );
            t1.createTable(s, "table1");
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(1),
                          new StringDataBox("John", 10),
                          new StringDataBox("Doe", 10)
                      ));
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(2),
                          new StringDataBox("Jane", 10),
                          new StringDataBox("Doe", 10)
                      ));

            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            // UPDATE table1 SET id = id + 10 WHERE lastName = 'Doe'
            t2.update("table1", "id", (DataBox x) -> new IntDataBox(x.getInt() + 10),
                      "lastName", PredicateOperator.EQUALS, new StringDataBox("Doe", 10));

            Iterator<Record> iter = t2.query("table1").execute();

            assertEquals(Arrays.asList(
                             new IntDataBox(11),
                             new StringDataBox("John", 10),
                             new StringDataBox("Doe", 10)
                         ), iter.next().getValues());

            assertEquals(Arrays.asList(
                             new IntDataBox(12),
                             new StringDataBox("Jane", 10),
                             new StringDataBox("Doe", 10)
                         ), iter.next().getValues());

            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testDeleteQuery() {
        try (Transaction t1 = db.beginTransaction()) {
            Schema s = new Schema(
                Arrays.asList("id", "firstName", "lastName"),
                Arrays.asList(Type.intType(), Type.stringType(10), Type.stringType(10))
            );
            t1.createTable(s, "table1");
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(1),
                          new StringDataBox("John", 10),
                          new StringDataBox("Doe", 10)
                      ));
            t1.insert("table1", Arrays.asList(
                          new IntDataBox(2),
                          new StringDataBox("Jane", 10),
                          new StringDataBox("Doe", 10)
                      ));

            t1.commit();
        }

        try (Transaction t2 = db.beginTransaction()) {
            // DELETE FROM table1 WHERE id <> 2
            t2.delete("table1", "id", PredicateOperator.NOT_EQUALS, new IntDataBox(2));

            Iterator<Record> iter = t2.query("table1").execute();

            assertEquals(Arrays.asList(
                             new IntDataBox(2),
                             new StringDataBox("Jane", 10),
                             new StringDataBox("Doe", 10)
                         ), iter.next().getValues());

            assertFalse(iter.hasNext());
        }
    }
}
