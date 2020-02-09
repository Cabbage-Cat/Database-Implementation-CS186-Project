package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.concurrency.DummyLockManager;
import edu.berkeley.cs186.database.databox.*;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.query.QueryPlan;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import static org.junit.Assert.*;

@Category({Proj99Tests.class})
public class TestDatabaseQueries {
    private Database database;
    private Transaction transaction;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        File tempDir = tempFolder.newFolder("myDb", "school");
        database = new Database(tempDir.getAbsolutePath(), 32, new DummyLockManager());
        database.setWorkMem(5); // B=5

        createSchemas();
        readTuplesFromFiles();

        transaction = database.beginTransaction();
    }

    @After
    public void teardown() {
        transaction.commit();
        database.close();
    }

    @Test
    @Category(SystemTests.class)
    public void testJoinStudentNamesWithClassNames() {
        QueryPlan queryPlan = this.transaction.query("Students", "S");
        queryPlan.join("Enrollments", "E", "S.sid", "E.sid");
        queryPlan.join("Courses", "C", "E.cid", "C.cid");
        List<String> columns = new ArrayList<>();
        columns.add("S.name");
        columns.add("C.name");
        queryPlan.project(columns);

        Iterator<Record> recordIterator = queryPlan.execute();

        int count = 0;
        while (recordIterator.hasNext()) {
            recordIterator.next();
            count++;
        }

        assertEquals(1000, count);
    }

    private void createSchemas() {
        List<String> studentSchemaNames = new ArrayList<>();
        studentSchemaNames.add("sid");
        studentSchemaNames.add("name");
        studentSchemaNames.add("major");
        studentSchemaNames.add("gpa");

        List<Type> studentSchemaTypes = new ArrayList<>();
        studentSchemaTypes.add(Type.intType());
        studentSchemaTypes.add(Type.stringType(20));
        studentSchemaTypes.add(Type.stringType(20));
        studentSchemaTypes.add(Type.floatType());

        Schema studentSchema = new Schema(studentSchemaNames, studentSchemaTypes);

        try(Transaction t = database.beginTransaction()) {
            t.createTable(studentSchema, "Students");

            List<String> courseSchemaNames = new ArrayList<>();
            courseSchemaNames.add("cid");
            courseSchemaNames.add("name");
            courseSchemaNames.add("department");

            List<Type> courseSchemaTypes = new ArrayList<>();
            courseSchemaTypes.add(Type.intType());
            courseSchemaTypes.add(Type.stringType(20));
            courseSchemaTypes.add(Type.stringType(20));

            Schema courseSchema = new Schema(courseSchemaNames, courseSchemaTypes);

            t.createTable(courseSchema, "Courses");

            List<String> enrollmentSchemaNames = new ArrayList<>();
            enrollmentSchemaNames.add("sid");
            enrollmentSchemaNames.add("cid");

            List<Type> enrollmentSchemaTypes = new ArrayList<>();
            enrollmentSchemaTypes.add(Type.intType());
            enrollmentSchemaTypes.add(Type.intType());

            Schema enrollmentSchema = new Schema(enrollmentSchemaNames, enrollmentSchemaTypes);

            t.createTable(enrollmentSchema, "Enrollments");
        }
    }

    private void readTuplesFromFiles() throws IOException {
        try(Transaction transaction = database.beginTransaction()) {
            // read student tuples
            List<String> studentLines = Files.readAllLines(Paths.get("students.csv"), Charset.defaultCharset());

            for (String line : studentLines) {
                String[] splits = line.split(",");
                List<DataBox> values = new ArrayList<>();

                values.add(new IntDataBox(Integer.parseInt(splits[0])));
                values.add(new StringDataBox(splits[1].trim(), 20));
                values.add(new StringDataBox(splits[2].trim(), 20));
                values.add(new FloatDataBox(Float.parseFloat(splits[3])));

                transaction.insert("Students", values);
            }

            List<String> courseLines = Files.readAllLines(Paths.get("courses.csv"), Charset.defaultCharset());

            for (String line : courseLines) {
                String[] splits = line.split(",");
                List<DataBox> values = new ArrayList<>();

                values.add(new IntDataBox(Integer.parseInt(splits[0])));
                values.add(new StringDataBox(splits[1].trim(), 20));
                values.add(new StringDataBox(splits[2].trim(), 20));

                transaction.insert("Courses", values);
            }

            List<String> enrollmentLines = Files.readAllLines(Paths.get("enrollments.csv"),
                                           Charset.defaultCharset());

            for (String line : enrollmentLines) {
                String[] splits = line.split(",");
                List<DataBox> values = new ArrayList<>();

                values.add(new IntDataBox(Integer.parseInt(splits[0])));
                values.add(new IntDataBox(Integer.parseInt(splits[1])));

                transaction.insert("Enrollments", values);
            }
        }
    }
}
