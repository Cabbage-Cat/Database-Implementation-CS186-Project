package edu.berkeley.cs186.database.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import edu.berkeley.cs186.database.categories.*;
import org.junit.Test;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.Type;
import org.junit.experimental.categories.Category;

@Category({Proj99Tests.class, SystemTests.class})
public class TestSchema {
    @Test
    public void testSizeInBytes() {
        Schema[] schemas = {
            // Single column.
            new Schema(Arrays.asList("x"), Arrays.asList(Type.boolType())),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.intType())),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.floatType())),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.stringType(1))),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.stringType(10))),

            // Multiple columns.
            new Schema(Arrays.asList("x", "y", "z"),
                       Arrays.asList(Type.boolType(), Type.intType(), Type.floatType())),
            new Schema(Arrays.asList("x", "y"),
                       Arrays.asList(Type.boolType(), Type.stringType(42))),
        };

        int[] expectedSizes = {1, 4, 4, 1, 10, 9, 43};

        assert(schemas.length == expectedSizes.length);
        for (int i = 0; i < schemas.length; ++i) {
            assertEquals(expectedSizes[i], schemas[i].getSizeInBytes());
        }
    }

    @Test
    public void testVerifyValidRecords() {
        try {
            Schema[] schemas = {
                new Schema(Arrays.asList(), Arrays.asList()),
                new Schema(Arrays.asList("x"), Arrays.asList(Type.boolType())),
                new Schema(Arrays.asList("x"), Arrays.asList(Type.intType())),
                new Schema(Arrays.asList("x"), Arrays.asList(Type.floatType())),
                new Schema(Arrays.asList("x"), Arrays.asList(Type.stringType(1))),
                new Schema(Arrays.asList("x"), Arrays.asList(Type.stringType(2))),
            };
            List<List<DataBox>> values = Arrays.asList(
                                             Arrays.asList(),
                                             Arrays.asList(new BoolDataBox(false)),
                                             Arrays.asList(new IntDataBox(0)),
                                             Arrays.asList(new FloatDataBox(0f)),
                                             Arrays.asList(new StringDataBox("a", 1)),
                                             Arrays.asList(new StringDataBox("ab", 2))
                                         );

            assert(schemas.length == values.size());
            for (int i = 0; i < schemas.length; ++i) {
                Schema s = schemas[i];
                List<DataBox> v = values.get(i);
                assertEquals(new Record(v), s.verify(v));
            }
        } catch (DatabaseException e) {
            fail(e.getMessage());
        }
    }

    @Test(expected = DatabaseException.class)
    public void testVerifyWrongSize() {
        Schema schema = new Schema(Arrays.asList("x"), Arrays.asList(Type.boolType()));
        List<DataBox> values = Arrays.asList();
        schema.verify(values);
    }

    @Test(expected = DatabaseException.class)
    public void testVerifyWrongType() {
        Schema schema = new Schema(Arrays.asList("x"), Arrays.asList(Type.boolType()));
        List<DataBox> values = Arrays.asList(new IntDataBox(42));
        schema.verify(values);
    }

    @Test
    public void testToAndFromBytes() {
        Schema[] schemas = {
            // Single column.
            new Schema(Arrays.asList("x"), Arrays.asList(Type.boolType())),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.intType())),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.floatType())),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.stringType(1))),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.stringType(10))),

            // Multiple columns.
            new Schema(Arrays.asList("x", "y", "z"),
                       Arrays.asList(Type.boolType(), Type.intType(), Type.floatType())),
            new Schema(Arrays.asList("x", "y"),
                       Arrays.asList(Type.boolType(), Type.stringType(42))),
        };

        for (Schema schema : schemas) {
            assertEquals(schema, Schema.fromBytes(ByteBuffer.wrap(schema.toBytes())));
        }
    }

    @Test
    public void testEquals() {
        Schema a = new Schema(Arrays.asList("x"), Arrays.asList(Type.intType()));
        Schema b = new Schema(Arrays.asList("y"), Arrays.asList(Type.intType()));
        Schema c = new Schema(Arrays.asList("x"), Arrays.asList(Type.boolType()));

        assertEquals(a, a);
        assertNotEquals(a, b);
        assertNotEquals(a, c);
        assertNotEquals(b, a);
        assertEquals(b, b);
        assertNotEquals(b, c);
        assertNotEquals(c, a);
        assertNotEquals(c, b);
        assertEquals(c, c);
    }
}
