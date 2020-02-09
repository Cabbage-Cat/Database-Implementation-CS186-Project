package edu.berkeley.cs186.database.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Arrays;

import edu.berkeley.cs186.database.categories.*;
import org.junit.Test;

import edu.berkeley.cs186.database.common.ByteBuffer;
import edu.berkeley.cs186.database.databox.BoolDataBox;
import edu.berkeley.cs186.database.databox.FloatDataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.StringDataBox;
import edu.berkeley.cs186.database.databox.Type;
import org.junit.experimental.categories.Category;

@Category({Proj99Tests.class, SystemTests.class})
public class TestRecord {
    @Test
    public void testToAndFromBytes() {
        Schema[] schemas = {
            new Schema(Arrays.asList("x"), Arrays.asList(Type.boolType())),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.intType())),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.floatType())),
            new Schema(Arrays.asList("x"), Arrays.asList(Type.stringType(3))),
            new Schema(Arrays.asList("w", "x", "y", "z"),
                       Arrays.asList(Type.boolType(), Type.intType(),
                                     Type.floatType(), Type.stringType(3))),
        };

        Record[] records = {
            new Record(Arrays.asList(new BoolDataBox(false))),
            new Record(Arrays.asList(new IntDataBox(0))),
            new Record(Arrays.asList(new FloatDataBox(0f))),
            new Record(Arrays.asList(new StringDataBox("foo", 3))),
            new Record(Arrays.asList(
                           new BoolDataBox(false),
                           new IntDataBox(0),
                           new FloatDataBox(0f),
                           new StringDataBox("foo", 3)
                       ))
        };

        assert(schemas.length == records.length);
        for (int i = 0; i < schemas.length; ++i) {
            Schema s = schemas[i];
            Record r = records[i];
            assertEquals(r, Record.fromBytes(ByteBuffer.wrap(r.toBytes(s)), s));
        }
    }

    @Test
    public void testEquals() {
        Record a = new Record(Arrays.asList(new BoolDataBox(false)));
        Record b = new Record(Arrays.asList(new BoolDataBox(true)));
        Record c = new Record(Arrays.asList(new IntDataBox(0)));

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
