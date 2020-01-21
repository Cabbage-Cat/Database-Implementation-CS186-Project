package edu.berkeley.cs186.database.databox;

import edu.berkeley.cs186.database.common.Buffer;

import java.nio.charset.Charset;

/**
 * A DataBox is an element of one of the primitive types specified in
 * Type.java. You can create
 *
 *   - booleans with new BoolDataBox(b),
 *   - integers with new IntDataBox(i),
 *   - floats with new FloatDataBox(f),
 *   - strings with new StringDataBox(s, n), and
 *   - longs with new LongDataBox(l).
 *
 * You can unwrap a databox by first pattern matching on its type and then
 * using one of getBool, getInt, getFloat, getString, and getLong:
 *
 *   Databox d = DataBox.fromBytes(bytes);
 *   switch (d.type().getTypeId()) {
 *     case BOOL:   { System.out.println(d.getBool()); }
 *     case INT:    { System.out.println(d.getInt()); }
 *     case FLOAT:  { System.out.println(d.getFloat()); }
 *     case STRING: { System.out.println(d.getString()); }
 *     case LONG:   { System.out.println(d.getLong()); }
 *   }
 */
public abstract class DataBox implements Comparable<DataBox> {
    public abstract Type type();

    public boolean getBool() {
        throw new DataBoxException("not boolean type");
    }

    public int getInt() {
        throw new DataBoxException("not int type");
    }

    public float getFloat() {
        throw new DataBoxException("not float type");
    }

    public String getString() {
        throw new DataBoxException("not String type");
    }

    public long getLong() {
        throw new DataBoxException("not Long type");
    }

    // Databoxes are serialized as follows:
    //
    //   - BoolDataBoxes are serialized to a single byte that is 0 if the
    //     BoolDataBox is false and 1 if the Databox is true.
    //   - An IntDataBox and a FloatDataBox are serialized to their 4-byte
    //     values (e.g. using ByteBuffer::putInt or ByteBuffer::putFloat).
    //   - The first byte of a serialized m-byte StringDataBox is the 4-byte
    //     number m. Then come the m bytes of the string.
    //
    // Note that when DataBoxes are serialized, they do not serialize their type.
    // That is, serialized DataBoxes are not self-descriptive; you need the type
    // of a Databox in order to parse it.
    public abstract byte[] toBytes();

    public static DataBox fromBytes(Buffer buf, Type type) {
        switch (type.getTypeId()) {
        case BOOL: {
            byte b = buf.get();
            assert (b == 0 || b == 1);
            return new BoolDataBox(b == 1);
        }
        case INT: {
            return new IntDataBox(buf.getInt());
        }
        case FLOAT: {
            return new FloatDataBox(buf.getFloat());
        }
        case STRING: {
            byte[] bytes = new byte[type.getSizeInBytes()];
            buf.get(bytes);
            String s = new String(bytes, Charset.forName("UTF-8"));
            return new StringDataBox(s, type.getSizeInBytes());
        }
        case LONG: {
            return new LongDataBox(buf.getLong());
        }
        default: {
            String err = String.format("Unhandled TypeId %s.",
                                       type.getTypeId().toString());
            throw new IllegalArgumentException(err);
        }
        }
    }
}
