package edu.berkeley.cs186.database.table;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;

/** A Record is just list of DataBoxes. */
public class Record {
    private List<DataBox> values;

    public Record(List<DataBox> values) {
        this.values = values;
    }

    public List<DataBox> getValues() {
        return this.values;
    }

    public byte[] toBytes(Schema schema) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(schema.getSizeInBytes());
        for (DataBox value : values) {
            byteBuffer.put(value.toBytes());
        }
        return byteBuffer.array();
    }

    /**
     * Takes a byte[] and decodes it into a Record. This method assumes that the
     * input byte[] represents a record that corresponds to this schema.
     *
     * @param buf the byte array to decode
     * @param schema the schema used for this record
     * @return the decoded Record
     */
    public static Record fromBytes(Buffer buf, Schema schema) {
        List<DataBox> values = new ArrayList<>();
        for (Type t : schema.getFieldTypes()) {
            values.add(DataBox.fromBytes(buf, t));
        }
        return new Record(values);
    }

    @Override
    public String toString() {
        return values.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Record)) {
            return false;
        }
        Record r = (Record) o;
        return values.equals(r.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}
