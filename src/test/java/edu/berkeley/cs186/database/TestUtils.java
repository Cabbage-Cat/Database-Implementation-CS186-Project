package edu.berkeley.cs186.database;

import edu.berkeley.cs186.database.databox.*;
import edu.berkeley.cs186.database.query.QueryPlanException;
import edu.berkeley.cs186.database.query.TestSourceOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestUtils {
    public static Schema createSchemaWithAllTypes() {
        List<String> names = Arrays.asList("bool", "int", "string", "float");
        List<Type> types = Arrays.asList(Type.boolType(), Type.intType(),
                                         Type.stringType(1), Type.floatType());
        return new Schema(names, types);
    }

    public static Schema createSchemaWithAllTypes(String prefix) {
        List<String> names = Arrays.asList(prefix + "bool", prefix + "int", prefix + "string",
                                           prefix + "float");
        List<Type> types = Arrays.asList(Type.boolType(), Type.intType(),
                                         Type.stringType(1), Type.floatType());
        return new Schema(names, types);
    }

    public static Schema createSchemaWithTwoInts() {
        List<Type> dataBoxes = new ArrayList<Type>();
        List<String> fieldNames = new ArrayList<String>();

        dataBoxes.add(Type.intType());
        dataBoxes.add(Type.intType());

        fieldNames.add("int1");
        fieldNames.add("int2");

        return new Schema(fieldNames, dataBoxes);
    }

    public static Schema createSchemaOfBool() {
        List<Type> dataBoxes = new ArrayList<Type>();
        List<String> fieldNames = new ArrayList<String>();

        dataBoxes.add(Type.boolType());

        fieldNames.add("bool");

        return new Schema(fieldNames, dataBoxes);
    }

    public static Schema createSchemaOfString(int len) {
        List<Type> dataBoxes = new ArrayList<Type>();
        List<String> fieldNames = new ArrayList<String>();

        dataBoxes.add(Type.stringType(len));
        fieldNames.add("string");

        return new Schema(fieldNames, dataBoxes);
    }

    public static Schema createSchemaOfIntAndString(int len) {
        List<String> names = Arrays.asList("int", "string");
        List<Type> types = Arrays.asList(Type.intType(),
                                         Type.stringType(len));
        return new Schema(names, types);
    }

    public static Schema createSchemaOfInt() {
        List<String> names = Arrays.asList("int");
        List<Type> types = Arrays.asList(Type.intType());
        return new Schema(names, types);
    }

    public static Record createRecordWithInt(int val) {
        List<DataBox> dataValues = new ArrayList<DataBox>();
        dataValues.add(new IntDataBox(val));
        return new Record(dataValues);
    }

    public static Record createRecordWithAllTypes() {
        List<DataBox> dataValues = new ArrayList<DataBox>();
        dataValues.add(new BoolDataBox(true));
        dataValues.add(new IntDataBox(1));
        dataValues.add(new StringDataBox("a", 1));
        dataValues.add(new FloatDataBox((float) 1.2));

        return new Record(dataValues);
    }

    public static Record createRecordWithAllTypesWithValue(int val) {
        List<DataBox> dataValues = new ArrayList<DataBox>();
        dataValues.add(new BoolDataBox(true));
        dataValues.add(new IntDataBox(val));
        dataValues.add(new StringDataBox("" + (char) (val % 79 + 0x30), 1));
        dataValues.add(new FloatDataBox((float) val));
        return new Record(dataValues);
    }

    public static Record createRecordWithIntAndStringWithValue(int val, String s, int len) {
        List<DataBox> dataValues = new ArrayList<DataBox>();
        dataValues.add(new IntDataBox(val));
        dataValues.add(new StringDataBox(s, len));
        return new Record(dataValues);
    }

    public static TestSourceOperator createTestSourceOperatorWithInts(List<Integer> values) {
        List<String> columnNames = new ArrayList<String>();
        columnNames.add("int");
        List<Type> columnTypes = new ArrayList<Type>();
        columnTypes.add(Type.intType());
        Schema schema = new Schema(columnNames, columnTypes);

        List<Record> recordList = new ArrayList<Record>();

        for (int v : values) {
            List<DataBox> recordValues = new ArrayList<DataBox>();
            recordValues.add(new IntDataBox(v));
            recordList.add(new Record(recordValues));
        }

        return new TestSourceOperator(recordList, schema);
    }

    public static TestSourceOperator createTestSourceOperatorWithFloats(List<Float> values) {
        List<String> columnNames = new ArrayList<String>();
        columnNames.add("float");
        List<Type> columnTypes = new ArrayList<Type>();
        columnTypes.add(Type.floatType());
        Schema schema = new Schema(columnNames, columnTypes);

        List<Record> recordList = new ArrayList<Record>();

        for (float v : values) {
            List<DataBox> recordValues = new ArrayList<DataBox>();
            recordValues.add(new FloatDataBox(v));
            recordList.add(new Record(recordValues));
        }

        return new TestSourceOperator(recordList, schema);
    }
}
