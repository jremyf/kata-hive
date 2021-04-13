import java.util.*;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ExplodeNumberUDF extends GenericUDTF {


    private PrimitiveObjectInspector intIO = null;

    //Defining input argument as string.
    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("ExplodeNumberUDF() takes exactly one argument");
        }

        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
            throw new UDFArgumentException("NameParserGenericUDTF() takes a int as a parameter");
        }

        // input
        intIO = (PrimitiveObjectInspector) args[0];

        // output
        List<String> fieldNames = new ArrayList<String>(4);
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(4);
        fieldNames.add("thousands");
        fieldNames.add("hundreds");
        fieldNames.add("tens");
        fieldNames.add("ones");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    public ArrayList<Object[]> processInputRecord(int number){


        ArrayList<Object[]> result = new ArrayList<Object[]>();

        int thousands = number%10000/1000;
        int hundreds = number%1000/100;
        int tens = number%100/10;
        int ones = number%10;

        result.add(new Object[] { thousands, hundreds, tens, ones });
        return result;
    }

    @Override
    public void process(Object[] record) throws HiveException {
        final int name = (int) intIO.getPrimitiveJavaObject(record[0]);
        ArrayList<Object[]> results = processInputRecord(name);
        for (Object[] r : results) {
            forward(r);
        }
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}