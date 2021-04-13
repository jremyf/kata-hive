import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.util.Collections;
import java.util.List;

public class CustomMinUDF extends GenericUDF {

    ListObjectInspector listOI;

    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("Only 1 argument: List<Int>");
        }
        ObjectInspector a = arguments[0];
        if (!(a instanceof ListObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list");
        }
        this.listOI = (ListObjectInspector) a;
        if (!(listOI.getListElementObjectInspector() instanceof IntObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list of int");
        }
        return listOI.getListElementObjectInspector();
    }

    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        List<IntWritable> list = (List<IntWritable>) this.listOI.getList(arguments[0].get());
        if (list == null) {
            return new IntWritable(0);
        }
        return Collections.min(list);
    }

    @Override
    public String getDisplayString(String[] args) {
        return "get min from a list";
    }
}