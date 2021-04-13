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


    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        return null;
    }

    @Override
    public String getDisplayString(String[] args) {
        return "get min from a list";
    }
}