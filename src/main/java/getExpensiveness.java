import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class getExpensiveness extends UDF {
    private Text result = new Text();

    public Text evaluate(Long price) {
        if (price == null) {
            return null;
        }
        if (price < 1000) {
            result.set("Cheap");
        } else if (price < 2000) {
            result.set("Medium");
        } else {
            result.set("Expensive");
        }
        return result;
    }
}
