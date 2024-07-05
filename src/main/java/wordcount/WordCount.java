package wordcount;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount {
    public static void main(String[] args)
            throws Exception {
        // Lấy môi trường thực thi
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // lấy các parameter
        ParameterTool params = ParameterTool.fromArgs(args);
        // cho phép param ở global
        env.getConfig().setGlobalJobParameters(params);
        // đọc input
        DataSet < String > text = env.readTextFile(params.get("input"));

        // đoạn code này thực hiện filter những keyword bắt đầu từ N [Nam, Nữ Nipum, Noman]
        DataSet < String > filtered = text.filter(new FilterFunction < String > ()
        {
            public boolean filter(String value) {
                return value.startsWith("N");
            }
        });


        // đoạn code này giống thế nhưng gọi hàm Tokenizer tạo ở dưới thay vì gọi trong
        DataSet < Tuple2 < String, Integer >> tokenized = filtered.map(new Tokenizer());



        // hiểu đơn giản tuple này giờ như table ta có thể đếm bằng cách groupBy + hàm aggregation (sum)
        // ở tây ta groupBy cột 0 và sum trên cột 1
        DataSet < Tuple2 < String, Integer >> counts = tokenized.groupBy(new int[] {
                0
        }).sum(1);



        if (params.has("output")) {
            counts.writeAsCsv(params.get("output"), "\n", " ");

            env.execute("WordCount Example");
        }
    }

    public static final class Tokenizer
            implements MapFunction < String, Tuple2 < String, Integer >> {
        public Tuple2 < String,
                Integer > map(String value) {
            return new Tuple2 < String, Integer > (value, Integer.valueOf(1));
        }
    }
}