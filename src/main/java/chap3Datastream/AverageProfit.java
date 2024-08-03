package chap3datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageProfit {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream < String > data = env.readTextFile("input/avg.txt");

        // month, product, category, profit, count
        DataStream < Tuple5 < String, String, String, Integer, Integer >> mapped =
                data.map(new Splitter()); // tuple  [June,Category5,Bat,12,1]  [June,Category4,Perfume,10,1]



        // groupBy 'month'
        DataStream < Tuple5 < String, String, String, Integer, Integer >> reduced =
                mapped.keyBy(t -> t.f0) // June { [Category5,Bat,12,1] [Category4,Perfume,10,1]}	//rolling reduce
                        .reduce(new Reduce1()); // reduced = { [Category4,Perfume,22,2] ..... }





        // month, avg. profit
        // tính toán lợi nhận trung bình trên tháng, ta lấy lợi nhuận chia cho số tháng
        DataStream < Tuple2 < String, Double >> profitPerMonth =
                reduced.map(new MapFunction < Tuple5 < String, String, String, Integer, Integer > , Tuple2 < String, Double >> () {
                    public Tuple2 < String, Double >

                    map(Tuple5 < String, String, String, Integer, Integer > input)
                    {
                        return new Tuple2 < String, Double > (
                                input.f0,
                                new Double((input.f3 * 1.0) / input.f4));
                    }
                });

        //profitPerMonth.print();
        profitPerMonth.writeAsText("/shared/profit_per_month.txt");

        // execute program
        env.execute("Avg Profit Per Month");
    }

    // *************************************************************************
    // USER FUNCTIONS                                                                                  // pre_result  = Category4,Perfume,22,2
    // *************************************************************************

    /**
     * Hàm reduce này Mang ý nghĩa tổng hợp vì vậy nó chỉ cố gắng thực hiện agg kiểu + - * /
     * Như vậy ta lưu ý rằng input và output ở dây phải cùng datatype
     * Input1 ở đây là bản ghi mới đi vào còn Input2 là tổng hợp của những bản ghi trước đó
     * Output sẽ có datatype giống với input
     * Ngoài ra flink còn hỗ trợ fold operation: output có thể có datatype khác với input
     * Tuy nhiên flink không recommend bạn dùng hàm này, và sẽ có dấu gạch ngang
     */

    public static class Reduce1 implements ReduceFunction < Tuple5 < String, String, String, Integer, Integer >> {
        public Tuple5 < String, String, String, Integer, Integer >

        reduce(Tuple5 < String, String, String, Integer, Integer > current,
               Tuple5 < String, String, String, Integer, Integer > pre_result)
        {
            // Ở đây giá trị 0,1,2 của luồng datastream trước bị bỏ, cộng số lượng sản phẩm bán và số lợi nhuận của
            // sản phẩm trước và sau.
            return new Tuple5 < String, String, String, Integer, Integer >(
                    current.f0,
                    current.f1,
                    current.f2,
                    current.f3 + pre_result.f3,
                    current.f4 + pre_result.f4);
        }
    }

    /**
     * Hàm map ở đây mang ý nghĩa ánh xạ: tức là từ a -> b
     * Ở đây từ một string đọc vào nó ánh xạ từ một String thành 1 tuple 5
     */

    public static class Splitter implements MapFunction < String, Tuple5 < String, String, String, Integer, Integer >> {
        public Tuple5 < String, String, String, Integer, Integer >

        map(String value) // 01-06-2018,June,Category5,Bat,12
        {
            String[] words = value.split(","); // words = [{01-06-2018},{June},{Category5},{Bat}.{12}

            // ignore timestamp, we don't need it for any calculations
            return new Tuple5 < String, String, String, Integer, Integer >
                    (words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
        } //    June    Category5      Bat                      12
    }
}
