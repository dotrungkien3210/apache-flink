package chap7StateCheckPointingFaultTolerance.broadcast;

import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction.Context;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction.ReadOnlyContext;
import org.apache.flink.util.Collector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class EmpCountBroadcastDemo {
    // Broadcast state descriptor được tạo từ map state descriptor.
    // Tham số truyền vào gồm type của key và type của state
    public static final MapStateDescriptor < String, String > excludeEmpDescriptor =
            new MapStateDescriptor < String, String > ("exclude_employ", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Đọc file và truyền vào luồng stream
        DataStream < String > excludeEmp = env.socketTextStream("localhost", 9090);
        // sau khi luồng streaming được đọc dữ liệu sẽ được broadcast thông qua hàm
        BroadcastStream < String > excludeEmpBroadcast = excludeEmp.broadcast(excludeEmpDescriptor);

        DataStream < Tuple2 < String, Integer >> employees = env.readTextFile("/home/jivesh/broadcast.txt")
                .map(new MapFunction < String, Tuple2 < String, String >> () {
                    public Tuple2 < String, String > map(String value) {
                        // dept, data
                        return new Tuple2 < String, String > (value.split(",")[3], value); // {(Purchase), (AXPM175755,Nana,Developer,Purchase,GH67D)}
                    }
                })
                .keyBy(t -> t.f0)
                .connect(excludeEmpBroadcast) // will return a BroadcastConnectedStream
                .process(new ExcludeEmp());

        employees.addSink(StreamingFileSink
                .forRowFormat(new Path("/home/jivesh/bd_out"),
                        new SimpleStringEncoder < Tuple2 < String, Integer >> ("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        env.execute("Broadcast Exmaple");
    }

    public static class ExcludeEmp extends KeyedBroadcastProcessFunction < String, Tuple2 < String, String > , String, Tuple2 < String, Integer >> {
        private transient ValueState < Integer > countState;

        public void processElement(Tuple2 < String, String > value, ReadOnlyContext ctx, Collector < Tuple2 < String, Integer >> out) throws Exception {
            Integer currCount = 0;

            if (countState != null) {
                currCount = countState.value();
            }

            // get card_id of current transaction
            final String cId = value.f1.split(",")[0];

            for (Map.Entry < String, String > cardEntry: ctx.getBroadcastState(excludeEmpDescriptor).immutableEntries()) {
                final String excludeId = cardEntry.getKey();
                if (cId.equals(excludeId))
                    return;
            }

            countState.update(currCount + 1); // dept    , current sum
            out.collect(new Tuple2 < String, Integer > (value.f0, currCount + 1));
        }

        public void processBroadcastElement(String empData, Context ctx, Collector < Tuple2 < String, Integer >> out) throws Exception {
            String id = empData.split(",")[0];
            ctx.getBroadcastState(excludeEmpDescriptor).put(id, empData);
        }

        public void open(Configuration conf) {
            ValueStateDescriptor < Integer > desc = new ValueStateDescriptor < Integer > ("", BasicTypeInfo.INT_TYPE_INFO);
            countState = getRuntimeContext().getState(desc);
        }
    }
}
