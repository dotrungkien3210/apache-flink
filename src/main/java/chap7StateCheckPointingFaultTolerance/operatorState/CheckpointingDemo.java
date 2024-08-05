package chap7StateCheckPointingFaultTolerance.operatorState;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class CheckpointingDemo {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Mặc định thì checkpoint bị disable, ta phải enable nó
        // start a checkpoint every 1000 ms
        // Thời gian checkpoint quyết định sau bao lâu thì cần một checkpoint
        // đóng vai trò quan trọng khi Flink phải quyết định khi nào cần chèn rào cản (barrier) vào luồng dữ liệu.
        env.enableCheckpointing(1000);

        // to set minimum progress time to happen between checkpoints
        // Ta biết bằng sau khi lấy snapshot, nó sẽ lưu xuống state backend
        // Đối với large state, sẽ mất nhiều thời gian hơn để hoàn thành.
        // Như vậy hệ thống lúc nào cũng sẽ trong trạng thái bận và sẽ không có nhiều thời gian xử lý
        // dòng lệnh này yêu cầu chương trình chỉ thực hiện processing trong 500ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within 10000 ms, or are discarded
        // Thời gian giới hạn để checkpoint hoàn thành, hoặc nó sẽ bị loại bỏ
        env.getCheckpointConfig().setCheckpointTimeout(10000);

        // Mức độ đảm bảo (guarantee level) gồm có "exactly-once" and "at-least-once".
        // exactly-once: tất cả element xử lý bởi operator chỉ 1 lần và không nhiều hơn
        // at-least-once: thì xử lý nhiều hơn 1 lần cho các trường hợp liên quan tới độ trễ
        // exactly-once có thể tốn nhiều thời gian hơn nên không dùng khi độ trễ cao
        // Vậy trong trường hợp cần low latency, ta sử dụng at-least one
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // AT_LEAST_ONCE

        // Cho phép số lượng checkpoint đồng thời
        // Theo mặc định thì system sẽ không trigger checkpoint khác khi mà có checkpoint đang chạy
        // tuy nhiên vẫn có thể cho phép nhiều checkpoint ghi đè trong một số trường hợp
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Theo mặc định thì checkpoint sẽ bị xóa đi khi mà job bị hủy
        // tùy chọn này có thể giúp giữ checkpoint ngay cả khi job bị hủy
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // DELETE_ON_CANCELLATION

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));
        // number of restart attempts , delay in each restart

        DataStream < String > data = env.socketTextStream("localhost", 9090);

        DataStream < Long > sum = data.map(new MapFunction < String, Tuple2 < Long, String >> () {
                    public Tuple2 < Long, String > map(String s) {
                        String[] words = s.split(",");
                        return new Tuple2 < Long, String > (Long.parseLong(words[0]), words[1]);
                    }
                })
                .keyBy(t -> t.f0)
                .flatMap(new StatefulMap());
        //sum.writeAsText("/home/jivesh/state2");
        sum.addSink(StreamingFileSink
                .forRowFormat(new Path("/home/jivesh/state2"),
                        new SimpleStringEncoder < Long > ("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        // execute program
        env.execute("State");
    }

    public static class StatefulMap extends RichFlatMapFunction < Tuple2 < Long, String > , Long > {
        private transient ValueState < Long > sum;
        private transient ValueState < Long > count;

        public void flatMap(Tuple2 < Long, String > input, Collector < Long > out) throws Exception {
            Long currCount = 0L;
            Long currSum = 0L;

            if (count.value() != null) {
                currCount = count.value();
            }

            if (sum.value() != null) {
                currSum = sum.value();
            }

            currCount += 1;
            currSum = currSum + Long.parseLong(input.f1);

            count.update(currCount);
            sum.update(currSum);

            if (currCount >= 10) {
                /* emit sum of last 10 elements */
                out.collect(sum.value());
                /* clear value */
                count.clear();
                sum.clear();
            }
        }
        public void open(Configuration conf) {
            ValueStateDescriptor < Long > descriptor = new ValueStateDescriptor < Long > ("sum", TypeInformation.of(new TypeHint < Long > () {}));
            sum = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor < Long > descriptor2 = new ValueStateDescriptor < Long > ("count", TypeInformation.of(new TypeHint < Long > () {}));
            count = getRuntimeContext().getState(descriptor2);
        }
    }
}