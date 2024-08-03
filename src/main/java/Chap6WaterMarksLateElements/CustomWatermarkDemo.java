package Chap6WaterMarksLateElements;

import java.sql.Timestamp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class CustomWatermarkDemo {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream < String > data = env.socketTextStream("localhost", 9090);

        WatermarkStrategy < Tuple2 < Long, String >> ws = (ctx -> new DemoWatermark());

        DataStream < Tuple2 < Long, String >> sum = data.map(new MapFunction < String, Tuple2 < Long, String >> () {
                    public Tuple2 < Long, String > map(String s) {
                        String[] words = s.split(",");
                        return new Tuple2 < Long, String > (Long.parseLong(words[0]), words[1]);
                    }
                })

                .assignTimestampsAndWatermarks(ws)

                .windowAll(EventTimeSessionWindows.withGap(Time.seconds(10)))

                .reduce(new ReduceFunction < Tuple2 < Long, String >> () {
                    public Tuple2 < Long, String > reduce(Tuple2 < Long, String > t1, Tuple2 < Long, String > t2) {
                        int num1 = Integer.parseInt(t1.f1);
                        int num2 = Integer.parseInt(t2.f1);
                        int sum = num1 + num2;
                        Timestamp t = new Timestamp(System.currentTimeMillis());
                        return new Tuple2 < Long, String > (t.getTime(), "" + sum);
                    }
                });
        sum.addSink(StreamingFileSink
                .forRowFormat(new Path("/home/jivesh/window"),
                        new SimpleStringEncoder < Tuple2 < Long, String >> ("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        // execute program
        env.execute("Window");
    }

    public static class DemoWatermark implements WatermarkGenerator < Tuple2 < Long, String >> {
        private final long allowedlatetime = 3500; // 3.5 seconds

        private long currentMaxTimestamp = 0;

        public void onEvent(Tuple2 < Long, String > element, long eventTimestamp, WatermarkOutput output) {
            currentMaxTimestamp = Math.max(eventTimestamp, currentMaxTimestamp);
        }

        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(currentMaxTimestamp - allowedlatetime));
        }
    }
}