package chap7StateCheckPointingFaultTolerance.operatorState;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class ManagedOperatorStateDemo implements SinkFunction < Tuple2 < String, Integer >> , CheckpointedFunction {
    private final int threshold;
    private transient ListState < Tuple2 < String, Integer >> checkpointedState;
    // We will use this list to hold elements before putting them into state.
    private List < Tuple2 < String, Integer >> bufferedElements;
    // constructor of state demo class for initialization of threshold variable and buffered element list.
    public ManagedOperatorStateDemo(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList < Tuple2 < String, Integer >> ();
    }

    /**
     *  What we want to write as output. You can attach any sync function to it, like writeAsText.
     *  This invoke method is called for every input record, and processes it according to logic provided in it.
     */
    public void invoke(Tuple2 < String, Integer > value, Context context) throws Exception {
        // add tất cả record vào Java buffered elements list.
        bufferedElements.add(value);
        // nếu size của buffer bằng với threshold thì tất cả element sẽ write vào sync function
        if (bufferedElements.size() == threshold) {
            for (Tuple2 < String, Integer > element: bufferedElements) {
                // send it to the sink
            }
            bufferedElements.clear();
        }
    }

    /**
     *
     * Nó nhận 1 tham số truyền vào FunctionSnapshotContext
     *  Các hàm của người dùng sẽ sử dụng manage state này, có thể tham gia snapshot
     *  Ta sẽ adding elements vào trong ListState,
     */

    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // đầu tiên là clear tất cả data có trong checkpointedState trước đó nếu có
        checkpointedState.clear();
        // tiếp theo adding all elements từ bufferedElements list vào trong state.
        for (Tuple2 < String, Integer > element: bufferedElements) {
            checkpointedState.add(element);
        }
    }

    /**
     * Cung cấp một ngữ cảnh để cho các hàm của user có thể khởi tạo bằng cách registering vào managed state.
     * Khi khởi tạo, state này sẽ báo nếu như stae empty hoặc được khôi phục từ lần chạy trước đó, giá trị mặc định
     */
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // State được tạo sẽ được mô tả tại đây bao gồm name của state và datatype của element mà state lưu trữ
        //
        ListStateDescriptor < Tuple2 < String, Integer >> descriptor = new ListStateDescriptor < Tuple2 < String, Integer >> ("buffered-elements",
                TypeInformation.of(new TypeHint < Tuple2 < String, Integer >> () {}));
        // đoạn code để accessing vào state
        // method này cũng mô tả cách mà redistribution of states trong trường hợp re-scaling
        // trên thực tế ta có 2 redistribution schemes
        // even split redistribution and union redistribution.
        checkpointedState = context.getOperatorStateStore().getListState(descriptor); // .getUninonListState(descriptor)
        // cuối cùng là check recover after failure
        // nếu là context.isRestored() có nghĩa là true
        if (context.isRestored()) {
            // ở đây lấy tất cả elements từ saved state,
            //and adding all those elements in the Java list.
            for (Tuple2 < String, Integer > element: checkpointedState.get()) {
                bufferedElements.add(element);

            }
        }
    }
}