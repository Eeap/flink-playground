package sumin.example.window.tumbling;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class EventProcessWindowFunction extends ProcessWindowFunction<Event, String, Long, TimeWindow> {
    ValueState<Integer> count;
    @Override
    public void process(Long aLong, ProcessWindowFunction<Event, String, Long, TimeWindow>.Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Integer.class));
        if (count.value() == null) {
            count.update(0);
        }
        count.update(count.value() + 1);
        for (Event event : iterable) {
            collector.collect("Window: " + context.window() +
                    " Count: " + count.value()+
                    " key: "+aLong+"\n"+
                    "event: "+event.toString());
        }
    }
}
