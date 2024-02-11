package sumin.example.window.sliding;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sumin.example.window.Event;
import sumin.example.window.EventSourceGenerator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.TimeZone;

public class SlidingEventTimeWindow {
    public StreamExecutionEnvironment env;
    public SlidingEventTimeWindow(StreamExecutionEnvironment env) {
        this.env = env;
    }
    public JobExecutionResult execute() throws Exception {
        // Timezone setting
        SimpleDateFormat setTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS Z");
        setTime.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));

        // Streaming Source with Source Function
        DataStreamSource<Event> source = env.addSource(new EventSourceGenerator());

        // Sliding Event Time Window
        source.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> {
                                    try {
                                        return event.getTime();
                                    } catch (ParseException e) {
                                        throw new RuntimeException(e);
                                    }
                                }))
                .keyBy(event -> event.eventID)
                .window(SlidingEventTimeWindows.of(Time.seconds(20),Time.seconds(5)))
                .process(new EventProcessWindowFunction())
                .print();
        return env.execute("SlidingEventTimeWindow Job");
    }
}
