package sumin.example.window.tumbling;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.TimeZone;

public class TumblingEventTimeWindow {
    public StreamExecutionEnvironment env;
    public TumblingEventTimeWindow(StreamExecutionEnvironment env) {
        this.env = env;
    }
    public JobExecutionResult execute() throws Exception {
        // Timezone setting
        SimpleDateFormat setTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS Z");
        setTime.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));

        // Streaming Source
        DataStreamSource<Event> source = env.fromElements(new Event(1L,"First", setTime.format(new Date()), "First message"),
                new Event(2L,"Second", setTime.format(new Date()), "Second message"),
                new Event(3L,"Third", setTime.format(new Date()), "Third message"));

        // Tumbling Event Time Window
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
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new EventProcessWindowFunction())
                .print();
        return env.execute("TumblingEventTimeWindow Job");
    }
}
