package sumin.example.window;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class EventSourceGenerator implements SourceFunction<Event> {
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        SimpleDateFormat setTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS Z");
        setTime.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
        while (true) {
            sourceContext.collect(new Event(1L,"First", setTime.format(System.currentTimeMillis()), "First message"));
            sourceContext.collect(new Event(2L,"Second", setTime.format(System.currentTimeMillis()), "Second message"));
            sourceContext.collect(new Event(3L,"Third", setTime.format(System.currentTimeMillis()), "Third message"));
            Thread.sleep(1000*5);
        }
    }

    @Override
    public void cancel() {

    }
}
