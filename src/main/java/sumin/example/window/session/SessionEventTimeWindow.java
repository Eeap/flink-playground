package sumin.example.window.session;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sumin.example.window.Event;
import sumin.example.window.EventSchema;
import sumin.example.window.EventSourceGenerator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.TimeZone;

public class SessionEventTimeWindow {
    public StreamExecutionEnvironment env;
    public SessionEventTimeWindow(StreamExecutionEnvironment env) {
        this.env = env;
    }
    public JobExecutionResult execute() throws Exception {
        // Timezone setting
        SimpleDateFormat setTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS Z");
        setTime.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));

        // Kafka Source
        KafkaSource<Event> kafkaSource = KafkaSource.<Event>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("example")
                .setGroupId("example-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new EventSchema()))
                .build();

        // watermark strategy
        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        return event.getTime();
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                });

        // Streaming Source with Kafka Source
        DataStreamSource<Event> source = env.fromSource(kafkaSource, watermarkStrategy, "Kafka Source");

        // Session Event Time Window
        SingleOutputStreamOperator<String> process = source.assignTimestampsAndWatermarks(
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
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .process(new EventProcessWindowFunction());

        // Kafka sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("example-output")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("session-event-time-window")
                .build();

        // send to Kafka
        process.sinkTo(kafkaSink);
        return env.execute("SessionEventTimeWindow Job");
    }
}
