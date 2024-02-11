# flink-playground

## Requirements
- **Java 11**
- **Flink 1.18.1**

## Content
| Directory                       | Window            | State      | source         | sink      | Description                                                |
|---------------------------------|-------------------|------------|----------------|-----------|------------------------------------------------------------|
| `sumin/example/window/tumbling` | TumblingEventTime | ValueState | FromElement    | print     | Fixed window Size with 10s, allow 5s lateness              |
| `sumin/example/window/sliding`  | SlidingEventTime  | ValueState | SourceFunction | print     | window is sliding every 5s with 20s size, allow 5s lateness |
| `sumin/example/window/sessiom`  | SessionEventTime  | ValueState | KafkaSource    | KafkaSink | window will end when time gap is 10s, allow 5s lateness    |
