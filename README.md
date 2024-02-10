# flink-playground

## Requirements
- **Java 11**
- **Flink 1.18.1**

## Content
| Directory                       | Window            | State      | Description                                                 |
|---------------------------------|-------------------|------------|-------------------------------------------------------------|
| `sumin/example/window/tumbling` | TumblingEventTime | ValueState | Fixed window Size with 10s, allow 5s lateness               |
| `sumin/example/window/sliding`  | SlidingEventTime  | ValueState | window is sliding every 5s with 20s size, allow 5s lateness |
