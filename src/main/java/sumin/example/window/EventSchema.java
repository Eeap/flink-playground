package sumin.example.window;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

@JsonPropertyOrder({"eventID", "eventName", "eventTime", "eventMessage"})
public class EventSchema implements DeserializationSchema<Event>, SerializationSchema<Event> {
    private ObjectMapper objectMapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();
    }

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Event.class);
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public byte[] serialize(Event event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
