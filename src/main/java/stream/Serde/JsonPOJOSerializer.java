package stream.Serde;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonPOJOSerializer<T> implements Serializer<T> {
    public static final String INCLUDE_NON_NULL = "include_non_null";
    public static final String INDENT_OUTPUT = "indent_output";
    public static final String ORDER_MAP_ENTRIES_BY_KEYS = "order_map_entries_by_key";

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJOSerializer() {
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        if (props.containsKey(INCLUDE_NON_NULL) && (Boolean) props.get(INCLUDE_NON_NULL)){
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        }

        if (props.containsKey(INDENT_OUTPUT) && (Boolean) props.get(INDENT_OUTPUT)) {
            objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        }

        if (props.containsKey(ORDER_MAP_ENTRIES_BY_KEYS) && (Boolean) props.get(ORDER_MAP_ENTRIES_BY_KEYS)) {
            objectMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

}
