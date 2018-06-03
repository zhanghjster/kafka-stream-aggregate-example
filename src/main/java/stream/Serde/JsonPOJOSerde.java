package stream.Serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonPOJOSerde<T> implements Serde<T> {
    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    public JsonPOJOSerde() {
        this.serializer = new JsonPOJOSerializer<>();
        this.deserializer = new JsonPOJODeserializer<>();
    }



    static public <T> Serde<T> with (Class<T> tClass, Map<String, Object> config, boolean isKey) {
        JsonPOJOSerde serde = new JsonPOJOSerde();
        config.put("JsonPOJOClass", tClass);
        serde.configure(config, isKey);
        return serde;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
