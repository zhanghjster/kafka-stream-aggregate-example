package myapps;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class DataPointTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        if (record != null && record.value() != null) {
            if (record.value() instanceof DataPoint) {
                DataPoint value = (DataPoint) record.value();
                return value.getTimestamp() * 1000L;
            }
        }

        return 0;
    }
}
