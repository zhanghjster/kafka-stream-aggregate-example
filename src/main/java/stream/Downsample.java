package stream;

import org.apache.kafka.common.serialization.Serde;
import stream.Serde.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Downsample {
    public static  void main(String args[]) {
        HashMap<String, Object> config = new HashMap<>();
        config.put(JsonPOJOSerializer.ORDER_MAP_ENTRIES_BY_KEYS, Boolean.TRUE);
        config.put(JsonPOJOSerializer.INCLUDE_NON_NULL, Boolean.TRUE);

        final Serde<Stats> statsSerde = JsonPOJOSerde.with(Stats.class, config, false);
        final Serde<AggregateKey> aggregateKeySerde = JsonPOJOSerde.with(AggregateKey.class, config, true);
        final Serde<DataPoint> dataPointSerde = JsonPOJOSerde.with(DataPoint.class, config, false);

        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-data-point-aggregate");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, DataPointTimestampExtractor.class);

        // prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, dataPointSerde.getClass());
       // prop.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);


        // 建立拓扑结构
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<Windowed<AggregateKey>, Stats> stream = builder.stream("data-point-input", Consumed.with(Serdes.String(), dataPointSerde)).
                selectKey((key, value) -> AggregateKey.with(value)).
                through("data-point-aggregate-key-repartition", Produced.with(aggregateKeySerde, dataPointSerde)).
                groupByKey().
                windowedBy(TimeWindows.of(10_000L).until(5000_000L)).
                aggregate(
                        () -> new Stats(),
                        (key, value, aggregate) -> aggregate.update(value),
                        Materialized.<AggregateKey, Stats, WindowStore<Bytes, byte[]>>as("data-point-stats-store").
                                withValueSerde(statsSerde).withKeySerde(aggregateKeySerde)
                ).toStream();

        // 以data point格式输出到topic
        stream.map((KeyValueMapper<Windowed<AggregateKey>, Stats, KeyValue<AggregateKey, DataPoint>>) (key, value) -> {
            DataPoint dataPoint = DataPoint.from(key.key()).withStats(value).withTimestamp(key.window().start()/1000);
            return KeyValue.pair(key.key().with(new Window(key.window().start(), key.window().end())), dataPoint);
        }).to("data-point-output", Produced.with(aggregateKeySerde, dataPointSerde));

        // 以stats格式输出到topic
        stream.map((KeyValueMapper<Windowed<AggregateKey>, Stats, KeyValue<AggregateKey, Stats>>) (key, value) -> {
            value.setWindow(new Window(key.window().start(), key.window().end()));
            value.setMetric(key.key().getMetric());
            value.setTags(key.key().getTags());

            AggregateKey aggregateKey = key.key();
            aggregateKey.setWindow(new Window(key.window().start(), key.window().end()));

            System.out.println(aggregateKey.getWindow());

            return KeyValue.pair(aggregateKey, value);
        }).to("data-point-stats-output", Produced.with(aggregateKeySerde, statsSerde));

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        // 根据拓扑结构建立stream
        final KafkaStreams streams = new KafkaStreams(topology, prop);
        final CountDownLatch latch = new CountDownLatch(1);

        // 运行并扑捉退出信号
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);
    }
}
