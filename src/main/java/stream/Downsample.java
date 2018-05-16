package stream;

import stream.Serde.JsonDeSerializer;
import stream.Serde.JsonSerializer;
import stream.Serde.WrapperSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Downsample {
    public static  void main(String args[]) {
        // 配置
        Properties prop = new Properties();
        DataPointSerde dataPointSerde = new DataPointSerde();

        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-data-point-aggregate");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, dataPointSerde.getClass());
        prop.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, DataPointTimestampExtractor.class);
       // prop.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        final StatsSerde statsSerde = new StatsSerde();
        final AggregateKeySerde aggregateKeySerde = new AggregateKeySerde();

        // 建立拓扑结构
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<Windowed<AggregateKey>, Stats> stream = builder.<String, DataPoint>stream("data-point-input").
                selectKey((key, value) -> AggregateKey.with(value)).
                through("data-point-aggregate-key-repartition", Produced.keySerde(aggregateKeySerde)).
                groupByKey().
                windowedBy(TimeWindows.of(10_000L).until(5000_000L)).
                aggregate(
                        () -> new Stats(),
                        (key, value, aggregate) -> aggregate.update(value),
                        Materialized.<AggregateKey, Stats, WindowStore<Bytes, byte[]>>as("stats-store").
                                withValueSerde(statsSerde).withKeySerde(aggregateKeySerde)
                ).toStream();

        // 以data point格式输出到topic
        stream.map((KeyValueMapper<Windowed<AggregateKey>, Stats, KeyValue<AggregateKey, DataPoint>>) (key, value) -> {
            DataPoint dataPoint = DataPoint.from(key.key()).withStats(value).withTimestamp(key.window().start()/1000);
            return KeyValue.pair(key.key(), dataPoint);
        }).to("data-point-output", Produced.keySerde(aggregateKeySerde));

        // 以stats格式输出到topic
        stream.map((KeyValueMapper<Windowed<AggregateKey>, Stats, KeyValue<AggregateKey, Stats>>) (key, value) -> {
            value.setWindow(key.window());
            value.setMetric(key.key().getMetric());
            value.setTags(key.key().getTags());
            return KeyValue.pair(key.key(), value);
        }).to("stats-output", Produced.with(aggregateKeySerde, statsSerde));


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

    static public final class DataPointSerde extends WrapperSerde<DataPoint> {
        public DataPointSerde() {
            super(new JsonSerializer<DataPoint>(), new JsonDeSerializer<DataPoint>(DataPoint.class));
        }
    }

    static public final class StatsSerde extends WrapperSerde<Stats> {
        public StatsSerde() {
            super(new JsonSerializer<Stats>(), new JsonDeSerializer<Stats>(Stats.class));
        }
    }

    static public final class AggregateKeySerde extends WrapperSerde<AggregateKey> {
        public AggregateKeySerde() {
            super(new JsonSerializer<>(), new JsonDeSerializer<>(AggregateKey.class));
        }
    }
}
