package myapps;

import myapps.Serde.JsonDeSerializer;
import myapps.Serde.JsonSerializer;
import myapps.Serde.WrapperSerde;
import org.apache.kafka.common.metrics.Stat;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import javax.xml.crypto.Data;
import java.util.Properties;
import java.util.TreeMap;
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
        prop.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);


        // 建立拓扑结构
        final StreamsBuilder builder = new StreamsBuilder();


        KStream<String, DataPoint> sink = builder.<String, DataPoint>stream("data-point-input").
                selectKey((key, value) -> {
                    String newKey = value.getMetric();
                    if (value.getTags() != null) {
                        TreeMap tags = new TreeMap(value.getTags());
                        newKey += tags.toString();
                    }
                    return newKey;
                }).
                through("data-point-metric-tag-key").
                groupByKey().
                windowedBy(TimeWindows.of(10_000L).until(5000_000L)).
                aggregate(
                        () -> new Stats(),
                        (key, value, aggregate) -> aggregate.update(value),
                        Materialized.<String, Stats, WindowStore<Bytes, byte[]>>as("data-point-value").withValueSerde(new StatsSerde())
                ).
                toStream().
                map((key, value) -> {
                    value.caculateAgv();
                    DataPoint dp = value.getDp();
                    dp.setValue(value.getCount());
                    System.out.println("window start "+key.window().start());
                    dp.setTimestamp(key.window().start());
                    return KeyValue.pair(key.key(), dp);
                });


        sink.to("data-point-output");

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
}
