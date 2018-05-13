package stream;

import stream.Serde.JsonDeSerializer;
import stream.Serde.JsonSerializer;
import stream.Serde.WrapperSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class UserRank {
    public static void main(String[] args) {
        // 配置
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-user-pv");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());

        Serde<Stats> statsSerde = new StatsSerde();

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Stats> stream = builder.<String, Double>stream("user-rank-input").
                groupByKey().
                windowedBy(TimeWindows.of(10_000L).until(3600_000L)).
                aggregate(
                    Stats::new,
                    (key, value, aggregate) -> {
                        aggregate.update(value);
                        return aggregate;
                    },
                    Materialized.<String, Stats, WindowStore<Bytes, byte[]>>as("aggregated-table-store").
                        withValueSerde(new StatsSerde())
                ).toStream().map((KeyValueMapper<Windowed<String>, Stats, KeyValue<String, Stats>>) (key, value) -> {
                    value.caculateAvg();
                    value.setWindow(new Stats.Window(key.window().start(), key.window().end()));
                    return new KeyValue<>(key.key(), value);
                });

        stream.to("user-rank-output", Produced.with(Serdes.String(), statsSerde));
        stream.foreach((key, value) -> System.out.println(value));

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

    static public final class StatsSerde extends WrapperSerde<Stats> {
        public StatsSerde() {
            super(new JsonSerializer<>(), new JsonDeSerializer<>(Stats.class));
        }
    }

}
