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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class UserRank {
    public static void main(String[] args) {
        // 配置
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-user-pv");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());

        Serde<Stats> tupleSerde = new TupleSerde();


        // 建立拓扑结构
        final StreamsBuilder builder = new StreamsBuilder();


        // 跨度为10秒的翻转窗口，回收时间为5000秒
        KTable<Windowed<String>, Stats> table = builder.<String, Double>stream("user-rank-input").
                groupByKey().
                windowedBy(TimeWindows.of(10_000L).until(5000_000L)).
                aggregate(
                    () -> new Stats(),
                    (key, value, aggregate) -> {
                        return aggregate;
                    },
                Materialized.<String, Stats, WindowStore<Bytes, byte[]>>as("aggregated-table-store").
                        withValueSerde(new TupleSerde())
        );

        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

        // print to stdout
        table.toStream().foreach((key, value) -> System.out.println(
                dateFormat.format(new Date(key.window().start()))

                )
        );

        table.toStream().map((KeyValueMapper<Windowed<String>, Stats, KeyValue<String, Stats>>) (key, value) -> {
            return new KeyValue<>(key.key(), value);
        }).to("user-rank-output", Produced.with(Serdes.String(), tupleSerde));

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

    static public final class TupleSerde extends WrapperSerde<Stats> {
        public TupleSerde() {
            super(new JsonSerializer<Stats>(), new JsonDeSerializer<Stats>(Stats.class));
        }
    }

}
