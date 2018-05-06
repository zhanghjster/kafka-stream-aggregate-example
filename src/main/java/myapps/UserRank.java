package myapps;

import myapps.Serde.JsonDeSerializer;
import myapps.Serde.JsonSerializer;
import myapps.Serde.WrapperSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
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

        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(new StringSerializer());
        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(new StringDeserializer());
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer,windowedDeserializer);

        Serde<Tuple> tupleSerde = new TupleSerde();



        // 建立拓扑结构
        final StreamsBuilder builder = new StreamsBuilder();

        // source processor
        KStream<String, Double> stream = builder.stream("user-rank-input");

        // group
        KGroupedStream<String, Double> group = stream.groupByKey();


        // 跨度为10秒的翻转窗口，回收时间为5000秒
        KTable<Windowed<String>, Tuple> table = group.windowedBy(TimeWindows.of(10_000L).until(5000_000L)).aggregate(
                () -> new Tuple(),
                (key, value, aggregate) -> {
                    aggregate.Count++;
                    aggregate.Sum += value;
                    aggregate.Last = value;
                    aggregate.Max = (aggregate.Max < value) ? value : aggregate.Max;
                    aggregate.Min = (aggregate.Min > value) ? value : aggregate.Min;
                    return aggregate;
                },
                Materialized.<String, Tuple, WindowStore<Bytes, byte[]>>as("aggregated-table-store").withValueSerde(new TupleSerde())
        );

        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

        // print to stdout
        table.toStream().foreach((key, value) -> System.out.println(
                dateFormat.format(new Date(key.window().start())) +
                        ": sum => " + value.Sum +
                        " count => " + value.Count +
                        " max => " + value.Max +
                        " min => " + value.Min +
                        " last => " + value.Last
                )
        );
        table.toStream().to("user-rank-output", Produced.with(windowedSerde, tupleSerde));

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

    static public final class TupleSerde extends WrapperSerde<Tuple> {
        public TupleSerde() {
            super(new JsonSerializer<Tuple>(), new JsonDeSerializer<Tuple>(Tuple.class));
        }
    }

}
