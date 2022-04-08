package io.lenses.streamreactor.testcontainers.base;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.lenses.streamreactor.testcontainers.containers.KafkaConnectContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Map;

import static io.lenses.streamreactor.testcontainers.containers.KafkaConnectContainer.CONNECT_PLUGIN_PATH;

public abstract class AbstractStreamReactorTest {

    public static final String CONFLUENT_PLATFORM_VERSION = Optional.ofNullable(System.getenv("CONFLUENT_VERSION"))
            .orElse("6.2.2");

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamReactorTest.class);

    protected static final Network network = Network.newNetwork();

    protected static final KafkaContainer kafkaContainer = new KafkaContainer(CONFLUENT_PLATFORM_VERSION)
            .withNetwork(network)
            .withNetworkAliases("broker");


    public static void startContainers(Stream<GenericContainer<?>> testContainers) {
        Startables.deepStart(testContainers).join();
    }

    public static void stopContainers(Stream<GenericContainer<?>> testContainers) {
        reverse(testContainers).forEachRemaining(GenericContainer::stop);
    }

    /**
     * Prepare kafka connect container.
     * Download and mount the connector to the plugin path.
     *
     * @param connector the connector name.
     * @return the kafka connect container.
     */
    public static KafkaConnectContainer connectContainer(final String connector) {
        Path connectorPath = connectorPath(connector);
        return new KafkaConnectContainer("confluentinc/cp-kafka-connect-base:" + CONFLUENT_PLATFORM_VERSION)
                .withNetwork(network)
                .withKafka(kafkaContainer)
                .withFileSystemBind(connectorPath.toString(),
                        CONNECT_PLUGIN_PATH,
                        BindMode.READ_ONLY)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .dependsOn(kafkaContainer);
    }

    /**
     * Drain a kafka topic.
     *
     * @param consumer            the kafka consumer.
     * @param expectedRecordCount the expected records.
     * @param <K>                 key type.
     * @param <V>                 value type.
     * @return the records.
     */
    public <K, V> List<ConsumerRecord<K, V>> drain(KafkaConsumer<K, V> consumer, int expectedRecordCount) {
        List<ConsumerRecord<K, V>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(1, TimeUnit.MINUTES, () -> {
            consumer.poll(Duration.ofMillis(50))
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return (allRecords.size() == expectedRecordCount);
        });

        return allRecords;
    }

    public <T> KafkaProducer<String, T> getProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        return new KafkaProducer<>(props);
    }

    public <T> KafkaProducer<String, T> getAvroProducer(String schemaRegistryUrl) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return new KafkaProducer<>(props);
    }

    public KafkaConsumer<String, String> getConsumer() {
        return new KafkaConsumer<>(
                ImmutableMap.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "cg-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(),
                new StringDeserializer()
        );
    }

    public static <T> Iterator<T> reverse(Stream<T> stream) {
        return stream.collect(Collectors.toCollection(LinkedList::new))
                .descendingIterator();
    }

    /**
     * Resolve the connector path using the connector name and kafka version directory suffix.
     *
     * @param connector the connector name.
     * @return the connector path.
     */
    public static Path connectorPath(String connector) {
        final String directorySuffix =
                Optional.ofNullable(System.getenv("KAFKA_VERSION_DIRECTORY_SUFFIX"))
                        .orElseThrow(
                                () -> new IllegalArgumentException("Please define the KAFKA_VERSION_DIRECTORY_SUFFIX environment variable."));

        try {
            final String regex = String.format(".*%s%s.*.jar", connector, directorySuffix);
            final List<Path> files = Files.find(Paths.get(String.join(File.separator,
                            System.getProperty("user.dir"), "kafka-connect-" + connector, "target")),
                    3,
                    (path, basicFileAttributes) -> path.toFile().getName().matches(regex)
            ).collect(Collectors.toList());
            LOGGER.info("Files: " + files.stream().map(file -> String.join("::", file.toString())).collect(Collectors.toList()));
            if (files.isEmpty()) {
                throw new RuntimeException(String.format("Please run `sbt \"project %s%s\" assembly`", connector, directorySuffix));
            }
            return files.get(0).getParent();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
