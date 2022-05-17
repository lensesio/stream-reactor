package io.lenses.streamreactor.connect.testcontainers;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.lenses.streamreactor.connect.testcontainers.base.AbstractStreamReactorTest;
import io.lenses.streamreactor.connect.testcontainers.containers.KafkaConnectContainer;
import io.lenses.streamreactor.connect.testcontainers.containers.SchemaRegistryContainer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamInfo;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.fest.assertions.Assertions.assertThat;

public class RedisTest extends AbstractStreamReactorTest {

    private static final KafkaConnectContainer connectContainer = connectContainer("redis");

    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(CONFLUENT_PLATFORM_VERSION)
            .withKafka(kafkaContainer)
            .withNetworkAliases("schema-registry");

    private static final String REDIS_HOST = "redis";

    private static final Integer REDIS_PORT = 6379;

    private static final GenericContainer<?> redisContainer = new GenericContainer<>("redis:6-alpine")
            .withNetwork(network)
            .withExposedPorts(REDIS_PORT)
            .withNetworkAliases(REDIS_HOST);

    private static Stream<GenericContainer<?>> testContainers() {
        return Stream.of(
                redisContainer,
                kafkaContainer,
                schemaRegistryContainer,
                connectContainer
        );
    }

    @BeforeClass
    public static void startContainers() {
        startContainers(testContainers());
    }

    @AfterClass
    public static void stopContainers() {
        stopContainers(testContainers());
    }

    @Test
    public void redisSink() throws Exception {

        try (Jedis jedis = new Jedis(redisContainer.getContainerIpAddress(), redisContainer.getMappedPort(REDIS_PORT));
             KafkaProducer<String, Object> producer = getAvroProducer(schemaRegistryContainer.getUrl())) {

            // Register the connector
            ConnectorConfiguration config = ConnectorConfiguration.create();
            config.with("connector.class", "com.datamountaineer.streamreactor.connect.redis.sink.RedisSinkConnector");
            config.with("tasks.max", "1");
            config.with("topics", "redis");
            config.with("connect.redis.host", REDIS_HOST);
            config.with("connect.redis.port", REDIS_PORT);
            config.with("connect.redis.kcql", "INSERT INTO lenses SELECT * FROM redis STOREAS STREAM");
            config.with("key.converter", "org.apache.kafka.connect.storage.StringConverter");
            config.with("value.converter", "io.confluent.connect.avro.AvroConverter");
            config.with("value.converter.schema.registry.url", "http://schema-registry:8081");
            connectContainer.registerConnector("redis-sink", config);

            String userSchema = "{\"type\":\"record\",\"name\":\"User\",\n" +
                    "  \"fields\":[{\"name\":\"firstName\",\"type\":\"string\"}," +
                    "{\"name\":\"lastName\",\"type\":\"string\"}," +
                    "{\"name\":\"age\",\"type\":\"int\"}," +
                    "{\"name\":\"salary\",\"type\":\"double\"}]}";
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(userSchema);
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("firstName", "John");
            avroRecord.put("lastName", "Smith");
            avroRecord.put("age", 30);
            avroRecord.put("salary", 4830);

            producer.send(new ProducerRecord<>("redis", avroRecord)).get();
            producer.flush();

            Unreliables.retryUntilTrue(1, TimeUnit.MINUTES, () -> {
                StreamInfo streamInfo = jedis.xinfoStream("lenses");
                return streamInfo.getLength() == 1;
            });

            Map<String, String> userFields = jedis.xinfoStream("lenses").getFirstEntry().getFields();
            assertThat(userFields.get("firstName")).isEqualTo("John");
            assertThat(userFields.get("lastName")).isEqualTo("Smith");
            assertThat(userFields.get("age")).isEqualTo("30");
            assertThat(userFields.get("salary")).isEqualTo("4830.0");
        }
    }
}
