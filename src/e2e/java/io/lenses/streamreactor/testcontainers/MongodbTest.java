package io.lenses.streamreactor.testcontainers;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.lenses.streamreactor.testcontainers.base.AbstractStreamReactorTest;
import io.lenses.streamreactor.testcontainers.containers.KafkaConnectContainer;
import io.lenses.streamreactor.testcontainers.containers.SchemaRegistryContainer;
import io.lenses.streamreactor.testcontainers.pojo.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MongoDBContainer;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.fest.assertions.Assertions.assertThat;

public class MongodbTest extends AbstractStreamReactorTest {

    private static final KafkaConnectContainer connectContainer = connectContainer("mongodb");

    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(CONFLUENT_PLATFORM_VERSION)
            .withKafka(kafkaContainer)
            .withNetworkAliases("schema-registry");

    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer()
            .withNetwork(network)
            .withNetworkAliases("mongo");

    private static Stream<GenericContainer<?>> testContainers() {
        return Stream.of(
                mongoDBContainer,
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
    public void mongoDBSink() throws Exception {

        try (MongoClient mongoClient = new MongoClient(mongoDBContainer.getContainerIpAddress(), mongoDBContainer.getMappedPort(27017));
             KafkaProducer<String, Order> producer = getProducer()) {

            // Register the connector
            ConnectorConfiguration config = ConnectorConfiguration.create();
            config.with("connector.class", "com.datamountaineer.streamreactor.connect.mongodb.sink.MongoSinkConnector");
            config.with("tasks.max", "1");
            config.with("topics", "orders");
            config.with("connect.mongo.kcql", "INSERT INTO orders SELECT * FROM orders");
            config.with("connect.mongo.db", "connect");
            config.with("connect.mongo.connection", "mongodb://mongo:27017");
            connectContainer.registerConnector("mongodb-sink", config);

            // Write records to topic
            Order order = new Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100);
            producer.send(new ProducerRecord<>("orders", order)).get();
            producer.flush();

            // Validate
            MongoDatabase database = mongoClient.getDatabase("connect");
            MongoCollection<Document> orders = database.getCollection("orders");

            Unreliables.retryUntilTrue(1, TimeUnit.MINUTES, () -> {
                long ordersCount = orders.count();
                return ordersCount == 1;
            });

            Document one = orders.find().iterator().next();
            assertThat(one.get("id", Long.class)).isEqualTo(1);
            assertThat(one.get("product", String.class)).isEqualTo("OP-DAX-P-20150201-95.7");
        }
    }
}
