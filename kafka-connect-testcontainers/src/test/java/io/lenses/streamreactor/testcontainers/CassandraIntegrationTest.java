package io.lenses.streamreactor.testcontainers;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.jayway.jsonpath.JsonPath;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.lenses.streamreactor.testcontainers.base.AbstractStreamReactorTest;
import io.lenses.streamreactor.testcontainers.containers.KafkaConnectContainer;
import io.lenses.streamreactor.testcontainers.pojo.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.fest.assertions.Assertions.assertThat;

public class CassandraIntegrationTest extends AbstractStreamReactorTest {

    private static final KafkaConnectContainer connectContainer = connectContainer("cassandra");

    public static final CassandraContainer<?> cassandraContainer = new CassandraContainer<>("cassandra:3.11.2")
            .withNetwork(network)
            .withNetworkAliases("cassandra");

    private static Stream<GenericContainer<?>> testContainers() {
        return Stream.of(
                cassandraContainer,
                kafkaContainer,
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
    public void cassandraSource() throws Exception {

        try (Cluster cluster = cassandraContainer.getCluster();
             Session cassandraSession = cluster.connect();
             KafkaConsumer<String, String> consumer = getConsumer()) {

            // Create keyspace and table
            cassandraSession.execute("CREATE KEYSPACE source WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");
            cassandraSession.execute("CREATE TABLE source.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) WITH CLUSTERING ORDER BY (created asc);");

            // Register the connector
            ConnectorConfiguration config = ConnectorConfiguration.create();
            config.with("connector.class", "com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceConnector");
            config.with("connect.cassandra.key.space", "source");
            config.with("connect.cassandra.kcql", "INSERT INTO orders-topic SELECT * FROM orders PK created INCREMENTALMODE=TIMEUUID");
            config.with("connect.cassandra.contact.points", "cassandra");
            config.with("key.converter", "org.apache.kafka.connect.json.JsonConverter");
            config.with("key.converter.schemas.enable", "false");
            config.with("value.converter", "org.apache.kafka.connect.json.JsonConverter");
            config.with("value.converter.schemas.enable", "false");
            connectContainer.registerConnector("cassandra-source", config);

            // Insert data
            cassandraSession.execute("INSERT INTO source.orders (id, created, product, qty, price) VALUES (1, now(), 'OP-DAX-P-20150201-95.7', 100, 94.2);");
            cassandraSession.execute("INSERT INTO source.orders (id, created, product, qty, price) VALUES (2, now(), 'OP-DAX-C-20150201-100', 100, 99.5);");
            cassandraSession.execute("INSERT INTO source.orders (id, created, product, qty, price) VALUES (3, now(), 'FU-KOSPI-C-20150201-100', 200, 150);");

            // Validate
            consumer.subscribe(Collections.singletonList("orders-topic"));
            List<ConsumerRecord<String, String>> changeEvents = drain(consumer, 3);

            ConsumerRecord<String, String> changeEvent = changeEvents.get(0);
            assertThat(JsonPath.<Integer>read(changeEvent.value(), "$.id")).isEqualTo(1);
            assertThat(JsonPath.<String>read(changeEvent.value(), "$.product")).isEqualTo("OP-DAX-P-20150201-95.7");
            assertThat(JsonPath.<Integer>read(changeEvent.value(), "$.qty")).isEqualTo(100);
            assertThat(JsonPath.<Double>read(changeEvent.value(), "$.price")).isEqualTo(94.2);

            changeEvent = changeEvents.get(1);
            assertThat(JsonPath.<Integer>read(changeEvent.value(), "$.id")).isEqualTo(2);
            assertThat(JsonPath.<String>read(changeEvent.value(), "$.product")).isEqualTo("OP-DAX-C-20150201-100");
            assertThat(JsonPath.<Integer>read(changeEvent.value(), "$.qty")).isEqualTo(100);
            assertThat(JsonPath.<Double>read(changeEvent.value(), "$.price")).isEqualTo(99.5);

            changeEvent = changeEvents.get(2);
            assertThat(JsonPath.<Integer>read(changeEvent.value(), "$.id")).isEqualTo(3);
            assertThat(JsonPath.<String>read(changeEvent.value(), "$.product")).isEqualTo("FU-KOSPI-C-20150201-100");
            assertThat(JsonPath.<Integer>read(changeEvent.value(), "$.qty")).isEqualTo(200);
            assertThat(JsonPath.<Double>read(changeEvent.value(), "$.price")).isEqualTo(150);

            consumer.unsubscribe();
        }
    }

    @Test
    public void cassandraSink() throws Exception {

        try (Cluster cluster = cassandraContainer.getCluster();
             Session cassandraSession = cluster.connect();
             KafkaProducer<String, Order> producer = getProducer()) {

            // Create keyspace and table
            cassandraSession.execute("CREATE KEYSPACE sink WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");
            cassandraSession.execute("CREATE TABLE sink.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) WITH CLUSTERING ORDER BY (created asc);");

            // Register the connector
            ConnectorConfiguration config = ConnectorConfiguration.create();
            config.with("connector.class", "com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector");
            config.with("tasks.max", "1");
            config.with("topics", "orders");
            config.with("connect.cassandra.key.space", "sink");
            config.with("connect.cassandra.port", "9042");
            config.with("connect.cassandra.kcql", "INSERT INTO orders SELECT * FROM orders");
            config.with("connect.cassandra.contact.points", "cassandra");
            connectContainer.registerConnector("cassandra-sink", config);

            // Write records to topic
            Order order = new Order(1, UUIDs.timeBased().toString(), "OP-DAX-P-20150201-95.7", 94.2, 100);
            producer.send(new ProducerRecord<>("orders", order)).get();
            producer.flush();

            // Validate
            Unreliables.retryUntilTrue(1, TimeUnit.MINUTES, () -> {
                ResultSet resultSet = cassandraSession.execute("SELECT * FROM sink.orders;");
                return resultSet.getAvailableWithoutFetching() == 1;
            });

            Row one = cassandraSession.execute("SELECT * FROM sink.orders;").one();
            assertThat(one.get("id", Integer.class)).isEqualTo(1);
            assertThat(one.get("product", String.class)).isEqualTo("OP-DAX-P-20150201-95.7");
            assertThat(one.get("price", Float.class)).isEqualTo(94.2F);
            assertThat(one.get("qty", Integer.class)).isEqualTo(100);
        }
    }
}
