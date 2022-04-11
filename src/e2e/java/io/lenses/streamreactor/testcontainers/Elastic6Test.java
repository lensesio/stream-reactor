package io.lenses.streamreactor.testcontainers;

import com.jayway.jsonpath.JsonPath;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.lenses.streamreactor.testcontainers.base.AbstractStreamReactorTest;
import io.lenses.streamreactor.testcontainers.containers.KafkaConnectContainer;
import io.lenses.streamreactor.testcontainers.containers.SchemaRegistryContainer;
import io.lenses.streamreactor.testcontainers.pojo.Order;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.fest.assertions.Assertions.assertThat;

@Ignore("Temporary ignore as it fails on CI but runs successfully locally")
public class Elastic6Test extends AbstractStreamReactorTest {

    private static final KafkaConnectContainer connectContainer = connectContainer("elastic6");

    private static final SchemaRegistryContainer schemaRegistryContainer = new SchemaRegistryContainer(CONFLUENT_PLATFORM_VERSION)
            .withKafka(kafkaContainer)
            .withNetworkAliases("schema-registry");

    private static final String ELASTICSEARCH_HOST = "elastic";

    private static final Integer ELASTICSEARCH_PORT = 9200;

    private static final ElasticsearchContainer elasticContainer = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:6.8.8")
            .withNetwork(network)
            .withExposedPorts(ELASTICSEARCH_PORT)
            .withNetworkAliases(ELASTICSEARCH_HOST);

    private static Stream<GenericContainer<?>> testContainers() {
        return Stream.of(
                elasticContainer,
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
    public void elasticSink() throws Exception {

        try (KafkaProducer<String, Order> producer = getProducer()) {

            // Register the connector
            ConnectorConfiguration config = ConnectorConfiguration.create();
            config.with("connector.class", "com.datamountaineer.streamreactor.connect.elastic6.ElasticSinkConnector");
            config.with("tasks.max", "1");
            config.with("topics", "orders");
            config.with("connect.elastic.protocol", "http");
            config.with("connect.elastic.hosts", ELASTICSEARCH_HOST);
            config.with("connect.elastic.port", ELASTICSEARCH_PORT);
            config.with("connect.elastic.cluster.name", "elasticsearch");
            config.with("connect.elastic.kcql", "INSERT INTO orders SELECT * FROM orders");
            config.with("connect.progress.enabled", "true");
            connectContainer.registerConnector("elastic-sink", config);

            // Write records to topic
            Order order = new Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100);
            producer.send(new ProducerRecord<>("orders", order)).get();
            producer.flush();

            OkHttpClient client = new OkHttpClient();
            Request request = new Request.Builder()
                    .url("http://" + elasticContainer.getHttpHostAddress() + "/orders/_search/?q=OP-DAX-P-20150201")
                    .build();

            Unreliables.retryUntilTrue(1, TimeUnit.MINUTES, () -> {
                Response response = client.newCall(request).execute();
                String body = response.body().string();
                return JsonPath.<Integer>read(body, "$.hits.total") == 1;
            });

            Response response = client.newCall(request).execute();
            String body = response.body().string();
            assertThat(JsonPath.<Integer>read(body, "$.hits.hits[0]._source.id")).isEqualTo(1);
            assertThat(JsonPath.<String>read(body, "$.hits.hits[0]._source.product")).isEqualTo("OP-DAX-P-20150201-95.7");
            assertThat(JsonPath.<Double>read(body, "$.hits.hits[0]._source.price")).isEqualTo(94.2);
            assertThat(JsonPath.<Integer>read(body, "$.hits.hits[0]._source.qty")).isEqualTo(100);
        }
    }
}
