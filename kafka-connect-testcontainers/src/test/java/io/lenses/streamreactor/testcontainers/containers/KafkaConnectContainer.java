package io.lenses.streamreactor.testcontainers.containers;

import io.debezium.testing.testcontainers.Connector;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class KafkaConnectContainer extends GenericContainer<KafkaConnectContainer> {

    public static final String CONNECT_PLUGIN_PATH = "/kafka/connect/";

    public static final int KAFKA_CONNECT_PORT = 8083;

    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectContainer.class);

    private final OkHttpClient client = new OkHttpClient();


    public KafkaConnectContainer(String containerImageName) {
        super(containerImageName);
        this.setWaitStrategy(Wait.forHttp("/connectors").forPort(KAFKA_CONNECT_PORT).forStatusCode(200));
        this.withEnv("CONNECT_GROUP_ID", "1");
        this.withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect_config");
        this.withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect_offsets");
        this.withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect_status");
        this.withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
        this.withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
        this.withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
        this.withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
        this.withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
        this.withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter");
        this.withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.storage.StringConverter");
        this.withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", this.getHost());
        this.withEnv("CONNECT_PLUGIN_PATH", CONNECT_PLUGIN_PATH);
        this.withExposedPorts(KAFKA_CONNECT_PORT);
    }

    public KafkaConnectContainer withKafka(KafkaContainer kafkaContainer) {
        return this.withKafka(kafkaContainer.getNetwork(), kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    public KafkaConnectContainer withKafka(Network network, String bootstrapServers) {
        this.withNetwork(network);
        this.withEnv("CONNECT_BOOTSTRAP_SERVERS", bootstrapServers);
        return this.self();
    }

    public void registerConnector(String name, ConnectorConfiguration configuration) throws IOException {
        Connector connector = Connector.from(name, configuration);
        this.registerConnector(connector.toJson(), this.getConnectorsUrl());
        Awaitility.await().atMost(10L, TimeUnit.SECONDS).until(() -> this.isConnectorConfigured(connector.getName()));
    }

    public void unregisterConnector(String name) throws IOException {
        Request request = new Request.Builder()
                .url(this.getConnectorUrl(name))
                .delete()
                .build();
        try (Response response = this.client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response + "Message: " + response.body().string());
            }
        }
    }

    private void registerConnector(String payload, String fullUrl) throws IOException {
        RequestBody body = RequestBody.create(payload, JSON);
        Request request = new Request.Builder()
                .url(fullUrl)
                .post(body)
                .build();
        try (Response response = this.client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response + "Message: " + response.body().string());
            }
        }
    }

    private boolean isConnectorConfigured(String connectorName) throws IOException {
        Request request = new Request.Builder()
                .url(this.getConnectorUrl(connectorName))
                .build();
        try (Response response = this.client.newCall(request).execute()) {
            return response.isSuccessful();
        }
    }

    public String getConnectorsUrl() {
        return this.getTarget() + "/connectors/";
    }

    public String getConnectorUrl(String connectorName) {
        return this.getConnectorsUrl() + connectorName;
    }

    public String getConnectorStatusUrl(String connectorName) {
        return this.getConnectorsUrl() + connectorName + "/status";
    }

    public String getTarget() {
        return "http://" + this.getContainerIpAddress() + ":" + this.getMappedPort(KAFKA_CONNECT_PORT);
    }

    /**
     * Print connector status for debugging purposes.
     *
     * @param connectorName the connector name.
     * @throws IOException on http connectivity issues.
     */
    public void printConnectorStatus(String connectorName) throws IOException {
        String statusUrl = getConnectorStatusUrl(connectorName);
        Request request = new Request.Builder()
                .url(statusUrl)
                .build();
        Response response = this.client.newCall(request).execute();
        String body = response.body().string();
        LOGGER.info(body);
    }
}
