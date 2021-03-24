package io.lenses.streamreactor.testcontainers.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

    public static final int SCHEMA_REGISTRY_PORT = 8081;

    public SchemaRegistryContainer(String confluentPlatformVersion) {
        super("confluentinc/cp-schema-registry:" + confluentPlatformVersion);
        this.setWaitStrategy(Wait.forHttp("/subjects").forPort(SCHEMA_REGISTRY_PORT).forStatusCode(200));
        withExposedPorts(SCHEMA_REGISTRY_PORT);
    }

    public SchemaRegistryContainer withKafka(KafkaContainer kafka) {
        this.dependsOn(kafka);
        return withKafka(kafka.getNetwork(), kafka.getNetworkAliases().get(0) + ":9092");
    }

    public SchemaRegistryContainer withKafka(Network network, String bootstrapServers) {
        this.withNetwork(network);
        this.withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        this.withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT);
        this.withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + bootstrapServers);
        return this.self();
    }

    public String getUrl() {
        return String.format("http://%s:%d", this.getContainerIpAddress(), this.getMappedPort(SCHEMA_REGISTRY_PORT));
    }
}
