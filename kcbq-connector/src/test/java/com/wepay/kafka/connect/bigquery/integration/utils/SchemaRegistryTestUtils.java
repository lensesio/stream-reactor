package com.wepay.kafka.connect.bigquery.integration.utils;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.test.TestUtils;
import org.eclipse.jetty.server.Server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import org.apache.kafka.connect.storage.Converter;

import static io.confluent.kafka.schemaregistry.ClusterTestHarness.KAFKASTORE_TOPIC;
import static java.util.Objects.requireNonNull;

public class SchemaRegistryTestUtils {

    protected String bootstrapServers;

    private String schemaRegistryUrl;

    private RestApp restApp;
    public SchemaRegistryTestUtils(String bootstrapServers) {
        this.bootstrapServers = requireNonNull(bootstrapServers);
    }

    public void start() throws Exception {
        int port = findAvailableOpenPort();
        restApp = new RestApp(port, null, this.bootstrapServers,
                KAFKASTORE_TOPIC, CompatibilityLevel.NONE.name, true, new Properties());
        restApp.start();

        TestUtils.waitForCondition(() -> restApp.restServer.isRunning(), 10000L,
                "Schema Registry start timed out.");

        schemaRegistryUrl = restApp.restServer.getURI().toString();
    }

    public void stop() throws Exception {
        restApp.stop();
    }

    public String schemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    private Integer findAvailableOpenPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }


    private KafkaProducer<byte[], byte[]> configureProducer() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        return new KafkaProducer<>(producerProps, new ByteArraySerializer(), new ByteArraySerializer());
    }


    public void produceRecords(
            Converter converter,
            List<SchemaAndValue> recordsList,
            String topic
    ) {
        try (KafkaProducer<byte[], byte[]> producer = configureProducer()) {
            for (int i = 0; i < recordsList.size(); i++) {
                SchemaAndValue schemaAndValue = recordsList.get(i);
                byte[] convertedStruct = converter.fromConnectData(topic, schemaAndValue.schema(), schemaAndValue.value());
                ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, 0, String.valueOf(i).getBytes(), convertedStruct);
                try {
                    producer.send(msg).get(1, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new KafkaException("Could not produce message: " + msg, e);
                }
            }
        }
    }

    public void produceRecordsWithKey(
            Converter keyConverter,
            Converter valueConverter,
            List<List<SchemaAndValue>> recordsList,
            String topic
    ) {
        try (KafkaProducer<byte[], byte[]> producer = configureProducer()) {
            for (int i = 0; i < recordsList.size(); i++) {
                List<SchemaAndValue> record = recordsList.get(i);
                SchemaAndValue key = record.get(0);
                SchemaAndValue value = record.get(1);
                byte[] convertedStructKey = keyConverter.fromConnectData(topic, key.schema(), key.value());
                byte[] convertedStructValue;
                if(value == null) {
                    convertedStructValue = valueConverter.fromConnectData(topic, null, null);
                } else {
                    convertedStructValue = valueConverter.fromConnectData(topic, value.schema(), value.value());
                }
                ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, 0, convertedStructKey, convertedStructValue);
                try {
                    producer.send(msg).get(1, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new KafkaException("Could not produce message: " + msg, e);
                }
            }
        }
    }

}
