package com.wepay.kafka.connect.bigquery.integration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.BigQueryTestUtils;
import com.wepay.kafka.connect.bigquery.integration.utils.SchemaRegistryTestUtils;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import io.confluent.connect.avro.AvroConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class BigQueryStorageWriteApiSinkConnectorIT extends BaseConnectorIT {

    private static final Logger logger = LoggerFactory.getLogger(BigQueryStorageWriteApiSinkConnectorIT.class);
    private static SchemaRegistryTestUtils schemaRegistry;
    private static String schemaRegistryUrl;
    private org.apache.kafka.connect.data.Schema valueSchema;
    private static final String CONNECTOR_NAME = "bigquery-storage-api-sink-connector";
    private BigQuery bigQuery;
    private Schema keySchema;
    private Converter keyConverter;
    private Converter valueConverter;
    private static final String KAFKA_FIELD_NAME = "kafkaKey";
    private static final long NUM_RECORDS_PRODUCED = 5;
    private static final int TASKS_MAX = 1;
    protected static final long COMMIT_MAX_DURATION_MS = TimeUnit.MINUTES.toMillis(7);
    @Before
    public void setup() throws Exception {
        startConnect();
        bigQuery = newBigQuery();
        schemaRegistry = new SchemaRegistryTestUtils(connect.kafka().bootstrapServers());
        schemaRegistry.start();
        schemaRegistryUrl = schemaRegistry.schemaRegistryUrl();

        valueSchema = SchemaBuilder.struct()
                .optional()
                .field("f1", Schema.STRING_SCHEMA)
                .field("f2", Schema.BOOLEAN_SCHEMA)
                .field("f3", Schema.FLOAT64_SCHEMA)
                .build();
        keySchema = SchemaBuilder.struct()
                .field("k1", Schema.INT64_SCHEMA)
                .build();


    }

    @After
    public void close() throws Exception {
        bigQuery = null;

        if (schemaRegistry != null) {
            schemaRegistry.stop();
        }
        stopConnect();
    }

    @Test
    public void testBaseJson() throws InterruptedException {
        // create topic in Kafka
        final String topic = suffixedTableOrTopic("storage-api-append-json" + System.nanoTime());

        final String table = sanitizedTable(topic);

        // create topic
        connect.kafka().createTopic(topic, 1);

        // clean table
        TableClearer.clearTables(bigQuery, dataset(), table);

        // createTable with correct schema
        createTable(table, false);

        // setup props for the sink connector
        Map<String, String> props = configs(topic);
        // use the JSON converter with schemas enabled
        props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        props.remove(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG);
        // start a sink connector
        connect.configureConnector(CONNECTOR_NAME, props);

        // wait for tasks to spin up
        waitForConnectorToStart(CONNECTOR_NAME, 1);

        // Instantiate the converters we'll use to send records to the connector
        initialiseJsonConverters();

        //produce records
        produceJsonRecords(topic);

        // wait for tasks to write to BigQuery and commit offsets for their records
        waitForCommittedRecords(CONNECTOR_NAME, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

        // verify records are present.
        List<List<Object>> testRows;
        try {
            testRows = readAllRows(bigQuery, table, "f3");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertEquals(expectedRows(), testRows.stream().map(row -> row.get(0)).collect(Collectors.toSet()));
    }

    @Test
    public void testBaseAvro() throws InterruptedException {
        // create topic in Kafka
        final String topic = suffixedTableOrTopic("storage-api-append" + System.nanoTime());
        final String table = sanitizedTable(topic);

        // create topic
        connect.kafka().createTopic(topic, 1);

        // clean table
        TableClearer.clearTables(bigQuery, dataset(), table);

        // setup props for the sink connector
        Map<String, String> props = configs(topic);

        // start a sink connector
        connect.configureConnector(CONNECTOR_NAME, props);

        // wait for tasks to spin up
        waitForConnectorToStart(CONNECTOR_NAME, 1);

        // Instantiate the converters we'll use to send records to the connector
        initialiseAvroConverters();

        //produce records
        produceAvroRecords(topic);

        // wait for tasks to write to BigQuery and commit offsets for their records
        waitForCommittedRecords(
                CONNECTOR_NAME, Collections.singleton(topic), NUM_RECORDS_PRODUCED, TASKS_MAX, COMMIT_MAX_DURATION_MS);

        // verify records are present.
        List<List<Object>> testRows;
        try {
            testRows = readAllRows(bigQuery, table, "f3");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertEquals(expectedRows(), testRows.stream().map(row -> row.get(0)).collect(Collectors.toSet()));
    }

    @Test
    public void testBaseWithAvroSchema() throws InterruptedException {
        // create topic in Kafka
        final String topic = suffixedTableOrTopic("storage-api-schema-update-append" + System.nanoTime());
        final String table = sanitizedTable(topic);

        // create topic
        connect.kafka().createTopic(topic, 1);

        // clean table
        TableClearer.clearTables(bigQuery, dataset(), table);

        // createTable with incorrect schema
        createTable(table, true);

        // setup props + schema update props for the sink connector
        Map<String, String> props = configs(topic);

        props.put(BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG, "true");
        props.put(BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG, "true");
        props.put(BigQuerySinkConfig.ALLOW_SCHEMA_UNIONIZATION_CONFIG, "true");
        props.put(BigQuerySinkConfig.ALL_BQ_FIELDS_NULLABLE_CONFIG, "true");

        // start a sink connector
        connect.configureConnector(CONNECTOR_NAME, props);

        // wait for tasks to spin up
        waitForConnectorToStart(CONNECTOR_NAME, 1);

        // Instantiate the converters we'll use to send records to the connector
        initialiseAvroConverters();

        //produce records
        produceAvroRecords(topic);

        // wait for tasks to write to BigQuery and commit offsets for their records
        waitForCommittedRecords(CONNECTOR_NAME, topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

        // verify records are present.
        List<List<Object>> testRows;
        try {
            testRows = readAllRows(bigQuery, table, "f3");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertEquals(expectedRows(), testRows.stream().map(row -> row.get(0)).collect(Collectors.toSet()));
    }

    private void createTable(String table, boolean incorrectSchema) {
        com.google.cloud.bigquery.Schema schema = incorrectSchema ? com.google.cloud.bigquery.Schema.of(
                Field.of("f1", StandardSQLTypeName.STRING),
                Field.of("f2", StandardSQLTypeName.BOOL)
        ) : com.google.cloud.bigquery.Schema.of(
                Field.of("f1", StandardSQLTypeName.STRING),
                Field.of("f2", StandardSQLTypeName.BOOL),
                Field.of("f3", StandardSQLTypeName.FLOAT64)
        );
        try {
            BigQueryTestUtils.createPartitionedTable(bigQuery, dataset(), table, schema);
        } catch (BigQueryException ex) {
            if (!ex.getError().getReason().equalsIgnoreCase("duplicate"))
                throw new ConnectException("Failed to create table: ", ex);
            else
                logger.info("Table {} already exist", table);
        }
    }

    private void produceAvroRecords(String topic) {
        List<List<SchemaAndValue>> records = new ArrayList<>();

        // Prepare records
        for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
            List<SchemaAndValue> record = new ArrayList<>();
            SchemaAndValue schemaAndValue = new SchemaAndValue(valueSchema, data(i));
            SchemaAndValue keyschemaAndValue = new SchemaAndValue(keySchema, new Struct(keySchema).put("k1", (long) i));

            record.add(keyschemaAndValue);
            record.add(schemaAndValue);

            records.add(record);
        }

        // send prepared records
        schemaRegistry.produceRecordsWithKey(keyConverter, valueConverter, records, topic);
    }

    private void initialiseAvroConverters() {
        keyConverter = new AvroConverter();
        valueConverter = new AvroConverter();
        keyConverter.configure(Collections.singletonMap(
                        SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
                ), true
        );
        valueConverter.configure(Collections.singletonMap(
                        SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl
                ), false
        );
    }

    private void produceJsonRecords(String topic) {
        // Prepare records
        for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
            Map<String, Object> kafkaValue = new HashMap<>();
            kafkaValue.put("f1", "api" + i);
            kafkaValue.put("f2", i % 2 == 0);
            kafkaValue.put("f3", i * 0.01);
            connect.kafka().produce(
                    topic,
                    null,
                    new String(valueConverter.fromConnectData(topic, null, kafkaValue)));
        }
    }

    private void initialiseJsonConverters() {
        keyConverter = converter(true);
        valueConverter = converter(false);
    }

    private Converter converter(boolean isKey) {
        Map<String, Object> props = new HashMap<>();
        props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
        Converter result = new JsonConverter();
        result.configure(props, isKey);
        return result;
    }

    private Map<String, String> configs(String topic) {
        Map<String, String> result = baseConnectorProps(1);
        result.put(SinkConnectorConfig.TOPICS_CONFIG, topic);
        result.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
        result.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
        result.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");
        // use the Avro converter with schemas enabled
        result.put(KEY_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
        result.put(
                ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);
        result.put(VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
        result.put(
                ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);

        result.put(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG, KAFKA_FIELD_NAME);

        result.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
        return result;
    }

    private Struct data(long iteration) {
        return new Struct(valueSchema)
                .put("f1", "api" + iteration)
                .put("f2", iteration % 2 == 0)
                .put("f3", iteration * 0.01);
    }

    private Set<Object> expectedRows() {
        Set<Object> rows = new HashSet<>();
        for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
            rows.add("api" + i);
        }
        return rows;
    }
}
