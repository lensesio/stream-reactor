package com.wepay.kafka.connect.bigquery.integration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.wepay.kafka.connect.bigquery.integration.utils.BigQueryTestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;

public class BigQueryErrantRecordHandlerIT extends BaseConnectorIT {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryErrantRecordHandlerIT.class);
  private static final String CONNECTOR_NAME = "kcbq-sink-connector";
  private static final long NUM_RECORDS_PRODUCED = 20;

  private BigQuery bigQuery;

  @Before
  public void setup() {
    bigQuery = newBigQuery();
    startConnect();
  }

  @After
  public void close() {
    bigQuery = null;
    stopConnect();
  }

  @Test
  public void testRecordsSentToDlqOnInvalidReason() throws InterruptedException {
    final String topic = suffixedTableOrTopic("test-dlq-feature");
    final String dlqTopic = "dlq_topic";
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, 1);


    final String table = sanitizedTable(topic);
    // Create table schema
    Schema schema = Schema.of(
        Field.of("f1", StandardSQLTypeName.STRING),
        Field.of("f2", StandardSQLTypeName.BOOL),
        Field.of("f3", StandardSQLTypeName.INT64)
    );

    // Try to create BigQuery table
    try {
      BigQueryTestUtils.createPartitionedTable(bigQuery, dataset(), table, schema);
    } catch (BigQueryException ex) {
      if (!ex.getError().getReason().equalsIgnoreCase("duplicate"))
        throw new ConnectException("Failed to create table: ", ex);
      else
        logger.info("Table {} already exist", table);
    }

    Map<String, String> props = connectorProps(topic, dlqTopic);

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Instantiate the converters we'll use to send records to the connector
    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Send Invalid records to BigQuery
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      String kafkaKey = key(keyConverter, topic, i);
      String kafkaValue = value(valueConverter, topic, i);
      logger.debug("Sending message with key '{}' and value '{}' to topic '{}'", kafkaKey, kafkaValue, topic);
      connect.kafka().produce(topic, kafkaKey, kafkaValue);
    }

    // Check records show up in dlq topic
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        (int) NUM_RECORDS_PRODUCED,
        Duration.ofSeconds(120).toMillis(), dlqTopic);

    Assert.assertEquals(NUM_RECORDS_PRODUCED, records.count());
  }

  @Test
  public void testRecordsSentToDlqOnRecordConversionError() throws InterruptedException {
    final String topic = suffixedTableOrTopic("test-dlq-feature");
    final String dlqTopic = "dlq_topic";
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, 1);

    Map<String, String> props = connectorProps(topic, dlqTopic);
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put("key.converter.schemas.enable", "false");
    props.put("value.converter.schemas.enable", "false");

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Send Invalid records to Kafka
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      String kafkaKey = "key-" + i;
      String kafkaValue = "\"f1\":1";
      logger.debug("Sending message with key '{}' and value '{}' to topic '{}'", kafkaKey, kafkaValue, topic);
      connect.kafka().produce(topic, kafkaKey, kafkaValue);
    }

    // Check records show up in dlq topic
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        (int) NUM_RECORDS_PRODUCED,
        Duration.ofSeconds(120).toMillis(), dlqTopic);

    Assert.assertEquals(NUM_RECORDS_PRODUCED, records.count());
  }

  private Map<String, String> connectorProps(String topicName, String dlqTopicName) {
    Map<String, String> result = baseConnectorProps(1);
    result.put(SinkConnectorConfig.TOPICS_CONFIG, topicName);

    // use the JSON converter with schemas enabled
    result.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    result.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    // DLQ Error Handler Configs
    result.put(SinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true");
    result.put(SinkConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
    result.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, dlqTopicName);
    result.put(SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    result.put(SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");

    return result;
  }

  private Converter converter(boolean isKey) {
    Map<String, Object> props = new HashMap<>();
    props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
    Converter result = new JsonConverter();
    result.configure(props, isKey);
    return result;
  }

  private String key(Converter converter, String topic, long iteration) {
    final org.apache.kafka.connect.data.Schema schema = SchemaBuilder.struct()
        .field("k1", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .build();

    final Struct struct = new Struct(schema)
        .put("k1", iteration);

    return new String(converter.fromConnectData(topic, schema, struct));
  }

  private String value(Converter converter, String topic, int iteration) {
    final org.apache.kafka.connect.data.Schema schema = SchemaBuilder.struct()
        .optional()
        .field("f1", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .field("f2", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
        .field("f3", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
        .build();

    final Struct struct = new Struct(schema)
        .put("f1", iteration % 2 == 0 ? "a string" : "another string")
        .put("f2", iteration % 3 == 0)
        .put("f3", "invalid value according to table schema");

    return new String(converter.fromConnectData(topic, schema, struct));
  }
}
