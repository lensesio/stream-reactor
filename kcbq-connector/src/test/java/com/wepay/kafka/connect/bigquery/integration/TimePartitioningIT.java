/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.integration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.TimePartitioning;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.TIME_PARTITIONING_TYPE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class TimePartitioningIT {

  private static final Logger logger = LoggerFactory.getLogger(TimePartitioningIT.class);

  private static final long NUM_RECORDS_PRODUCED = 20;
  private static final int TASKS_MAX = 1;

  private static BaseConnectorIT testBase;

  private BigQuery bigQuery;

  private final TimePartitioning.Type partitioningType;
  private final boolean usePartitionDecorator;
  private final boolean messageTimePartitioning;
  private final int testCase;
  private final long testStartTime;
  private final String connectorName;

  public TimePartitioningIT(
      TimePartitioning.Type partitioningType,
      boolean usePartitionDecorator,
      boolean messageTimePartitioning,
      int testCase
  ) {
    this.partitioningType = partitioningType;
    this.usePartitionDecorator = usePartitionDecorator;
    this.messageTimePartitioning = messageTimePartitioning;
    this.testCase = testCase;
    this.testStartTime = System.currentTimeMillis();
    this.connectorName = "kcbq-time-partitioning-test-" + testCase;
  }

  @Parameterized.Parameters(name = "{index}: partitioningType: {0}, usePartitionDecorator: {1}, messageTimePartitioning: {2}")
  public static Iterable<Object[]> data() {
    int testCase = 0;
    return Arrays.asList(
      new Object[] {TimePartitioning.Type.HOUR, false, false, testCase++ },
      new Object[] {TimePartitioning.Type.DAY, true, true, testCase++ },
      new Object[] {TimePartitioning.Type.DAY, true, false, testCase++ },
      new Object[] {TimePartitioning.Type.DAY, false, false, testCase++ },
      new Object[] {TimePartitioning.Type.MONTH, false, false, testCase++ },
      new Object[] {TimePartitioning.Type.YEAR, false, false, testCase }
    );
  }

  @BeforeClass
  public static void globalSetup() {
    testBase = new BaseConnectorIT() {};
    BigQuery bigQuery = testBase.newBigQuery();
    data().forEach(args -> {
      int testCase = (int) args[3];
      TableClearer.clearTables(bigQuery, testBase.dataset(), table(testCase));
    });
    testBase.startConnect();
  }

  @Before
  public void setup() {
    bigQuery = testBase.newBigQuery();
  }

  @After
  public void close() {
    bigQuery = null;
    testBase.connect.deleteConnector(connectorName);
  }

  @AfterClass
  public static void globalCleanup() {
    testBase.stopConnect();
  }

  private Map<String, String> partitioningProps() {
    Map<String, String> result = new HashMap<>();

    // use the JSON converter with schemas enabled
    result.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    result.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    result.put(BIGQUERY_PARTITION_DECORATOR_CONFIG, Boolean.toString(usePartitionDecorator));
    result.put(BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG, Boolean.toString(messageTimePartitioning));
    result.put(TIME_PARTITIONING_TYPE_CONFIG, partitioningType.name());

    return result;
  }

  private static String table(int testCase) {
    return testBase.suffixedAndSanitizedTable("test-time-partitioning-" + testCase);
  }

  @Test
  public void testTimePartitioning() throws Throwable {
    // create topic in Kafka
    final String topic = testBase.suffixedTableOrTopic("test-time-partitioning-" + testCase);
    testBase.connect.kafka().createTopic(topic);

    // setup props for the sink connector
    Map<String, String> props = testBase.baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");

    props.putAll(partitioningProps());

    // start a sink connector
    testBase.connect.configureConnector(connectorName, props);

    // wait for tasks to spin up
    testBase.waitForConnectorToStart(connectorName, TASKS_MAX);

    // Instantiate the converter we'll use to send records to the connector
    Converter valueConverter = converter();

    // Instantiate the producer we'll use to write records to Kafka
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testBase.connect.kafka().bootstrapServers());
    Producer<Void, String> valueProducer = new KafkaProducer<>(
        producerProps, Serdes.Void().serializer(), Serdes.String().serializer()
    );

    // Send records to Kafka
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      String kafkaValue = value(valueConverter, topic, i);
      logger.debug("Sending message with value '{}' to topic '{}'", kafkaValue, topic);
      ProducerRecord<Void, String> kafkaRecord = new ProducerRecord<>(topic, null, timestamp((i % 3) - 1), null, kafkaValue);
      try {
        valueProducer.send(kafkaRecord).get(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new ConnectException("Failed to produce data to embedded Kafka cluster", e);
      }
    }

    // wait for tasks to write to BigQuery and commit offsets for their records
    testBase.waitForCommittedRecords(
        connectorName,
        Collections.singleton(topic),
        NUM_RECORDS_PRODUCED,
        TASKS_MAX,
        TimeUnit.MINUTES.toMillis(3)
    );

    String table = table(testCase);

    // Might fail to read from the table for a little bit; keep retrying until it's available
    waitForCondition(
        () -> {
          try {
            testBase.readAllRows(bigQuery, table, "i");
            return true;
          } catch (RuntimeException e) {
            logger.debug("Failed to read rows from table {}", table, e);
            return false;
          }
        },
        TimeUnit.MINUTES.toMillis(5),
        "Could not read from table to verify data after connector committed offsets for the expected number of records"
    );

    List<List<Object>> allRows = testBase.readAllRows(bigQuery, table, "i");
    // Just check to make sure we sent the expected number of rows to the table
    assertEquals(NUM_RECORDS_PRODUCED, allRows.size());

    // Ensure that the table was created with the expected time partitioning type
    StandardTableDefinition tableDefinition = bigQuery.getTable(TableId.of(testBase.dataset(), table)).getDefinition();
    Optional<TimePartitioning.Type> actualPartitioningType = Optional.ofNullable((tableDefinition).getTimePartitioning())
        .map(TimePartitioning::getType);
    assertEquals(Optional.of(partitioningType), actualPartitioningType);

    // Verify that at least one record landed in each of the targeted partitions
    if (usePartitionDecorator && messageTimePartitioning) {
      for (int i = -1; i < 2; i++) {
        long partitionTime = timestamp(i);
        TableResult tableResult = bigQuery.query(QueryJobConfiguration.of(String.format(
            "SELECT * FROM `%s`.`%s` WHERE _PARTITIONTIME = TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(%d), %s)",
            testBase.dataset(),
            table,
            partitionTime,
            partitioningType.toString()
        )));
        assertTrue(
            "Should have seen at least one row in partition corresponding to timestamp " + partitionTime,
            tableResult.getValues().iterator().hasNext()
        );
      }
    }
  }

  private Converter converter() {
    Map<String, Object> props = new HashMap<>();
    props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
    Converter result = new JsonConverter();
    result.configure(props, false);
    return result;
  }

  private String value(Converter converter, String topic, long iteration) {
    final Schema schema = SchemaBuilder.struct()
        .optional()
        .field("i", Schema.INT64_SCHEMA)
        .field("f1", Schema.STRING_SCHEMA)
        .field("f2", Schema.BOOLEAN_SCHEMA)
        .field("f3", Schema.FLOAT64_SCHEMA)
        .build();

    final Struct struct = new Struct(schema)
        .put("i", iteration)
        .put("f1", iteration % 2 == 0 ? "a string" : "another string")
        .put("f2", iteration % 3 == 0)
        .put("f3", iteration / 39.80);

    return new String(converter.fromConnectData(topic, schema, struct));
  }

  /**
   * @param shiftAmount how many partitions forward/backward to shift the timestamp by,
   *                    relative to the partition corresponding to the start of the test
   */
  private long timestamp(long shiftAmount) {
    long partitionDelta;
    switch (partitioningType) {
      case HOUR:
        partitionDelta = TimeUnit.HOURS.toMillis(1);
        break;
      case DAY:
        partitionDelta = TimeUnit.DAYS.toMillis(1);
        break;
      case MONTH:
        partitionDelta = TimeUnit.DAYS.toMillis(31);
        break;
      case YEAR:
        partitionDelta = TimeUnit.DAYS.toMillis(366);
        break;
      default:
        throw new ConnectException("Unexpected partitioning type: " + partitioningType);
    }

    return testStartTime + (shiftAmount * partitionDelta);
  }
}
