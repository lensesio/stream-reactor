/*
 * Copyright 2017-2020 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.wepay.kafka.connect.bigquery.BigQuerySinkTask;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GCSToBQWriterTest {

  private static SinkPropertiesFactory propertiesFactory;

  @BeforeClass
  public static void initializePropertiesFactory() {
    propertiesFactory = new SinkPropertiesFactory();
  }

  @Test
  public void testGCSNoFailure() {
    // test succeeding on first attempt
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    expectTable(bigQuery);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(storage, times(1)).create(any(), (byte[]) any());
  }

  @Test
  public void testGCSSomeFailures() {
    // test failure through all configured retry attempts.
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    expectTable(bigQuery);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();

    when(storage.create((BlobInfo) any(), (byte[]) any()))
        .thenThrow(new StorageException(500, "internal server error")) // throw first time
        .thenReturn(null); // return second time. (we don't care about the result.)

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(storage, times(2)).create((BlobInfo) any(), (byte[]) any());
  }

  @Test
  public void testGCSAllFailures() {
    // test failure through all configured retry attempts.
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    expectTable(bigQuery);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();

    when(storage.create((BlobInfo) any(), (byte[]) any()))
        .thenThrow(new StorageException(500, "internal server error"));

    BigQuerySinkTask testTask = new BigQuerySinkTask(bigQuery, schemaRetriever, storage, schemaManager, cache);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    try {
      testTask.flush(Collections.emptyMap());
      Assert.fail("expected testTask.flush to fail.");
    } catch (ConnectException ex) {
      verify(storage, times(4)).create((BlobInfo) any(), (byte[]) any());
    }
  }

  private void expectTable(BigQuery mockBigQuery) {
    Table mockTable = mock(Table.class);
    when(mockBigQuery.getTable(any())).thenReturn(mockTable);
  }

  /**
   * Utility method for making and retrieving properties based on provided parameters.
   * 
   * @param bigqueryRetry     The number of retries.
   * @param bigqueryRetryWait The wait time for each retry.
   * @param topic             The topic of the record.
   * @param dataset           The dataset of the record.
   * @return The map of bigquery sink configurations.
   */
  private Map<String, String> makeProperties(String bigqueryRetry,
      String bigqueryRetryWait,
      String topic,
      String dataset) {
    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG, bigqueryRetry);
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG, bigqueryRetryWait);
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);
    properties.put(BigQuerySinkTaskConfig.TASK_ID_CONFIG, "9");
    // gcs config
    properties.put(BigQuerySinkConfig.ENABLE_BATCH_CONFIG, topic);
    properties.put(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG, "myBucket");
    return properties;
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   * 
   * @param topic     The topic of the record.
   * @param partition The partition of the record.
   * @param field     The name of the field in the record's struct.
   * @param value     The content of the field.
   * @return The spoofed SinkRecord.
   */
  private SinkRecord spoofSinkRecord(String topic,
      int partition,
      long kafkaOffset,
      String field,
      String value) {
    Schema basicRowSchema =
        SchemaBuilder
            .struct()
            .field(field, Schema.STRING_SCHEMA)
            .build();
    Struct basicRowValue = new Struct(basicRowSchema);
    basicRowValue.put(field, value);
    return new SinkRecord(topic,
        partition,
        null,
        null,
        basicRowSchema,
        basicRowValue,
        kafkaOffset,
        null,
        null);
  }

}
