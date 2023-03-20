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

package com.wepay.kafka.connect.bigquery.config;

import com.google.cloud.bigquery.Schema;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for connector and task configs; contains properties shared between the two of them.
 */
public class BigQuerySinkConfig extends AbstractConfig {
  // Values taken from https://github.com/apache/kafka/blob/1.1.1/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/SinkConnectorConfig.java#L33
  public static final String TOPICS_CONFIG =                     SinkConnector.TOPICS_CONFIG;
  private static final ConfigDef.Type TOPICS_TYPE =              ConfigDef.Type.LIST;
  private static final ConfigDef.Importance TOPICS_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String TOPICS_GROUP =                     "Common";
  private static final int TOPICS_ORDER_IN_GROUP =               4;
  private static final ConfigDef.Width TOPICS_WIDTH =            ConfigDef.Width.LONG;
  private static final String TOPICS_DOC =
      "List of topics to consume, separated by commas";
  public static final String TOPICS_DEFAULT = "";
  private static final String TOPICS_DISPLAY =                   "Topics";

  public static final String TOPICS_REGEX_CONFIG =                     "topics.regex";
  private static final ConfigDef.Type TOPICS_REGEX_TYPE =              ConfigDef.Type.STRING;
  private static final ConfigDef.Importance TOPICS_REGEX_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String TOPICS_REGEX_GROUP =                     "Common";
  private static final int TOPICS_REGEX_ORDER_IN_GROUP =               4;
  private static final ConfigDef.Width TOPICS_REGEX_WIDTH =            ConfigDef.Width.LONG;
  private static final String TOPICS_REGEX_DOC = "Regular expression giving topics to consume. " +
	  "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. " +
	  "Only one of " + TOPICS_CONFIG + " or " + TOPICS_REGEX_CONFIG + " should be specified.";
  public static final String TOPICS_REGEX_DEFAULT = "";
  private static final String TOPICS_REGEX_DISPLAY = "Topics regex";

  public static final String ENABLE_BATCH_CONFIG =                         "enableBatchLoad";
  private static final ConfigDef.Type ENABLE_BATCH_TYPE =                  ConfigDef.Type.LIST;
  private static final List<String> ENABLE_BATCH_DEFAULT =                 Collections.emptyList();
  private static final ConfigDef.Importance ENABLE_BATCH_IMPORTANCE =      ConfigDef.Importance.LOW;
  private static final String ENABLE_BATCH_DOC =
      "Beta Feature; use with caution: The sublist of topics to be batch loaded through GCS";

  public static final String BATCH_LOAD_INTERVAL_SEC_CONFIG =             "batchLoadIntervalSec";
  private static final ConfigDef.Type BATCH_LOAD_INTERVAL_SEC_TYPE =      ConfigDef.Type.INT;
  private static final Integer BATCH_LOAD_INTERVAL_SEC_DEFAULT =          120;
  private static final ConfigDef.Importance BATCH_LOAD_INTERVAL_SEC_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String BATCH_LOAD_INTERVAL_SEC_DOC =
      "The interval, in seconds, in which to attempt to run GCS to BQ load jobs. Only relevant "
      + "if enableBatchLoad is configured.";

  public static final String GCS_BUCKET_NAME_CONFIG =                     "gcsBucketName";
  private static final ConfigDef.Type GCS_BUCKET_NAME_TYPE =              ConfigDef.Type.STRING;
  private static final Object GCS_BUCKET_NAME_DEFAULT =                   "";
  private static final ConfigDef.Importance GCS_BUCKET_NAME_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String GCS_BUCKET_NAME_DOC =
      "The name of the bucket in which gcs blobs used to batch load to BigQuery "
      + "should be located. Only relevant if enableBatchLoad is configured.";

  public static final String GCS_FOLDER_NAME_CONFIG =                     "gcsFolderName";
  private static final ConfigDef.Type GCS_FOLDER_NAME_TYPE =              ConfigDef.Type.STRING;
  public static final String GCS_FOLDER_NAME_DEFAULT =                   "";
  private static final ConfigDef.Importance GCS_FOLDER_NAME_IMPORTANCE =  ConfigDef.Importance.MEDIUM;
  private static final String GCS_FOLDER_NAME_DOC =
          "The name of the folder under the bucket in which gcs blobs used to batch load to BigQuery "
                  + "should be located. Only relevant if enableBatchLoad is configured.";

  public static final String PROJECT_CONFIG =                     "project";
  private static final ConfigDef.Type PROJECT_TYPE =              ConfigDef.Type.STRING;
  private static final ConfigDef.Importance PROJECT_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String PROJECT_DOC =
      "The BigQuery project to write to";

  public static final String DEFAULT_DATASET_CONFIG =             "defaultDataset";
  private static final ConfigDef.Type DEFAULT_DATASET_TYPE =       ConfigDef.Type.STRING;
  private static final Object DEFAULT_DATASET_DEFAULT =             ConfigDef.NO_DEFAULT_VALUE;
  private static final ConfigDef.Importance DEFAULT_DATASET_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String DEFAULT_DATASET_DOC =                  "The default dataset to be used";

  public static final String SCHEMA_RETRIEVER_CONFIG =         "schemaRetriever";
  private static final ConfigDef.Type SCHEMA_RETRIEVER_TYPE =  ConfigDef.Type.CLASS;
  private static final Class<?> SCHEMA_RETRIEVER_DEFAULT = IdentitySchemaRetriever.class;
  private static final ConfigDef.Importance SCHEMA_RETRIEVER_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String SCHEMA_RETRIEVER_DOC =
      "A class that can be used for automatically creating tables and/or updating schemas";

  public static final String KEYFILE_CONFIG =                     "keyfile";
  private static final ConfigDef.Type KEYFILE_TYPE =              ConfigDef.Type.PASSWORD;
  public static final String KEYFILE_DEFAULT =                    null;
  private static final ConfigDef.Importance KEYFILE_IMPORTANCE =  ConfigDef.Importance.MEDIUM;
  private static final String KEYFILE_DOC =
      "The file containing a JSON key with BigQuery service account credentials";

  public static final String KEY_SOURCE_CONFIG =                "keySource";
  private static final ConfigDef.Type KEY_SOURCE_TYPE =         ConfigDef.Type.STRING;
  public static final String KEY_SOURCE_DEFAULT = GcpClientBuilder.KeySource.FILE.name();
  private static final ConfigDef.Validator KEY_SOURCE_VALIDATOR = ConfigDef.ValidString.in(
      Stream.of(GcpClientBuilder.KeySource.values())
          .map(GcpClientBuilder.KeySource::name)
          .collect(Collectors.toList())
          .toArray(new String[0])
  );
  private static final ConfigDef.Importance KEY_SOURCE_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String KEY_SOURCE_DOC =
          "Determines whether the keyfile config is the path to the credentials json, or the json itself";

  public static final String SANITIZE_TOPICS_CONFIG =                     "sanitizeTopics";
  private static final ConfigDef.Type SANITIZE_TOPICS_TYPE =              ConfigDef.Type.BOOLEAN;
  public static final Boolean SANITIZE_TOPICS_DEFAULT =                   false;
  private static final ConfigDef.Importance SANITIZE_TOPICS_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String SANITIZE_TOPICS_DOC =
      "Whether to automatically sanitize topic names before using them as table names;"
      + " if not enabled topic names will be used directly as table names";

  public static final String SANITIZE_FIELD_NAME_CONFIG =                     "sanitizeFieldNames";
  private static final ConfigDef.Type SANITIZE_FIELD_NAME_TYPE =              ConfigDef.Type.BOOLEAN;
  public static final Boolean SANITIZE_FIELD_NAME_DEFAULT =                   false;
  private static final ConfigDef.Importance SANITIZE_FIELD_NAME_IMPORTANCE =
          ConfigDef.Importance.MEDIUM;
  private static final String SANITIZE_FIELD_NAME_DOC =
          "Whether to automatically sanitize field names before using them as field names in big query. "
                  + "Big query specifies that field name can only contain letters, numbers, and "
                  + "underscores. The sanitizer will replace the invalid symbols with underscore. "
                  + "If the field name starts with a digit, the sanitizer will add an underscore in "
                  + "front of field name. Note: field a.b and a_b will have same value after sanitizing, "
                  + "and might cause key duplication error.";

  public static final String KAFKA_KEY_FIELD_NAME_CONFIG =        "kafkaKeyFieldName";
  private static final ConfigDef.Type KAFKA_KEY_FIELD_NAME_TYPE = ConfigDef.Type.STRING;
  public static final String KAFKA_KEY_FIELD_NAME_DEFAULT =       null;
  private static final ConfigDef.Validator KAFKA_KEY_FIELD_NAME_VALIDATOR = new ConfigDef.NonEmptyString();
  private static final ConfigDef.Importance KAFKA_KEY_FIELD_NAME_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String KAFKA_KEY_FIELD_NAME_DOC = "The name of the field of Kafka key. " +
          "Default to be null, which means Kafka Key Field will not be included.";

  public static final String KAFKA_DATA_FIELD_NAME_CONFIG =        "kafkaDataFieldName";
  private static final ConfigDef.Type KAFKA_DATA_FIELD_NAME_TYPE = ConfigDef.Type.STRING;
  public static final String KAFKA_DATA_FIELD_NAME_DEFAULT =       null;
  private static final ConfigDef.Validator KAFKA_DATA_FIELD_NAME_VALIDATOR = new ConfigDef.NonEmptyString();
  private static final ConfigDef.Importance KAFKA_DATA_FIELD_NAME_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String KAFKA_DATA_FIELD_NAME_DOC = "The name of the field of Kafka Data. " +
          "Default to be null, which means Kafka Data Field will not be included. ";

  public static final String AVRO_DATA_CACHE_SIZE_CONFIG =                 "avroDataCacheSize";
  private static final ConfigDef.Type AVRO_DATA_CACHE_SIZE_TYPE =          ConfigDef.Type.INT;
  public static final Integer AVRO_DATA_CACHE_SIZE_DEFAULT =               100;
  private static final ConfigDef.Validator AVRO_DATA_CACHE_SIZE_VALIDATOR =
      ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance AVRO_DATA_CACHE_SIZE_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String AVRO_DATA_CACHE_SIZE_DOC =
      "The size of the cache to use when converting schemas from Avro to Kafka Connect";

  public static final String CONVERT_DOUBLE_SPECIAL_VALUES_CONFIG =    "convertDoubleSpecialValues";
  public static final ConfigDef.Type CONVERT_DOUBLE_SPECIAL_VALUES_TYPE =   ConfigDef.Type.BOOLEAN;
  public static final Boolean CONVERT_DOUBLE_SPECIAL_VALUES_DEFAULT =       false;
  public static final ConfigDef.Importance CONVERT_DOUBLE_SPECIAL_VALUES_IMPORTANCE =
      ConfigDef.Importance.LOW;
  public static final String CONVERT_DOUBLE_SPECIAL_VALUES_DOC =
          "Should +Infinity be converted to Double.MAX_VALUE and -Infinity and NaN be "
          + "converted to Double.MIN_VALUE so they can make it to BigQuery";

  public static final String ALL_BQ_FIELDS_NULLABLE_CONFIG = "allBQFieldsNullable";
  private static final ConfigDef.Type ALL_BQ_FIELDS_NULLABLE_TYPE = ConfigDef.Type.BOOLEAN;
  private static final Boolean ALL_BQ_FIELDS_NULLABLE_DEFAULT = false;
  private static final ConfigDef.Importance ALL_BQ_FIELDS_NULLABLE_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String ALL_BQ_FIELDS_NULLABLE_DOC =
      "If true, no fields in any produced BigQuery schema will be REQUIRED. All "
      + "non-nullable avro fields will be translated as NULLABLE (or REPEATED, if arrays).";

  public static final String TABLE_CREATE_CONFIG =                     "autoCreateTables";
  private static final ConfigDef.Type TABLE_CREATE_TYPE =              ConfigDef.Type.BOOLEAN;
  public static final boolean TABLE_CREATE_DEFAULT =                   true;
  private static final ConfigDef.Importance TABLE_CREATE_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String TABLE_CREATE_DOC =
          "Automatically create BigQuery tables if they don't already exist";

  public static final String AUTO_CREATE_BUCKET_CONFIG =                    "autoCreateBucket";
  private static final ConfigDef.Type AUTO_CREATE_BUCKET_TYPE =             ConfigDef.Type.BOOLEAN;
  public static final Boolean AUTO_CREATE_BUCKET_DEFAULT =                  true;
  private static final ConfigDef.Importance AUTO_CREATE_BUCKET_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String AUTO_CREATE_BUCKET_DOC =
          "Whether to automatically create the given bucket, if it does not exist. " +
                  "Only relevant if enableBatchLoad is configured.";

  public static final String ALLOW_NEW_BIGQUERY_FIELDS_CONFIG =                    "allowNewBigQueryFields";
  private static final ConfigDef.Type ALLOW_NEW_BIGQUERY_FIELDS_TYPE =             ConfigDef.Type.BOOLEAN;
  public static final Boolean ALLOW_NEW_BIGQUERY_FIELDS_DEFAULT =                  false;
  private static final ConfigDef.Importance ALLOW_NEW_BIGQUERY_FIELDS_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String ALLOW_NEW_BIGQUERY_FIELDS_DOC =
          "If true, new fields can be added to BigQuery tables during subsequent schema updates";

  public static final String ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG =                    "allowBigQueryRequiredFieldRelaxation";
  private static final ConfigDef.Type ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_TYPE =             ConfigDef.Type.BOOLEAN;
  public static final Boolean ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_DEFAULT =                  false;
  private static final ConfigDef.Importance ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_DOC =
          "If true, fields in BigQuery Schema can be changed from REQUIRED to NULLABLE";

  public static final String ALLOW_SCHEMA_UNIONIZATION_CONFIG =                    "allowSchemaUnionization";
  private static final ConfigDef.Type ALLOW_SCHEMA_UNIONIZATION_TYPE =             ConfigDef.Type.BOOLEAN;
  public static final Boolean ALLOW_SCHEMA_UNIONIZATION_DEFAULT =                  false;
  private static final ConfigDef.Importance ALLOW_SCHEMA_UNIONIZATION_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String ALLOW_SCHEMA_UNIONIZATION_DOC =
          "If true, the existing table schema (if one is present) will be unionized with new "
              + "record schemas during schema updates";

  public static final String UPSERT_ENABLED_CONFIG =                    "upsertEnabled";
  private static final ConfigDef.Type UPSERT_ENABLED_TYPE =             ConfigDef.Type.BOOLEAN;
  public static final boolean UPSERT_ENABLED_DEFAULT =                  false;
  private static final ConfigDef.Importance UPSERT_ENABLED_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String UPSERT_ENABLED_DOC =
      "Enable upsert functionality on the connector through the use of record keys, intermediate "
      + "tables, and periodic merge flushes. Row-matching will be performed based on the contents " 
      + "of record keys.";

  public static final String DELETE_ENABLED_CONFIG =                    "deleteEnabled";
  private static final ConfigDef.Type DELETE_ENABLED_TYPE =             ConfigDef.Type.BOOLEAN;
  public static final boolean DELETE_ENABLED_DEFAULT =                  false;
  private static final ConfigDef.Importance DELETE_ENABLED_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String DELETE_ENABLED_DOC =
      "Enable delete functionality on the connector through the use of record keys, intermediate " 
      + "tables, and periodic merge flushes. A delete will be performed when a record with a null " 
      + "value (i.e., a tombstone record) is read.";

  public static final String INTERMEDIATE_TABLE_SUFFIX_CONFIG =                     "intermediateTableSuffix";
  private static final ConfigDef.Type INTERMEDIATE_TABLE_SUFFIX_TYPE =              ConfigDef.Type.STRING;
  public static final String INTERMEDIATE_TABLE_SUFFIX_DEFAULT =                    "tmp";
  private static final ConfigDef.Validator INTERMEDIATE_TABLE_SUFFIX_VALIDATOR =    new ConfigDef.NonEmptyString();
  private static final ConfigDef.Importance INTERMEDIATE_TABLE_SUFFIX_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String INTERMEDIATE_TABLE_SUFFIX_DOC =
      "A suffix that will be appended to the names of destination tables to create the names for " 
      + "the corresponding intermediate tables. Multiple intermediate tables may be created for a " 
      + "single destination table, but their names will always start with the name of the " 
      + "destination table, followed by this suffix, and possibly followed by an additional " 
      + "suffix.";

  public static final String MERGE_INTERVAL_MS_CONFIG =                    "mergeIntervalMs";
  private static final ConfigDef.Type MERGE_INTERVAL_MS_TYPE =              ConfigDef.Type.LONG;
  public static final long MERGE_INTERVAL_MS_DEFAULT =                     60_000L;
  private static final ConfigDef.Validator MERGE_INTERVAL_MS_VALIDATOR =   ConfigDef.LambdaValidator.with(
      (name, value) -> {
        if (value == null) {
          return;
        }
        long parsedValue = (long) ConfigDef.parseType(name, value, MERGE_INTERVAL_MS_TYPE);

        if (parsedValue == 0) {
          throw new ConfigException(name, value, "Cannot be zero");
        } else if (parsedValue < -1) {
          throw new ConfigException(name, value, "Cannot be less than -1");
        }
      },
      () -> "Either a positive integer or -1 to disable time interval-based merging"
  );
  private static final ConfigDef.Importance MERGE_INTERVAL_MS_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String MERGE_INTERVAL_MS_DOC =
      "How often (in milliseconds) to perform a merge flush, if upsert/delete is enabled. Can be "
      + "set to -1 to disable periodic flushing.";

  public static final String MERGE_RECORDS_THRESHOLD_CONFIG =                    "mergeRecordsThreshold";
  private static final ConfigDef.Type MERGE_RECORDS_THRESHOLD_TYPE =             ConfigDef.Type.LONG;
  public static final long MERGE_RECORDS_THRESHOLD_DEFAULT =                     -1;
  private static final ConfigDef.Validator MERGE_RECORDS_THRESHOLD_VALIDATOR =   ConfigDef.LambdaValidator.with(
      (name, value) -> {
        if (value == null) {
          return;
        }
        long parsedValue = (long) ConfigDef.parseType(name, value, MERGE_RECORDS_THRESHOLD_TYPE);

        if (parsedValue == 0) {
          throw new ConfigException(name, value, "Cannot be zero");
        } else if (parsedValue < -1) {
          throw new ConfigException(name, value, "Cannot be less than -1");
        }
      },
      () -> "Either a positive integer or -1 to disable throughput-based merging"
  );
  private static final ConfigDef.Importance MERGE_RECORDS_THRESHOLD_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String MERGE_RECORDS_THRESHOLD_DOC =
      "How many records to write to an intermediate table before performing a merge flush, if " 
      + "upsert/delete is enabled. Can be set to -1 to disable record count-based flushing.";

  public static final String THREAD_POOL_SIZE_CONFIG =                  "threadPoolSize";
  private static final ConfigDef.Type THREAD_POOL_SIZE_TYPE =           ConfigDef.Type.INT;
  public static final Integer THREAD_POOL_SIZE_DEFAULT =                10;
  private static final ConfigDef.Validator THREAD_POOL_SIZE_VALIDATOR = ConfigDef.Range.atLeast(1);
  private static final ConfigDef.Importance THREAD_POOL_SIZE_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String THREAD_POOL_SIZE_DOC =
      "The size of the BigQuery write thread pool. This establishes the maximum number of "
          + "concurrent writes to BigQuery.";

  public static final String QUEUE_SIZE_CONFIG =                    "queueSize";
  private static final ConfigDef.Type QUEUE_SIZE_TYPE =             ConfigDef.Type.LONG;
  // should this even have a default?
  public static final Long QUEUE_SIZE_DEFAULT =                     -1L;
  private static final ConfigDef.Validator QUEUE_SIZE_VALIDATOR =   ConfigDef.Range.atLeast(-1);
  private static final ConfigDef.Importance QUEUE_SIZE_IMPORTANCE = ConfigDef.Importance.HIGH;
  private static final String QUEUE_SIZE_DOC =
      "The maximum size (or -1 for no maximum size) of the worker queue for bigQuery write "
          + "requests before all topics are paused. This is a soft limit; the size of the queue can "
          + "go over this before topics are paused. All topics will be resumed once a flush is "
          + "requested or the size of the queue drops under half of the maximum size.";

  public static final String BIGQUERY_RETRY_CONFIG =                    "bigQueryRetry";
  private static final ConfigDef.Type BIGQUERY_RETRY_TYPE =             ConfigDef.Type.INT;
  public static final Integer BIGQUERY_RETRY_DEFAULT =                  0;
  private static final ConfigDef.Validator BIGQUERY_RETRY_VALIDATOR =   ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance BIGQUERY_RETRY_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String BIGQUERY_RETRY_DOC =
      "The number of retry attempts that will be made per BigQuery request that fails with a "
          + "backend error or a quota exceeded error";

  public static final String BIGQUERY_RETRY_WAIT_CONFIG =               "bigQueryRetryWait";
  private static final ConfigDef.Type BIGQUERY_RETRY_WAIT_CONFIG_TYPE = ConfigDef.Type.LONG;
  public static final Long BIGQUERY_RETRY_WAIT_DEFAULT =                1000L;
  private static final ConfigDef.Validator BIGQUERY_RETRY_WAIT_VALIDATOR =
      ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance BIGQUERY_RETRY_WAIT_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String BIGQUERY_RETRY_WAIT_DOC =
      "The minimum amount of time, in milliseconds, to wait between BigQuery backend or quota "
          +  "exceeded error retry attempts.";

  public static final String BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG =
      "bigQueryMessageTimePartitioning";
  private static final ConfigDef.Type BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG_TYPE =
      ConfigDef.Type.BOOLEAN;
  public static final Boolean BIGQUERY_MESSAGE_TIME_PARTITIONING_DEFAULT =                   false;
  private static final ConfigDef.Importance BIGQUERY_MESSAGE_TIME_PARTITIONING_IMPORTANCE =
      ConfigDef.Importance.HIGH;
  private static final String BIGQUERY_MESSAGE_TIME_PARTITIONING_DOC =
      "Whether or not to use the message time when inserting records. "
          + "Default uses the connector processing time.";

  public static final String BIGQUERY_PARTITION_DECORATOR_CONFIG =
      "bigQueryPartitionDecorator";
  private static final ConfigDef.Type BIGQUERY_PARTITION_DECORATOR_CONFIG_TYPE =
      ConfigDef.Type.BOOLEAN;
  //This has been set to true to preserve the existing behavior. However, we can set it to false if field based partitioning is used in BigQuery
  public static final Boolean BIGQUERY_PARTITION_DECORATOR_DEFAULT =                 true;
  private static final ConfigDef.Importance BIGQUERY_PARTITION_DECORATOR_IMPORTANCE =
      ConfigDef.Importance.HIGH;
  private static final String BIGQUERY_PARTITION_DECORATOR_DOC =
      "Whether or not to append partition decorator to BigQuery table name when inserting records. "
          + "Default is true. Setting this to true appends partition decorator to table name (e.g. table$yyyyMMdd depending on the configuration set for bigQueryPartitionDecorator). "
          + "Setting this to false bypasses the logic to append the partition decorator and uses raw table name for inserts.";

  public static final String BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG = "timestampPartitionFieldName";
  private static final ConfigDef.Type BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_TYPE = ConfigDef.Type.STRING;
  private static final String BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_DEFAULT = null;
  private static final ConfigDef.Validator BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_VALIDATOR = new ConfigDef.NonEmptyString();
  private static final ConfigDef.Importance BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_DOC =
      "The name of the field in the value that contains the timestamp to partition by in BigQuery"
          + " and enable timestamp partitioning for each table. Leave this configuration blank,"
          + " to enable ingestion time partitioning for each table.";

  public static final String BIGQUERY_CLUSTERING_FIELD_NAMES_CONFIG = "clusteringPartitionFieldNames";
  private static final ConfigDef.Type BIGQUERY_CLUSTERING_FIELD_NAMES_TYPE = ConfigDef.Type.LIST;
  private static final List<String> BIGQUERY_CLUSTERING_FIELD_NAMES_DEFAULT = null;
  private static final ConfigDef.Validator BIGQUERY_CLUSTERING_FIELD_NAMES_VALIDATOR = (name, value) -> {
    if (value == null) {
      return;
    }

    @SuppressWarnings("unchecked")
    List<String> parsedValue = (List<String>) value;
    if (parsedValue.size() > 4) {
      throw new ConfigException(name, value, "You may only specify up to four clustering field names.");
    }
  };
  private static final ConfigDef.Importance BIGQUERY_CLUSTERING_FIELD_NAMES_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String BIGQUERY_CLUSTERING_FIELD_NAMES_DOC =
      "List of fields on which data should be clustered by in BigQuery, separated by commas";

  public static final String CONVERT_DEBEZIUM_TIMESTAMP_TO_INTEGER_CONFIG = "convertDebeziumTimestampToInteger";
  private static final ConfigDef.Type CONVERT_DEBEZIUM_TIMESTAMP_TO_INTEGER_TYPE = ConfigDef.Type.BOOLEAN;
  private static final Boolean CONVERT_DEBEZIUM_TIMESTAMP_TO_INTEGER_DEFAULT = false;
  private static final ConfigDef.Importance CONVERT_DEBEZIUM_TIMESTAMP_TO_INTEGER_IMPORTANCE =
          ConfigDef.Importance.MEDIUM;

  /**
   * Return the ConfigDef object used to define this config's fields.
   *
   * @return The ConfigDef object used to define this config's fields.
   */
  public static ConfigDef getConfig() {
    return new ConfigDef()
        .define(
            TOPICS_CONFIG,
            TOPICS_TYPE,
            TOPICS_DEFAULT,
            TOPICS_IMPORTANCE,
            TOPICS_DOC,
            TOPICS_GROUP,
            TOPICS_ORDER_IN_GROUP,
            TOPICS_WIDTH,
            TOPICS_DISPLAY)
        .define(
            TOPICS_REGEX_CONFIG,
            TOPICS_REGEX_TYPE,
            TOPICS_REGEX_DEFAULT,
            TOPICS_REGEX_IMPORTANCE,
            TOPICS_REGEX_DOC,
            TOPICS_REGEX_GROUP,
            TOPICS_REGEX_ORDER_IN_GROUP,
            TOPICS_REGEX_WIDTH,
            TOPICS_REGEX_DISPLAY)
        .define(
            ENABLE_BATCH_CONFIG,
            ENABLE_BATCH_TYPE,
            ENABLE_BATCH_DEFAULT,
            ENABLE_BATCH_IMPORTANCE,
            ENABLE_BATCH_DOC
        ).define(
            BATCH_LOAD_INTERVAL_SEC_CONFIG,
            BATCH_LOAD_INTERVAL_SEC_TYPE,
            BATCH_LOAD_INTERVAL_SEC_DEFAULT,
            BATCH_LOAD_INTERVAL_SEC_IMPORTANCE,
            BATCH_LOAD_INTERVAL_SEC_DOC
        ).define(
            GCS_BUCKET_NAME_CONFIG,
            GCS_BUCKET_NAME_TYPE,
            GCS_BUCKET_NAME_DEFAULT,
            GCS_BUCKET_NAME_IMPORTANCE,
            GCS_BUCKET_NAME_DOC
        ).define(
            GCS_FOLDER_NAME_CONFIG,
            GCS_FOLDER_NAME_TYPE,
            GCS_FOLDER_NAME_DEFAULT,
            GCS_FOLDER_NAME_IMPORTANCE,
            GCS_FOLDER_NAME_DOC
        ).define(
            PROJECT_CONFIG,
            PROJECT_TYPE,
            PROJECT_IMPORTANCE,
            PROJECT_DOC
        ).define(
            DEFAULT_DATASET_CONFIG,
            DEFAULT_DATASET_TYPE,
            DEFAULT_DATASET_DEFAULT,
            DEFAULT_DATASET_IMPORTANCE,
            DEFAULT_DATASET_DOC
        ).define(
            SCHEMA_RETRIEVER_CONFIG,
            SCHEMA_RETRIEVER_TYPE,
            SCHEMA_RETRIEVER_DEFAULT,
            SCHEMA_RETRIEVER_IMPORTANCE,
            SCHEMA_RETRIEVER_DOC
        ).define(
            KEYFILE_CONFIG,
            KEYFILE_TYPE,
            KEYFILE_DEFAULT,
            KEYFILE_IMPORTANCE,
            KEYFILE_DOC
        ).define(
            KEY_SOURCE_CONFIG,
            KEY_SOURCE_TYPE,
            KEY_SOURCE_DEFAULT,
            KEY_SOURCE_VALIDATOR,
            KEY_SOURCE_IMPORTANCE,
            KEY_SOURCE_DOC
        ).define(
            SANITIZE_TOPICS_CONFIG,
            SANITIZE_TOPICS_TYPE,
            SANITIZE_TOPICS_DEFAULT,
            SANITIZE_TOPICS_IMPORTANCE,
            SANITIZE_TOPICS_DOC
        ).define(
            SANITIZE_FIELD_NAME_CONFIG,
            SANITIZE_FIELD_NAME_TYPE,
            SANITIZE_FIELD_NAME_DEFAULT,
            SANITIZE_FIELD_NAME_IMPORTANCE,
            SANITIZE_FIELD_NAME_DOC
        ).define(
            KAFKA_KEY_FIELD_NAME_CONFIG,
            KAFKA_KEY_FIELD_NAME_TYPE,
            KAFKA_KEY_FIELD_NAME_DEFAULT,
            KAFKA_KEY_FIELD_NAME_VALIDATOR,
            KAFKA_KEY_FIELD_NAME_IMPORTANCE,
            KAFKA_KEY_FIELD_NAME_DOC
        ).define(
            KAFKA_DATA_FIELD_NAME_CONFIG,
            KAFKA_DATA_FIELD_NAME_TYPE,
            KAFKA_DATA_FIELD_NAME_DEFAULT,
            KAFKA_DATA_FIELD_NAME_VALIDATOR,
            KAFKA_DATA_FIELD_NAME_IMPORTANCE,
            KAFKA_DATA_FIELD_NAME_DOC
        ).define(
            AVRO_DATA_CACHE_SIZE_CONFIG,
            AVRO_DATA_CACHE_SIZE_TYPE,
            AVRO_DATA_CACHE_SIZE_DEFAULT,
            AVRO_DATA_CACHE_SIZE_VALIDATOR,
            AVRO_DATA_CACHE_SIZE_IMPORTANCE,
            AVRO_DATA_CACHE_SIZE_DOC
        ).define(
            ALL_BQ_FIELDS_NULLABLE_CONFIG,
            ALL_BQ_FIELDS_NULLABLE_TYPE,
            ALL_BQ_FIELDS_NULLABLE_DEFAULT,
            ALL_BQ_FIELDS_NULLABLE_IMPORTANCE,
            ALL_BQ_FIELDS_NULLABLE_DOC
        ).define(
            CONVERT_DOUBLE_SPECIAL_VALUES_CONFIG,
            CONVERT_DOUBLE_SPECIAL_VALUES_TYPE,
            CONVERT_DOUBLE_SPECIAL_VALUES_DEFAULT,
            CONVERT_DOUBLE_SPECIAL_VALUES_IMPORTANCE,
            CONVERT_DOUBLE_SPECIAL_VALUES_DOC
         ).define(
            TABLE_CREATE_CONFIG,
            TABLE_CREATE_TYPE,
            TABLE_CREATE_DEFAULT,
            TABLE_CREATE_IMPORTANCE,
            TABLE_CREATE_DOC
        ).define(
            AUTO_CREATE_BUCKET_CONFIG,
            AUTO_CREATE_BUCKET_TYPE,
            AUTO_CREATE_BUCKET_DEFAULT,
            AUTO_CREATE_BUCKET_IMPORTANCE,
            AUTO_CREATE_BUCKET_DOC
        ).define(
            ALLOW_NEW_BIGQUERY_FIELDS_CONFIG,
            ALLOW_NEW_BIGQUERY_FIELDS_TYPE,
            ALLOW_NEW_BIGQUERY_FIELDS_DEFAULT,
            ALLOW_NEW_BIGQUERY_FIELDS_IMPORTANCE,
            ALLOW_NEW_BIGQUERY_FIELDS_DOC
        ).define(
            ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG,
            ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_TYPE,
            ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_DEFAULT,
            ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_IMPORTANCE,
            ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_DOC
        ).define(
            ALLOW_SCHEMA_UNIONIZATION_CONFIG,
            ALLOW_SCHEMA_UNIONIZATION_TYPE,
            ALLOW_SCHEMA_UNIONIZATION_DEFAULT,
            ALLOW_SCHEMA_UNIONIZATION_IMPORTANCE,
            ALLOW_SCHEMA_UNIONIZATION_DOC
        ).define(
            UPSERT_ENABLED_CONFIG,
            UPSERT_ENABLED_TYPE,
            UPSERT_ENABLED_DEFAULT,
            UPSERT_ENABLED_IMPORTANCE,
            UPSERT_ENABLED_DOC
        ).define(
            DELETE_ENABLED_CONFIG,
            DELETE_ENABLED_TYPE,
            DELETE_ENABLED_DEFAULT,
            DELETE_ENABLED_IMPORTANCE,
            DELETE_ENABLED_DOC
        ).define(
            INTERMEDIATE_TABLE_SUFFIX_CONFIG,
            INTERMEDIATE_TABLE_SUFFIX_TYPE,
            INTERMEDIATE_TABLE_SUFFIX_DEFAULT,
            INTERMEDIATE_TABLE_SUFFIX_VALIDATOR,
            INTERMEDIATE_TABLE_SUFFIX_IMPORTANCE,
            INTERMEDIATE_TABLE_SUFFIX_DOC
        ).define(
            MERGE_INTERVAL_MS_CONFIG,
            MERGE_INTERVAL_MS_TYPE,
            MERGE_INTERVAL_MS_DEFAULT,
            MERGE_INTERVAL_MS_VALIDATOR,
            MERGE_INTERVAL_MS_IMPORTANCE,
            MERGE_INTERVAL_MS_DOC
        ).define(
            MERGE_RECORDS_THRESHOLD_CONFIG,
            MERGE_RECORDS_THRESHOLD_TYPE,
            MERGE_RECORDS_THRESHOLD_DEFAULT,
            MERGE_RECORDS_THRESHOLD_VALIDATOR,
            MERGE_RECORDS_THRESHOLD_IMPORTANCE,
            MERGE_RECORDS_THRESHOLD_DOC
        ).define(
            THREAD_POOL_SIZE_CONFIG,
            THREAD_POOL_SIZE_TYPE,
            THREAD_POOL_SIZE_DEFAULT,
            THREAD_POOL_SIZE_VALIDATOR,
            THREAD_POOL_SIZE_IMPORTANCE,
            THREAD_POOL_SIZE_DOC
        ).define(
            QUEUE_SIZE_CONFIG,
            QUEUE_SIZE_TYPE,
            QUEUE_SIZE_DEFAULT,
            QUEUE_SIZE_VALIDATOR,
            QUEUE_SIZE_IMPORTANCE,
            QUEUE_SIZE_DOC
        ).define(
            BIGQUERY_RETRY_CONFIG,
            BIGQUERY_RETRY_TYPE,
            BIGQUERY_RETRY_DEFAULT,
            BIGQUERY_RETRY_VALIDATOR,
            BIGQUERY_RETRY_IMPORTANCE,
            BIGQUERY_RETRY_DOC
        ).define(
            BIGQUERY_RETRY_WAIT_CONFIG,
            BIGQUERY_RETRY_WAIT_CONFIG_TYPE,
            BIGQUERY_RETRY_WAIT_DEFAULT,
            BIGQUERY_RETRY_WAIT_VALIDATOR,
            BIGQUERY_RETRY_WAIT_IMPORTANCE,
            BIGQUERY_RETRY_WAIT_DOC
        ).define(
            BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG,
            BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG_TYPE,
            BIGQUERY_MESSAGE_TIME_PARTITIONING_DEFAULT,
            BIGQUERY_MESSAGE_TIME_PARTITIONING_IMPORTANCE,
            BIGQUERY_MESSAGE_TIME_PARTITIONING_DOC
        ).define(
            BIGQUERY_PARTITION_DECORATOR_CONFIG,
            BIGQUERY_PARTITION_DECORATOR_CONFIG_TYPE,
            BIGQUERY_PARTITION_DECORATOR_DEFAULT,
            BIGQUERY_PARTITION_DECORATOR_IMPORTANCE,
            BIGQUERY_PARTITION_DECORATOR_DOC
        ).define(
            BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG,
            BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_TYPE,
            BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_DEFAULT,
            BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_VALIDATOR,
            BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_IMPORTANCE,
            BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_DOC
        ).define(
            BIGQUERY_CLUSTERING_FIELD_NAMES_CONFIG,
            BIGQUERY_CLUSTERING_FIELD_NAMES_TYPE,
            BIGQUERY_CLUSTERING_FIELD_NAMES_DEFAULT,
            BIGQUERY_CLUSTERING_FIELD_NAMES_VALIDATOR,
            BIGQUERY_CLUSTERING_FIELD_NAMES_IMPORTANCE,
            BIGQUERY_CLUSTERING_FIELD_NAMES_DOC
        ).defineInternal(
            CONVERT_DEBEZIUM_TIMESTAMP_TO_INTEGER_CONFIG,
            CONVERT_DEBEZIUM_TIMESTAMP_TO_INTEGER_TYPE,
            CONVERT_DEBEZIUM_TIMESTAMP_TO_INTEGER_DEFAULT,
            CONVERT_DEBEZIUM_TIMESTAMP_TO_INTEGER_IMPORTANCE
        );
  }

  private static final List<MultiPropertyValidator<BigQuerySinkConfig>> MULTI_PROPERTY_VALIDATIONS = new ArrayList<>();

  static {
    // Note that order matters here: validations are performed in the order they're added to this list, and if a
    // property or any of the properties that it depends on has an error, validation for it gets skipped.
    // This comes in handy for things like checking for the existence of tables, which requires valid BigQuery
    // credentials. We validate those credentials before checking for tables so that we can safely assume while
    // checking for those tables that the credentials are already valid.
    MULTI_PROPERTY_VALIDATIONS.add(new CredentialsValidator.BigQueryCredentialsValidator());
    MULTI_PROPERTY_VALIDATIONS.add(new CredentialsValidator.GcsCredentialsValidator());
    MULTI_PROPERTY_VALIDATIONS.add(new GcsBucketValidator());
    MULTI_PROPERTY_VALIDATIONS.add(new PartitioningModeValidator());
    MULTI_PROPERTY_VALIDATIONS.add(new UpsertDeleteValidator.UpsertValidator());
    MULTI_PROPERTY_VALIDATIONS.add(new UpsertDeleteValidator.DeleteValidator());
  }

  /**
   * Used in conjunction with {@link com.wepay.kafka.connect.bigquery.BigQuerySinkConnector#validate(Map)} to perform
   * preflight configuration checks. Simple validations that only require a single property value at a time (such as
   * ensuring that boolean properties only contain true/false values, or that values for required properties are
   * provided) are handled automatically by the {@link #getConfig() ConfigDef} for this class and optionally-defined
   * custom {@link ConfigDef.Validator validators}. Other, more sophisticated validations that require multiple
   * property values at a time (such as checking if all of the tables the connector will write to already exist if
   * automatic table creation is disabled) are performed manually in a subsequent step.
   *
   * @return a {@link Config} object containing all errors that the connector was able to detect during preflight
   * validation of this configuration; never null
   */
  public Config validate() {
    List<ConfigValue> initialValidation = getConfig().validate(originalsStrings());
    Map<String, ConfigValue> valuesByName = initialValidation
        .stream()
        .collect(Collectors.toMap(ConfigValue::name, Function.identity()));
    MULTI_PROPERTY_VALIDATIONS.forEach(validator -> {
      ConfigValue value = valuesByName.get(validator.propertyName());
      validator.validate(value, this, valuesByName).ifPresent(value::addErrorMessage);
    });
    return new Config(initialValidation);
  }

  /**
   * @return the key, which is (depending on the key source property) either a path to a file or a raw JSON string
   */
  public String getKey() {
    return Optional.ofNullable(getPassword(KEYFILE_CONFIG)).map(Password::value).orElse(null);
  }

  /**
   * @return the {@link com.wepay.kafka.connect.bigquery.GcpClientBuilder.KeySource key source type} that dictates how
   * the {@link #getKey()} should be be interpreted
   */
  public GcpClientBuilder.KeySource getKeySource() {
    String rawKeySource = getString(KEY_SOURCE_CONFIG);
    try {
      return GcpClientBuilder.KeySource.valueOf(rawKeySource);
    } catch (IllegalArgumentException e) {
      // Should never happen with preflight validation of the key source property
      throw new ConnectException("Invalid key source type: " + rawKeySource);
    }
  }

  public static boolean upsertDeleteEnabled(Map<String, String> props) {
    String upsertStr = props.get(UPSERT_ENABLED_CONFIG);
    String deleteStr = props.get(DELETE_ENABLED_CONFIG);
    return Boolean.TRUE.toString().equalsIgnoreCase(upsertStr)
        || Boolean.TRUE.toString().equalsIgnoreCase(deleteStr);
  }

  public static boolean gcsBatchLoadingEnabled(Map<String, String> props) {
    String batchLoadStr = props.get(ENABLE_BATCH_CONFIG);
    return batchLoadStr != null && !batchLoadStr.isEmpty();
  }

  /**
   * Returns the keyfile
   */
  public String getKeyFile() {
    return Optional.ofNullable(getPassword(KEYFILE_CONFIG)).map(Password::value).orElse(null);
  }

  /**
   * Return a new instance of the configured Schema Converter.
   * @return a {@link SchemaConverter} for BigQuery.
   */
  public SchemaConverter<Schema> getSchemaConverter() {
    return new BigQuerySchemaConverter(
        getBoolean(ALL_BQ_FIELDS_NULLABLE_CONFIG),
        getBoolean(SANITIZE_FIELD_NAME_CONFIG));
  }

  /**
   * Return a new instance of the configured Record Converter.
   * @return a {@link RecordConverter} for BigQuery.
   */
  public RecordConverter<Map<String, Object>> getRecordConverter() {
    return new BigQueryRecordConverter(getBoolean(CONVERT_DOUBLE_SPECIAL_VALUES_CONFIG), getBoolean(CONVERT_DEBEZIUM_TIMESTAMP_TO_INTEGER_CONFIG));
  }

  /**
   * Locate the class specified by the user for use in retrieving schemas, and instantiate it.
   * @return A new instance of the user's specified SchemaRetriever class.
   * @throws ConfigException If the user did not specify a SchemaRetriever class.
   * @throws ConfigException If the specified class does not implement the SchemaRetriever
   *                         interface.
   * @throws ConfigException If the specified class does not have a no-args constructor.
   */
  public SchemaRetriever getSchemaRetriever() {
    Class<?> userSpecifiedClass = getClass(SCHEMA_RETRIEVER_CONFIG);

    if (userSpecifiedClass == null) {
      throw new ConfigException(
          "Cannot request new instance of SchemaRetriever when class has not been specified"
      );
    }

    if (!SchemaRetriever.class.isAssignableFrom(userSpecifiedClass)) {
      throw new ConfigException(
          "Class specified for " + SCHEMA_RETRIEVER_CONFIG
              + " property does not implement " + SchemaRetriever.class.getName()
              + " interface"
      );
    }

    Class<? extends SchemaRetriever> schemaRetrieverClass =
        userSpecifiedClass.asSubclass(SchemaRetriever.class);

    Constructor<? extends SchemaRetriever> schemaRetrieverConstructor;
    try {
      schemaRetrieverConstructor = schemaRetrieverClass.getConstructor();
    } catch (NoSuchMethodException nsme) {
      throw new ConfigException(
          "Class specified for SchemaRetriever must have a no-args constructor",
          nsme
      );
    }

    SchemaRetriever schemaRetriever;
    try {
      schemaRetriever = schemaRetrieverConstructor.newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        exception) {
      throw new ConfigException(
          "Failed to instantiate class specified for SchemaRetriever",
          exception
      );
    }

    schemaRetriever.configure(originalsStrings());

    return schemaRetriever;
  }

  /**
   * If the connector is configured to load Kafka data into BigQuery, this config defines
   * the name of the kafka data field. A structure is created under the field name to contain
   * kafka data schema including topic, offset, partition and insertTime.
   *
   * @return Field name of Kafka Data to be used in BigQuery
   */
  public Optional<String> getKafkaKeyFieldName() {
    return Optional.ofNullable(getString(KAFKA_KEY_FIELD_NAME_CONFIG));
  }

  /**
   * If the connector is configured to load Kafka keys into BigQuery, this config defines
   * the name of the kafka key field. A structure is created under the field name to contain
   * a topic's Kafka key schema.
   *
   * @return Field name of Kafka Key to be used in BigQuery
   */
  public Optional<String> getKafkaDataFieldName() {
    return Optional.ofNullable(getString(KAFKA_DATA_FIELD_NAME_CONFIG));
  }

  public boolean isUpsertDeleteEnabled() {
    return getBoolean(UPSERT_ENABLED_CONFIG) || getBoolean(DELETE_ENABLED_CONFIG);
  }

  /**
   * Returns the field name to use for timestamp partitioning.
   * @return String that represents the field name.
   */
  public Optional<String> getTimestampPartitionFieldName() {
    return Optional.ofNullable(getString(BIGQUERY_TIMESTAMP_PARTITION_FIELD_NAME_CONFIG));
  }

  /**
   * Returns the field names to use for clustering.
   * @return List of Strings that represent the field names.
   */
  public Optional<List<String>> getClusteringPartitionFieldNames() {
    return Optional
        .ofNullable(getList(BIGQUERY_CLUSTERING_FIELD_NAMES_CONFIG))
        // With Java 11 there's Predicate::not, but for now we have to just manually invert the isEmpty check
        .filter(l -> !l.isEmpty());
  }

  protected BigQuerySinkConfig(ConfigDef config, Map<String, String> properties) {
    super(config, properties);
  }

  public BigQuerySinkConfig(Map<String, String> properties) {
    this(getConfig(), properties);
  }
}
