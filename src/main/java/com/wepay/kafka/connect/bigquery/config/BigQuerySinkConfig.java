package com.wepay.kafka.connect.bigquery.config;

/*
 * Copyright 2016 WePay, Inc.
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


import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.kafkadata.KafkaDataBQRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.kafkadata.KafkaDataBQSchemaConverter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.apache.kafka.connect.sink.SinkConnector;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for connector and task configs; contains properties shared between the two of them.
 */
public class BigQuerySinkConfig extends AbstractConfig {
  private static final ConfigDef config;
  private static final Validator validator = new Validator();

  public static final String TOPICS_CONFIG =                     SinkConnector.TOPICS_CONFIG;
  private static final ConfigDef.Type TOPICS_TYPE =              ConfigDef.Type.LIST;
  private static final ConfigDef.Importance TOPICS_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String TOPICS_DOC =
      "A list of Kafka topics to read from";

  public static final String PROJECT_CONFIG =                     "project";
  private static final ConfigDef.Type PROJECT_TYPE =              ConfigDef.Type.STRING;
  private static final ConfigDef.Importance PROJECT_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String PROJECT_DOC =
      "The BigQuery project to write to";

  public static final String DATASETS_CONFIG =                     "datasets";
  private static final ConfigDef.Type DATASETS_TYPE =              ConfigDef.Type.LIST;
  private static final Object DATASETS_DEFAULT =                   ConfigDef.NO_DEFAULT_VALUE;
  private static final ConfigDef.Validator DATASETS_VALIDATOR =    validator;
  private static final ConfigDef.Importance DATASETS_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String DATASETS_DOC =
      "Names for the datasets kafka topics will write to "
      + "(form of <topic regex>=<dataset>)";

  public static final String KEYFILE_CONFIG =                     "keyfile";
  private static final ConfigDef.Type KEYFILE_TYPE =              ConfigDef.Type.STRING;
  public static final String KEYFILE_DEFAULT =                    null;
  private static final ConfigDef.Importance KEYFILE_IMPORTANCE =  ConfigDef.Importance.MEDIUM;
  private static final String KEYFILE_DOC =
      "The file containing a JSON key with BigQuery service account credentials";

  public static final String REGISTRY_CONFIG =                     "registry";
  private static final ConfigDef.Type REGISTRY_TYPE =              ConfigDef.Type.STRING;
  public static final String REGISTRY_DEFAULT =                    null;
  private static final ConfigDef.Importance REGISTRY_IMPORTANCE =  ConfigDef.Importance.MEDIUM;
  private static final String REGISTRY_DOC =
      "The base URL of a schema registry";

  public static final String MAX_WRITE_CONFIG =                     "maxWriteSize";
  private static final ConfigDef.Type MAX_WRITE_TYPE =              ConfigDef.Type.INT;
  public static final Integer MAX_WRITE_DEFAULT =                   100000;
  private static final ConfigDef.Validator MAX_WRITE_VALIDATOR =    ConfigDef.Range.atLeast(-1);
  private static final ConfigDef.Importance MAX_WRITE_IMPORTANCE =  ConfigDef.Importance.MEDIUM;
  private static final String MAX_WRITE_DOC =
      "The maxiumum number of records to write at once to BigQuery, or -1 for no limit "
          + "(cannot be zero)";

  public static final String SANITIZE_TOPICS_CONFIG =                     "sanitizeTopics";
  private static final ConfigDef.Type SANITIZE_TOPICS_TYPE =              ConfigDef.Type.BOOLEAN;
  public static final Boolean SANITIZE_TOPICS_DEFAULT =                   false;
  private static final ConfigDef.Importance SANITIZE_TOPICS_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String SANITIZE_TOPICS_DOC =
      "Whether to automatically sanitize topic names before using them as table names;"
      + " if not enabled topic names will be used directly as table names";

  public static final String INCLUDE_KAFKA_DATA_CONFIG =                   "includeKafkaData";
  public static final ConfigDef.Type INCLUDE_KAFKA_DATA_TYPE =             ConfigDef.Type.BOOLEAN;
  public static final Boolean INCLUDE_KAFKA_DATA_DEFAULT =                 false;
  public static final ConfigDef.Importance INCLUDE_KAFKA_DATA_IMPORTANCE =
      ConfigDef.Importance.LOW;
  public static final String INSTANCE_KAFKA_DATA_DOC =
      "Whether to include an extra block containing the Kafka source topic, offset, "
      + "and partition information in the resulting BigQuery rows.";

  public static final String AVRO_DATA_CACHE_SIZE_CONFIG =                 "avroDataCacheSize";
  private static final ConfigDef.Type AVRO_DATA_CACHE_SIZE_TYPE =          ConfigDef.Type.INT;
  public static final Integer AVRO_DATA_CACHE_SIZE_DEFAULT =               100;
  private static final ConfigDef.Validator AVRO_DATA_CACHE_SIZE_VALIDATOR =
      ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance AVRO_DATA_CACHE_SIZE_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String AVRO_DATA_CACHE_SIZE_DOC =
      "The size of the cache to use when converting schemas from Avro to Kafka Connect";

  static {
    config = new ConfigDef()
        .define(
            TOPICS_CONFIG,
            TOPICS_TYPE,
            TOPICS_IMPORTANCE,
            TOPICS_DOC
        ).define(
            PROJECT_CONFIG,
            PROJECT_TYPE,
            PROJECT_IMPORTANCE,
            PROJECT_DOC
        ).define(
            DATASETS_CONFIG,
            DATASETS_TYPE,
            DATASETS_DEFAULT,
            DATASETS_VALIDATOR,
            DATASETS_IMPORTANCE,
            DATASETS_DOC
        ).define(
            KEYFILE_CONFIG,
            KEYFILE_TYPE,
            KEYFILE_DEFAULT,
            KEYFILE_IMPORTANCE,
            KEYFILE_DOC
        ).define(
            REGISTRY_CONFIG,
            REGISTRY_TYPE,
            REGISTRY_DEFAULT,
            REGISTRY_IMPORTANCE,
            REGISTRY_DOC
        ).define(
            MAX_WRITE_CONFIG,
            MAX_WRITE_TYPE,
            MAX_WRITE_DEFAULT,
            MAX_WRITE_VALIDATOR,
            MAX_WRITE_IMPORTANCE,
            MAX_WRITE_DOC
        ).define(
            SANITIZE_TOPICS_CONFIG,
            SANITIZE_TOPICS_TYPE,
            SANITIZE_TOPICS_DEFAULT,
            SANITIZE_TOPICS_IMPORTANCE,
            SANITIZE_TOPICS_DOC
        ).define(
            INCLUDE_KAFKA_DATA_CONFIG,
            INCLUDE_KAFKA_DATA_TYPE,
            INCLUDE_KAFKA_DATA_DEFAULT,
            INCLUDE_KAFKA_DATA_IMPORTANCE,
            INSTANCE_KAFKA_DATA_DOC
        ).define(
            AVRO_DATA_CACHE_SIZE_CONFIG,
            AVRO_DATA_CACHE_SIZE_TYPE,
            AVRO_DATA_CACHE_SIZE_DEFAULT,
            AVRO_DATA_CACHE_SIZE_VALIDATOR,
            AVRO_DATA_CACHE_SIZE_IMPORTANCE,
            AVRO_DATA_CACHE_SIZE_DOC
        );
  }

  @SuppressWarnings("unchecked")
  public static class Validator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
      switch (name) {
        case DATASETS_CONFIG:
          ensureValidMap(name, (List<String>) value);
          break;
        default:
          break;
      }
    }

    protected static void ensureValidMap(String name, List<String> values) {
      if (values == null) {
        return;
      }
      values.forEach((entry) -> parseMapping(entry, name));
    }

    /**
    * Ensures the mapping given is valid, then returns an entry containing its key and value.
    * Checks to make sure that the given String adheres to the specified format, and throws
    * an exception if it does not. Trims leading and trailing whitespace, and then checks to make
    * sure that both Strings are still non-empty.
    *
    * @param mapping The mapping to parse (should be of the form &lt;key&gt;=&lt;value&gt;)
    * @param name The name of the field. Used in error messages.
    * @return A Map.Entry containing the parsed key/value pair.
    */
    protected static Map.Entry<String, String> parseMapping(String mapping, String name) {
      String[] keyValue = mapping.split("=");
      if (keyValue.length != 2) {
        throw new ConfigException(
            "Invalid mapping for " + name
            + " property: '" + mapping
            + "' (must follow format '<key>=<value>')"
        );
      }

      String key = keyValue[0].trim();
      if (key.isEmpty()) {
        throw new ConfigException(
            "Empty key found in mapping '" + mapping
            + "' for " + name + " property"
        );
      }

      String value = keyValue[1].trim();
      if (value.isEmpty()) {
        throw new ConfigException(
            "Empty value found in mapping '" + mapping
            + "' for " + name + " property"
        );
      }

      return new AbstractMap.SimpleEntry<>(key, value);
    }
  }

  /**
   * Parses a config map, which must be provided as a list of Strings of the form
   * '&lt;key&gt;=&lt;value&gt;' into a Map. Locates that list, splits its key and value pairs, and
   * returns they Map they represent.
   *
   * @param name The name of the property the mapping is given for. Used in exception messages.
   * @return A Map containing the given key and value pairs.
   */
  public Map<String, String> getMap(String name) {
    List<String> assocList = getList(name);
    Map<String, String> configMap = new HashMap<>();
    if (assocList != null) {
      for (String mapping : assocList) {
        Map.Entry<String, String> entry = validator.parseMapping(mapping, name);
        configMap.put(entry.getKey(), entry.getValue());
      }
    }
    return configMap;
  }

  private List<Map.Entry<Pattern, String>> getSinglePatterns(String property) {
    List<String> propList = getList(property);
    List<Map.Entry<Pattern, String>> patternList = new ArrayList<>();
    if (propList != null) {
      for (String propValue : propList) {
        Map.Entry<String, String> mapping = validator.parseMapping(propValue, property);
        Pattern propPattern = Pattern.compile(mapping.getKey());
        Map.Entry<Pattern, String> patternEntry =
            new AbstractMap.SimpleEntry<>(propPattern, mapping.getValue());
        patternList.add(patternEntry);
      }
    }
    return patternList;
  }

  private Map<String, String> getSingleMatches(
      List<Map.Entry<Pattern, String>> patterns,
      List<String> values,
      String valueProperty,
      String patternProperty) {
    Map<String, String> matches = new HashMap<>();
    for (String value : values) {
      String match = null;
      for (Map.Entry<Pattern, String> pattern : patterns) {
        Matcher patternMatcher = pattern.getKey().matcher(value);
        if (patternMatcher.matches()) {
          if (match != null) {
            String secondMatch = pattern.getValue();
            throw new ConfigException(
                "Value '" + value
                + "' for property '" + valueProperty
                + "' matches " + patternProperty
                + " regexes for both '" + match
                + "' and '" + secondMatch + "'"
            );
          }
          match = pattern.getValue();
        }
      }
      if (match == null) {
        throw new ConfigException(
            "Value '" + value
            + "' for property '" + valueProperty
            + "' failed to match any of the provided " + patternProperty
            + " regexes"
        );
      }
      matches.put(value, match);
    }
    return matches;
  }

  /**
   * Return a Map detailing which BigQuery table each topic should write to.
   *
   * @return A Map associating Kafka topic names to BigQuery table IDs.
   */
  public Map<String, String> getTopicsToDatasets() {
    return getSingleMatches(
        getSinglePatterns(DATASETS_CONFIG),
        getList(TOPICS_CONFIG),
        TOPICS_CONFIG,
        DATASETS_CONFIG
    );
  }

  private String sanitizeTableName(String tableName) {
    // Take anything that isn't valid in a table name and turn it into an underscore.
    return tableName.replaceAll("[^a-zA-Z0-9_]", "_");
  }

  /**
   * Convert a topic name to a table name, depending on whether sanitization has been specified.
   *
   * @param topic The topic name to convert
   * @return The resulting table name. Note that different topic names can lead to the same table
   *         name if sanitization is enabled.
   */
  public String getTableFromTopic(String topic) {
    return getBoolean(SANITIZE_TOPICS_CONFIG) ? sanitizeTableName(topic) : topic;
  }

  /**
   * Return a Map detailing which topic each table corresponds to. If sanitization has been enabled,
   * there is a possibility that there are multiple possible schemas a table could correspond to. In
   * that case, each table must only be written to by one topic, or an exception is thrown.
   *
   * @param topicsToDatasets A Map detailing which topics belong to which datasets.
   * @return The resulting Map from TableId to topic name.
   */
  public Map<TableId, String> getTablesToTopics(Map<String, String> topicsToDatasets) {
    Map<TableId, String> tablesToTopics = new HashMap<>();
    for (Map.Entry<String, String> topicDataset : topicsToDatasets.entrySet()) {
      String topic = topicDataset.getKey();
      TableId tableId = TableId.of(topicDataset.getValue(), getTableFromTopic(topic));
      if (tablesToTopics.put(tableId, topic) != null) {
        throw new ConfigException("Cannot have multiple topics writing to the same table");
      }
    }
    return tablesToTopics;
  }

  /**
   * Return a new instance of the configured Schema Converter.
   * @return a {@link SchemaConverter} for BigQuery.
   */
  public SchemaConverter<Schema> getSchemaConverter() {
    return getBoolean(INCLUDE_KAFKA_DATA_CONFIG) ? new KafkaDataBQSchemaConverter()
      : new BigQuerySchemaConverter();
  }

  /**
   * Return a new instance of the configured Record Converter.
   * @return a {@link RecordConverter} for BigQuery.
   */
  public RecordConverter<Map<String, Object>> getRecordConverter() {
    return getBoolean(INCLUDE_KAFKA_DATA_CONFIG) ? new KafkaDataBQRecordConverter()
      : new BigQueryRecordConverter();
  }

  /**
   * Return the ConfigDef object used to define this config's fields.
   *
   * @return The ConfigDef object used to define this config's fields.
   */
  public static ConfigDef getConfig() {
    return config;
  }

  protected BigQuerySinkConfig(ConfigDef config, Map<String, String> properties) {
    super(config, properties);
  }

  public BigQuerySinkConfig(Map<String, String> properties) {
    super(config, properties);
  }
}
