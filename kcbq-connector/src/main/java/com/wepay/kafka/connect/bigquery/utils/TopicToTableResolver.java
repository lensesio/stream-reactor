package com.wepay.kafka.connect.bigquery.utils;

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

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A utility class that will resolve topic names to table names based on format strings using regex
 * capture groups.
 */
public class TopicToTableResolver {

  /**
   * Return a Map detailing which BigQuery table each topic should write to.
   * @param config Config that contains properties used to generate the map
   * @return A Map associating Kafka topic names to BigQuery table names.
   */
  public static Map<String, String> getTopicsToTables(BigQuerySinkConfig config) {
      return getMatchesForTableNames(
          config.getSinglePatterns(config.TOPICS_TO_TABLES_CONFIG),
          config.getList(config.TOPICS_CONFIG),
          config.TOPICS_CONFIG,
          config.TOPICS_TO_TABLES_CONFIG,
          config.getBoolean(config.SANITIZE_TOPICS_CONFIG)
      );
  }

  /**
   * Takes a list of topic names and for each finds a matching regex pattern. If there is a match,
   * take the capture groups and arrange them according to the format string for the corresponding
   * regex pattern. If not, use the topic name (potentially sanitized).
   *
   * @param patterns List of mappings from regex patterns to a format string
   * @param values List of values to format using matching regex patterns
   * @param valueProperty Name of the property containing the list of values
   * @param patternProperty Name of the property containing maps of regex patterns to format strings
   * @return a map from topic names to table names.
   */
  private static Map<String, String> getMatchesForTableNames(
      List<Map.Entry<Pattern, String>> patterns,
      List<String> values,
      String valueProperty,
      String patternProperty,
      Boolean sanitize) {
    Map<String, String> matches = new HashMap<>();
    for (String value : values) {
      String match = null;
      String previousPattern = null;
      for (Map.Entry<Pattern, String> pattern : patterns) {
        Matcher patternMatcher = pattern.getKey().matcher(value);
        if (patternMatcher.matches()) {
          if (match != null) {
            String secondMatch = pattern.getKey().toString();
            throw new ConfigException(
                "Value '" + value
                    + "' for property '" + valueProperty
                    + "' matches " + patternProperty
                    + " regexes for both '" + previousPattern
                    + "' and '" + secondMatch + "'"
            );
          }
          String formatString = pattern.getValue();
          try {
            match = patternMatcher.replaceAll(formatString);
            previousPattern = pattern.getKey().toString();
          } catch (IndexOutOfBoundsException e) {
            throw new ConfigException(
                "Format string '" + formatString
                    + "' is invalid in property '" + patternProperty
                    + "'", e);
          }
        }
      }
      if (match == null) {
        match = (sanitize) ? sanitizeTableName(value) : value;
      }
      matches.put(value, match);
    }
    return matches;
  }

  private static String sanitizeTableName(String tableName) {
    // Take anything that isn't valid in a table name and turn it into an underscore.
    return tableName.replaceAll("[^a-zA-Z0-9_]", "_");
  }
}
