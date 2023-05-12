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

package com.wepay.kafka.connect.bigquery.utils;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableNameUtils {

  public static String table(TableId table) {
    return String.format("table `%s`.`%s`", table.getDataset(), table.getTable());
  }

  public static TableName tableName(TableId id) {
    return TableName.of(id.getProject(), id.getDataset(), id.getTable());
  }
  public static String intTable(TableId table) {
    return "intermediate " + table(table);
  }

  public static String destTable(TableId table) {
    return "destination " + table(table);
  }

  public static TableId tableId(TableName name) {
    return TableId.of(name.getProject(), name.getDataset(), name.getTable());
  }

  public static String[] getDataSetAndTableName(BigQuerySinkTaskConfig config, String topic) {
    String tableName;
    Map<String, String> topic2TableMap = config.getTopic2TableMap().orElse(null);
    String dataset = config.getString(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG);

    if (topic2TableMap != null) {
      tableName = topic2TableMap.getOrDefault(topic, topic);
    } else {
      String[] smtReplacement = topic.split(":");

      if (smtReplacement.length == 2) {
        dataset = smtReplacement[0];
        tableName = smtReplacement[1];
      } else if (smtReplacement.length == 1) {
        tableName = smtReplacement[0];
      } else {
        throw new ConnectException(String.format(
                "Incorrect regex replacement format in topic name '%s'. "
                        + "SMT replacement should either produce the <dataset>:<tableName> format "
                        + "or just the <tableName> format.",
                topic
        ));
      }
      if (config.getBoolean(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG)) {
        tableName = FieldNameSanitizer.sanitizeName(tableName);
      }
    }

    return new String[]{dataset, tableName};
  }

  /**
   * Returns list of all tablenames in project/project_id/dataset/dataset_id/tablename format
   */
  public static List<String> getAllTableNames(BigQuerySinkTaskConfig config) {
    String projectId = config.getString(BigQuerySinkTaskConfig.PROJECT_CONFIG);
    return config.getList(BigQuerySinkConfig.TOPICS_CONFIG)
            .stream()
            .map(topic -> {
              String[] dataSetAndTopic = getDataSetAndTableName(config, topic);
              return TableName.of(projectId, dataSetAndTopic[0], dataSetAndTopic[1]).toString();
            })
            .collect(Collectors.toList());
  }
}
