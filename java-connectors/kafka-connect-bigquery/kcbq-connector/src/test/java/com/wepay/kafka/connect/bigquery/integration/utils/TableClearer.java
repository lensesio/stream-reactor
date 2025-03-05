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

package com.wepay.kafka.connect.bigquery.integration.utils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.table;

public class TableClearer {
  private static final Logger logger = LoggerFactory.getLogger(TableClearer.class);

  /**
   * Clear out one or more BigQuery tables. Useful in integration testing to provide a clean slate
   * before creating a connector and writing to those tables.
   * @param bigQuery The BigQuery client to use when sending table deletion requests.
   * @param dataset The dataset that the to-be-cleared tables belong to.
   * @param tables The tables to clear.
   */
  public static void clearTables(BigQuery bigQuery, String dataset, Collection<String> tables) {
    for (String tableName : tables) {
      TableId table = TableId.of(dataset, FieldNameSanitizer.sanitizeName(tableName));
      if (bigQuery.delete(table)) {
        logger.info("{} deleted successfully", table(table));
      } else {
        logger.info("{} does not exist", table(table));
      }
    }
  }

  /**
   * Clear out one or more BigQuery tables. Useful in integration testing to provide a clean slate
   * before creating a connector and writing to those tables.
   * @param bigQuery The BigQuery client to use when sending table deletion requests.
   * @param dataset The dataset that the to-be-cleared tables belong to.
   * @param tables The tables to clear.
   */
  public static void clearTables(BigQuery bigQuery, String dataset, String... tables) {
    clearTables(bigQuery, dataset, Arrays.asList(tables));
  }
}
