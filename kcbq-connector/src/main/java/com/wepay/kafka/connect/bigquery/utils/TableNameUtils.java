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
}
