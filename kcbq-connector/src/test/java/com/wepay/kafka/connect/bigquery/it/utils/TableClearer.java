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

package com.wepay.kafka.connect.bigquery.it.utils;

import com.google.cloud.bigquery.BigQuery;

import com.wepay.kafka.connect.bigquery.BigQueryHelper;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableClearer {
  private static final Logger logger = LoggerFactory.getLogger(TableClearer.class);
  private static String keySource;


  /**
   * Clears tables in the given project and dataset, using a provided JSON service account key.
   */
  public static void main(String[] args) {
    if (args.length < 4) {
      usage();
    }
    int tablesStart = 3;
    BigQuery bigQuery = new BigQueryHelper().connect(args[1], args[0]);
    for (int i = tablesStart; i < args.length; i++) {
      // May be consider using sanitizeTopics property value in future to decide table name
      // sanitization but as currently we always run test cases with sanitizeTopics value as true
      // hence sanitize table name prior delete. This is required else it makes test cases flaky.
      String table = FieldNameSanitizer.sanitizeName(args[i]);
      if (bigQuery.delete(args[2], table)) {
        logger.info("Table {} in dataset {} deleted successfully", table, args[2]);
      } else {
        logger.info("Table {} in dataset {} does not exist", table, args[2]);
      }
    }
  }

  private static void usage() {
    System.err.println(
        "usage: TableClearer <key_file> <project_name> <dataset_name> <table> [<table> ...]"
    );
    System.exit(1);
  }
}
