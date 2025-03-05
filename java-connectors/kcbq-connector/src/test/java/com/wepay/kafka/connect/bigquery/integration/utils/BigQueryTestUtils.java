package com.wepay.kafka.connect.bigquery.integration.utils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryTestUtils {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryTestUtils.class);

  public static void createPartitionedTable(BigQuery bigQuery, String datasetName, String tableName,
                                            Schema schema) {
    try {
      TableId tableId = TableId.of(datasetName, tableName);

      TimePartitioning partitioning =
          TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
              .build();

      StandardTableDefinition tableDefinition =
          StandardTableDefinition.newBuilder()
              .setSchema(schema)
              .setTimePartitioning(partitioning)
              .build();
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

      bigQuery.create(tableInfo);
      logger.info("Partitioned table {} created successfully", tableName);
    } catch (BigQueryException e) {
      logger.error("Failed to create partitioned table {} in dataset {}", tableName, datasetName);
      throw e;
    }
  }
}
