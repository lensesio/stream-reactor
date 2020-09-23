package com.wepay.kafka.connect.bigquery.write.row;

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


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.gson.Gson;

import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.GCSConnectException;

import org.apache.kafka.connect.errors.ConnectException;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;

/**
 * A class for batch writing list of rows to BigQuery through GCS.
 */
public class GCSToBQWriter {
  private static final Logger logger = LoggerFactory.getLogger(GCSToBQWriter.class);

  private static Gson gson = new Gson();

  private final Storage storage;

  private final BigQuery bigQuery;

  private final SchemaManager schemaManager;

  private static final int WAIT_MAX_JITTER = 1000;

  private static final Random random = new Random();

  private int retries;
  private long retryWaitMs;
  private boolean autoCreateTables;


  public static final String GCS_METADATA_TABLE_KEY = "sinkTable";

  /**
   * Initializes a batch GCS writer with a full list of rows to write.
   * @param storage GCS Storage
   * @param bigQuery {@link BigQuery} Object used to perform upload
   * @param retries Maximum number of retries
   * @param retryWaitMs Minimum number of milliseconds to wait before retrying
   */
  public GCSToBQWriter(Storage storage,
                       BigQuery bigQuery,
                       SchemaManager schemaManager,
                       int retries,
                       long retryWaitMs,
                       boolean autoCreateTables) {
    this.storage = storage;
    this.bigQuery = bigQuery;
    this.schemaManager = schemaManager;

    this.retries = retries;
    this.retryWaitMs = retryWaitMs;
    this.autoCreateTables = autoCreateTables;
  }

  /**
   * Write rows to BQ through GCS.
   *
   * @param rows the rows to write.
   * @param tableId the BQ table to write to.
   * @param bucketName the GCS bucket to write to.
   * @param blobName the name of the GCS blob to write.
   * @throws InterruptedException if interrupted.
   */
  public void writeRows(SortedMap<SinkRecord, RowToInsert> rows,
                        TableId tableId,
                        String bucketName,
                        String blobName) throws InterruptedException {

    // Get Source URI
    BlobId blobId = BlobId.of(bucketName, blobName);

    Map<String, String> metadata = getMetadata(tableId);
    BlobInfo blobInfo =
         BlobInfo.newBuilder(blobId).setContentType("text/json").setMetadata(metadata).build();

    // Check if the table specified exists
    // This error shouldn't be thrown. All tables should be created by the connector at startup
    if (autoCreateTables && bigQuery.getTable(tableId) == null) {
      attemptTableCreate(tableId, new ArrayList<>(rows.keySet()));
    }

    int attemptCount = 0;
    boolean success = false;
    while (!success && (attemptCount <= retries)) {
      if (attemptCount > 0) {
        waitRandomTime();
      }
      // Perform GCS Upload
      try {
        uploadRowsToGcs(rows, blobInfo);
        success = true;
      } catch (StorageException se) {
        logger.warn("Exceptions occurred for table {}, attempting retry", tableId.getTable());
      }
      attemptCount++;
    }

    if (success) {
      logger.info("Batch loaded {} rows", rows.size());
    } else {
      throw new ConnectException(String.format("Failed to load %d rows into GCS within %d re-attempts.", rows.size(), retries));
    }

  }

  private static Map<String, String> getMetadata(TableId tableId) {
    StringBuilder sb = new StringBuilder();
    if (tableId.getProject() != null) {
      sb.append(tableId.getProject()).append(":");
    }
    String serializedTableId =
        sb.append(tableId.getDataset()).append(".").append(tableId.getTable()).toString();
    Map<String, String> metadata =
        Collections.singletonMap(GCS_METADATA_TABLE_KEY, serializedTableId);
    return metadata;
  }

  /**
   * Creates a JSON string containing all records and uploads it as a blob to GCS.
   * @return The blob uploaded to GCS
   */
  private Blob uploadRowsToGcs(SortedMap<SinkRecord, RowToInsert> rows, BlobInfo blobInfo) {
    try {
      Blob resultBlob = uploadBlobToGcs(toJson(rows.values()).getBytes("UTF-8"), blobInfo);
      return resultBlob;
    } catch (UnsupportedEncodingException uee) {
      throw new GCSConnectException("Failed to upload blob to GCS", uee);
    }
  }

  private Blob uploadBlobToGcs(byte[] blobContent, BlobInfo blobInfo) {
    return storage.create(blobInfo, blobContent); // todo options: like a retention policy maybe?
  }

  /**
   * Converts a list of rows to a serialized JSON string of records.
   * @param rows rows to be serialized
   * @return The resulting newline delimited JSON string containing all records in the original
   *         list
   */
  private String toJson(Collection<RowToInsert> rows) {
    StringBuilder jsonRecordsBuilder = new StringBuilder("");
    for (RowToInsert row : rows) {
      Map<String, Object> record = row.getContent();
      jsonRecordsBuilder.append(gson.toJson(record));
      jsonRecordsBuilder.append("\n");
    }
    return jsonRecordsBuilder.toString();
  }

  /**
   * Wait at least {@link #retryWaitMs}, with up to an additional 1 second of random jitter.
   * @throws InterruptedException if interrupted.
   */
  private void waitRandomTime() throws InterruptedException {
    Thread.sleep(retryWaitMs + random.nextInt(WAIT_MAX_JITTER));
  }

  private void attemptTableCreate(TableId tableId, List<SinkRecord> records) {
    try {
      logger.info("Table {} does not exist, auto-creating table ", tableId);
      schemaManager.createTable(tableId, records);
    } catch (BigQueryException exception) {
      throw new BigQueryConnectException(
              "Failed to create table " + tableId, exception);
    }
  }
}
