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

package com.wepay.kafka.connect.bigquery.write.batch;

import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.convert.RecordConverter;

import com.wepay.kafka.connect.bigquery.write.row.GCSToBQWriter;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Batch Table Writer that uploads records to GCS as a blob
 * and then triggers a load job from that GCS file to BigQuery.
 */
public class GCSBatchTableWriter implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(GCSBatchTableWriter.class);

  private final TableId tableId;

  private final String bucketName;
  private final String blobName;

  private final List<RowToInsert> rows;
  private final GCSToBQWriter writer;

  /**
   * @param rows The list of rows that should be written through GCS
   * @param writer {@link GCSToBQWriter} to use
   * @param tableId the BigQuery table id of the table to write to
   * @param bucketName the name of the GCS bucket where the blob should be uploaded
   * @param baseBlobName the base name of the blob in which the serialized rows should be uploaded.
   *                     The full name is [baseBlobName]_[writerId]_
   */
  private GCSBatchTableWriter(List<RowToInsert> rows,
                              GCSToBQWriter writer,
                              TableId tableId,
                              String bucketName,
                              String baseBlobName) {
    this.tableId = tableId;
    this.bucketName = bucketName;
    this.blobName = baseBlobName;

    this.rows = rows;
    this.writer = writer;
  }

  @Override
  public void run() {
    try {
      writer.writeRows(rows, tableId, bucketName, blobName);
    } catch (ConnectException ex) {
      throw new ConnectException("Failed to write rows to GCS", ex);
    } catch (InterruptedException ex) {
      throw new ConnectException("Thread interrupted while batch writing", ex);
    }
  }

  /**
   * A Builder for constructing GCSBatchTableWriters.
   */
  public static class Builder implements TableWriterBuilder {
    private final String bucketName;
    private String blobName;

    private final TableId tableId;

    private List<RowToInsert> rows;
    private final RecordConverter<Map<String, Object>> recordConverter;
    private final GCSToBQWriter writer;

    /**
     * Create a {@link GCSBatchTableWriter.Builder}.
     *
     * @param writer the {@link GCSToBQWriter} to use.
     * @param tableId The bigquery table to be written to.
     * @param gcsBucketName The GCS bucket to write to.
     * @param gcsBlobName The name of the GCS blob to write.
     * @param recordConverter the {@link RecordConverter} to use.
     */
    public Builder(GCSToBQWriter writer,
                   TableId tableId,
                   String gcsBucketName,
                   String gcsBlobName,
                   RecordConverter<Map<String, Object>> recordConverter) {

      this.bucketName = gcsBucketName;
      this.blobName = gcsBlobName;

      this.tableId = tableId;

      this.rows = new ArrayList<>();
      this.recordConverter = recordConverter;
      this.writer = writer;
    }

    public Builder setBlobName(String blobName) {
      this.blobName = blobName;
      return this;
    }

    /**
     * Adds a record to the builder.
     * @param rowToInsert the row to add
     */
    public void addRow(RowToInsert rowToInsert) {
      rows.add(rowToInsert);
    }

    public GCSBatchTableWriter build() {
      return new GCSBatchTableWriter(rows, writer, tableId, bucketName, blobName);
    }
  }
}
