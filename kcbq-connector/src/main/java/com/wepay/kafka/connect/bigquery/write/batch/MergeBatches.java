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

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.wepay.kafka.connect.bigquery.MergeQueries;
import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.intTable;

public class MergeBatches {
  private static final Logger logger = LoggerFactory.getLogger(MergeBatches.class);
  private static final long STREAMING_BUFFER_AVAILABILITY_WAIT_MS = 10_000L;

  private static long streamingBufferAvailabilityWaitMs = STREAMING_BUFFER_AVAILABILITY_WAIT_MS;

  private final String intermediateTableSuffix;
  private final BiMap<TableId, TableId> intermediateToDestinationTables;
  private final ConcurrentMap<TableId, AtomicInteger> batchNumbers;
  private final ConcurrentMap<TableId, ConcurrentMap<Integer, Batch>> batches;
  private final Map<TopicPartition, Long> offsets;

  @VisibleForTesting
  public static void setStreamingBufferAvailabilityWait(long waitMs) {
    streamingBufferAvailabilityWaitMs = waitMs;
  }

  @VisibleForTesting
  public static void resetStreamingBufferAvailabilityWait() {
    streamingBufferAvailabilityWaitMs = STREAMING_BUFFER_AVAILABILITY_WAIT_MS;
  }

  public MergeBatches(String intermediateTableSuffix) {
    this.intermediateTableSuffix = intermediateTableSuffix;

    this.intermediateToDestinationTables = Maps.synchronizedBiMap(HashBiMap.create());
    this.batchNumbers = new ConcurrentHashMap<>();
    this.batches = new ConcurrentHashMap<>();
    this.offsets = new HashMap<>();
  }

  /**
   * Get the latest safe-to-commit offsets for every topic partition that has had at least one
   * record make its way to a destination table.
   * @return the offsets map which can be used in
   * {@link org.apache.kafka.connect.sink.SinkTask#preCommit(Map)}; never null
   */
  public Map<TopicPartition, OffsetAndMetadata> latestOffsets() {
    synchronized (offsets) {
      return offsets.entrySet().stream().collect(Collectors.toMap(
          Map.Entry::getKey,
          entry -> new OffsetAndMetadata(entry.getValue())
      ));
    }
  }

  /**
   * @return a thread-safe map from intermediate tables to destination tables; never null
   */
  public Map<TableId, TableId> intermediateToDestinationTables() {
    return intermediateToDestinationTables;
  }

  /**
   * @return a collection of all currently-in-use intermediate tables; never null
   */
  public Collection<TableId> intermediateTables() {
    return intermediateToDestinationTables.keySet();
  }

  /**
   * Get the intermediate table for a given destination table, computing a new one if necessary
   * @param destinationTable the destination table to fetch an intermediate table for
   * @return the {@link TableId} of the intermediate table; never null
   */
  public TableId intermediateTableFor(TableId destinationTable) {
    return intermediateToDestinationTables.inverse()
        .computeIfAbsent(destinationTable, this::newIntermediateTable);
  }

  private TableId newIntermediateTable(TableId destinationTable) {
    String tableName = FieldNameSanitizer.sanitizeName(
        destinationTable.getTable() + intermediateTableSuffix
    );
    TableId result = TableId.of(
        destinationTable.getDataset(),
        tableName
    );

    batchNumbers.put(result, new AtomicInteger());
    batches.put(result, new ConcurrentHashMap<>());

    return result;
  }

  public TableId destinationTableFor(TableId intermediateTable) {
    return intermediateToDestinationTables.get(intermediateTable);
  }

  /**
   * Find a batch number for the record, insert that number into the converted value, record the
   * offset for that record, and return the total size of that batch.
   * @param record the record for the batch
   * @param intermediateTable the intermediate table the record will be streamed into
   * @param convertedRecord the converted record that will be passed to the BigQuery client
   * @return the total number of records in the batch that this record is added to
   */
  public long addToBatch(SinkRecord record, TableId intermediateTable, Map<String, Object> convertedRecord) {
    AtomicInteger batchCount = batchNumbers.get(intermediateTable);
    // Synchronize here to ensure that the batch number isn't bumped in the middle of this method.
    // On its own, that wouldn't be such a bad thing, but since a merge flush is supposed to
    // immediately follow that bump, it might cause some trouble if we want to add this row to the
    // batch but a merge flush on that batch has already started. This way, either the batch number
    // is bumped before we add the row to the batch (in which case, the row is added to the fresh
    // batch), or the row is added to the batch before preparation for the flush takes place and it
    // is safely counted and tracked there.
    synchronized (batchCount) {
      int batchNumber = batchCount.get();
      convertedRecord.put(MergeQueries.INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD, batchNumber);

      Batch batch = batches.get(intermediateTable).computeIfAbsent(batchNumber, n -> new Batch());
      batch.recordOffsetFor(record);

      long pendingBatchSize = batch.increment();
      logger.trace("Added record to batch {} for {}; {} rows are currently pending",
          batchNumber, intTable(intermediateTable), pendingBatchSize);
      return batch.total();
    }
  }

  /**
   * Record a successful write of a list of rows to the given intermediate table, and decrease the
   * pending record counts for every applicable batch accordingly.
   * @param intermediateTable the intermediate table
   * @param rows the rows
   */
  public void onRowWrites(TableId intermediateTable, Collection<InsertAllRequest.RowToInsert> rows) {
    Map<Integer, Long> rowsByBatch = rows.stream().collect(Collectors.groupingBy(
        row -> (Integer) row.getContent().get(MergeQueries.INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD),
        Collectors.counting()
    ));

    rowsByBatch.forEach((batchNumber, batchSize) -> {
      Batch batch = batch(intermediateTable, batchNumber);
      synchronized (batch) {
        long remainder = batch.recordWrites(batchSize);
        batch.notifyAll();
        logger.trace("Notified merge flush executor of successful write of {} rows "
                + "for batch {} for {}; {} rows remaining",
            batchSize, batchNumber, intTable(intermediateTable), remainder);
      }
    });
  }

  /**
   * Increment the batch number for the given table, and return the old batch number.
   * @param intermediateTable the table whose batch number should be incremented
   * @return the batch number for the table, pre-increment
   */
  public int incrementBatch(TableId intermediateTable) {
    AtomicInteger batchCount = batchNumbers.get(intermediateTable);
    // See addToBatch for an explanation of the synchronization here
    synchronized (batchCount) {
      return batchCount.getAndIncrement();
    }
  }

  /**
   * Prepare to merge the batch for the given table, by ensuring that all prior batches for that
   * table have completed and that all rows for the batch itself have been written.
   * @param intermediateTable the table for the batch
   * @param batchNumber the batch number to prepare to flush
   * @return whether a flush is necessary (will be false if no rows were present in the given batch)
   */
  public boolean prepareToFlush(TableId intermediateTable, int batchNumber) {
    final ConcurrentMap<Integer, Batch> allBatchesForTable = batches.get(intermediateTable);
    if (batchNumber != 0) {
      final int priorBatchNumber = batchNumber - 1;
      synchronized (allBatchesForTable) {
        logger.debug("Ensuring batch {} is completed for {} before flushing batch {}",
            priorBatchNumber, intTable(intermediateTable), batchNumber);
        while (allBatchesForTable.containsKey(priorBatchNumber)) {
          try {
            allBatchesForTable.wait();
          } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for batch {} to complete for {}",
                batchNumber, intTable(intermediateTable));
            throw new ExpectedInterruptException(String.format(
                "Interrupted while waiting for batch %d to complete for %s",
                batchNumber, intTable(intermediateTable)
            ));
          }
        }
      }
    } else {
      logger.debug("Flushing first batch for {}", intTable(intermediateTable));
    }

    final Batch currentBatch = allBatchesForTable.get(batchNumber);
    if (currentBatch == null) {
      logger.trace("No rows to write in batch {} for {}", batchNumber, intTable(intermediateTable));
      return false;
    }

    synchronized (currentBatch) {
      logger.debug("{} rows currently remaining for batch {} for {}",
          currentBatch.pending(), batchNumber, intTable(intermediateTable));
      while (currentBatch.pending() != 0) {
        logger.trace("Waiting for all rows for batch {} from {} to be written before flushing; {} remaining",
            batchNumber, intTable(intermediateTable), currentBatch.pending());
        try {
          currentBatch.wait();
        } catch (InterruptedException e) {
          logger.warn("Interrupted while waiting for all rows for batch {} from {} to be written",
              batchNumber, intTable(intermediateTable));
          throw new ExpectedInterruptException(String.format(
              "Interrupted while waiting for all rows for batch %d from %s to be written",
              batchNumber, intTable(intermediateTable)
          ));
        }
      }
    }

    try {
      logger.trace(
          "Waiting {}ms before running merge query on batch {} from {} "
              + "in order to ensure that all rows are available in the streaming buffer",
          streamingBufferAvailabilityWaitMs, batchNumber, intTable(intermediateTable));
      Thread.sleep(streamingBufferAvailabilityWaitMs);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while waiting before merge flushing batch {} for {}",
          batchNumber, intTable(intermediateTable));
      throw new ExpectedInterruptException(String.format(
          "Interrupted while waiting before merge flushing batch %d for %s",
          batchNumber, intTable(intermediateTable)
      ));
    }
    return true;
  }

  /**
   * Record a successful merge flush of all of the rows for the given batch in the intermediate
   * table, alert any waiting merge flushes that are predicated on the completion of this merge
   * flush, and marke the offsets for all of those rows as safe to commit.
   * @param intermediateTable the source of the merge flush
   * @param batchNumber the batch for the merge flush
   */
  public void recordSuccessfulFlush(TableId intermediateTable, int batchNumber) {
    logger.trace("Successfully merge flushed batch {} for {}",
        batchNumber, intTable(intermediateTable));
    final ConcurrentMap<Integer, Batch> allBatchesForTable = batches.get(intermediateTable);
    Batch batch = allBatchesForTable.remove(batchNumber);

    synchronized (allBatchesForTable) {
      allBatchesForTable.notifyAll();
    }

    synchronized (offsets) {
      offsets.putAll(batch.offsets());
    }
  }

  private Batch batch(TableId intermediateTable, int batchNumber) {
    return batches.get(intermediateTable).get(batchNumber);
  }

  private static class Batch {
    private final AtomicLong pending;
    private final AtomicLong total;
    private final Map<TopicPartition, Long> offsets;

    public Batch() {
      this.total = new AtomicLong();
      this.pending = new AtomicLong();
      this.offsets = new HashMap<>();
    }

    public long pending() {
      return pending.get();
    }

    public long total() {
      return total.get();
    }

    public Map<TopicPartition, Long> offsets() {
      return offsets;
    }

    public void recordOffsetFor(SinkRecord record) {
      offsets.put(
          new TopicPartition(record.topic(), record.kafkaPartition()),
          // Use the offset of the record plus one here since that'll be the offset that we'll
          // resume at if/when this record is the last-committed record and then the task is
          // restarted
          record.kafkaOffset() + 1);
    }

    /**
     * Increment the total and pending number of records, and return the number of pending records
     * @return the number of pending records for this batch
     */
    public long increment() {
      total.incrementAndGet();
      return pending.incrementAndGet();
    }

    public long recordWrites(long numRows) {
      return pending.addAndGet(-numRows);
    }
  }
}
