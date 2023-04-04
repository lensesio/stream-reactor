package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.RowError;

import com.google.common.annotations.VisibleForTesting;
import com.google.rpc.Status;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiErrorResponses;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * An extension of {@link StorageWriteApiBase} which uses default streams to write data following at least once semantic
 */
public class StorageWriteApiDefaultStream extends StorageWriteApiBase {
    private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiDefaultStream.class);
    private static final ConcurrentMap<String, JsonStreamWriter> tableToStream = new ConcurrentHashMap<>();

    public StorageWriteApiDefaultStream(int retry,
                                        long retryWait,
                                        BigQueryWriteSettings writeSettings,
                                        boolean autoCreateTables,
                                        ErrantRecordHandler errantRecordHandler,
                                        SchemaManager schemaManager) throws IOException {
        super(retry, retryWait, writeSettings, autoCreateTables, errantRecordHandler, schemaManager);
    }

    @Override
    public void shutdown() {
        logger.info("Closing all writer for default stream on all tables");
        tableToStream.keySet().forEach(this::closeAndDelete);
        logger.info("Closed all writer for default stream on all tables");
    }

    /**
     * Either gets called when shutting down the task or when we receive exception that the stream
     * is actually closed on Google side. This will close and remove the stream from our cache.
     * @param tableName The table name for which stream has to be removed.
     */
    private void closeAndDelete(String tableName) {
        logger.debug("Closing stream on table {}", tableName);
        if(tableToStream.containsKey(tableName)) {
            synchronized (tableToStream) {
                tableToStream.get(tableName).close();
                tableToStream.remove(tableName);
            }
            logger.debug("Closed stream on table {}", tableName);
        }
    }

    /**
     * @param rows Rows of <SinkRecord, JSONObject > format
     * @return Returns list of all SinkRecords
     */
    private List<SinkRecord> getSinkRecords(List<Object[]> rows) {
        return rows.stream()
                .map(row -> (SinkRecord) row[0])
                .collect(Collectors.toList());
    }

    /**
     * Open a default stream on table if not already present
     * @param table The table on which stream has to be opened
     * @param rows  The input rows (would be sent while table creation to identify schema)
     * @return JSONStreamWriter which would be used to write data to bigquery table
     */
    @VisibleForTesting
    JsonStreamWriter getDefaultStream(TableName table, List<Object[]> rows) {
        String tableName = table.toString();
        return tableToStream.computeIfAbsent(tableName, t -> {
            boolean tableCreationOrUpdateAttempted = false;
            int additionalRetries = 0;
            int additionalWait = 0;
            Exception mostRecentException;
            int attempt = 0;
            do {
                try {
                    if (attempt > 0) {
                        waitRandomTime(additionalWait);
                    }
                    return JsonStreamWriter.newBuilder(t, getWriteClient()).build();
                } catch (Exception e) {
                    if (BigQueryStorageWriteApiErrorResponses.isTableMissing(e.getMessage()) && getAutoCreateTables()) {
                        if (!tableCreationOrUpdateAttempted) {
                            logger.info("Attempting to create table {} ...", tableName);
                            tableCreationOrUpdateAttempted = true;
                            attemptTableCreation(TableNameUtils.tableId(table), getSinkRecords(rows));
                            // Table takes time to be available for after creation
                            additionalWait = 30000; // 30 seconds;
                            additionalRetries = 30;
                            logger.info("Table creation completed {} ...", tableName);
                        }
                    } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(e.getMessage())) {
                        logger.error("Failed to create Default stream writer on table {}", tableName);
                        throw new BigQueryStorageWriteApiConnectException("Failed to create Default stream writer on table " + tableName, e);
                    }
                    logger.warn("Failed to create Default stream writer on table {} due to {}. Retry attempt {}...", tableName, e.getMessage(), attempt);
                    mostRecentException = e;
                    attempt++;
                }
            } while (attempt < (retry + additionalRetries));
            throw new BigQueryStorageWriteApiConnectException(
                    String.format(
                            "Exceeded %s attempts to create Default stream writer on table %s ",
                            (retry + additionalRetries), tableName
                    ), mostRecentException);

        });
    }

    /**
     * Calls AppendRows and handles exception if the ingestion fails
     *
     * @param tableName  The table to write data to
     * @param rows       The records to write
     * @param streamName The stream to use to write table to table. This will be DEFAULT always.
     */
    @Override
    public void appendRows(TableName tableName, List<Object[]> rows, String streamName) {
        JSONArray jsonArr = new JSONArray();

        for (Object[] item : rows) {
            jsonArr.put(item[1]);
        }

        logger.debug("Sending {} records to write Api default stream on {} ...", rows.size(), tableName);
        int attempt = 0;
        Exception mostRecentException = null;
        boolean tableCreationOrUpdateAttempted = false;
        int additionalRetries = 0;
        int additionalWait = 0;
        do {
            try {
                if (attempt > 0) {
                    waitRandomTime(additionalWait);
                }
                logger.trace("Sending records to Storage API writer...");
                JsonStreamWriter writer = getDefaultStream(tableName, rows);
                ApiFuture<AppendRowsResponse> response = writer.append(jsonArr);
                AppendRowsResponse writeResult = response.get();

                logger.trace("Received response from Storage API writer...");
                if (writeResult.hasUpdatedSchema()) {
                    logger.warn("Sent records schema does not match with table schema, will attempt to update schema");
                    if (!tableCreationOrUpdateAttempted) {
                        logger.info("Attempting to update table schema {} ...", tableName);
                        tableCreationOrUpdateAttempted = true;
                        attemptSchemaUpdate(TableNameUtils.tableId(tableName), getSinkRecords(rows));
                        // Table takes time to be available for after schema update
                        additionalWait = 30000; // 30 seconds
                        additionalRetries = 30;
                        logger.info("Schema update completed {} ...", tableName);
                    }
                    attempt++;
                } else if (writeResult.hasError()) {
                    Status errorStatus = writeResult.getError();
                    String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, errorStatus.getMessage());
                    if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(errorStatus.getCode())) {
                        mostRecentException = new BigQueryStorageWriteApiConnectException(tableName.getTable(), writeResult.getRowErrorsList());
                        if (getErrantRecordHandler().getErrantRecordReporter() != null) {
                            //Routes to DLQ
                            List<Object[]> filteredRecords = sendBadRecordsToDlqAndFilterGood(rows, convertToMap(writeResult.getRowErrorsList()), mostRecentException);
                            if (filteredRecords.isEmpty()) {
                                logger.info("All records have been sent to Dlq.");
                                return;
                            } else {
                                rows = filteredRecords;
                                logger.debug("Sending {} filtered records to bigquery again", rows.size());
                                jsonArr = new JSONArray();
                                for (Object[] item : rows) {
                                    jsonArr.put(item[1]);
                                }
                            }
                        } else {
                            // Fail if no DLQ
                            logger.warn("DLQ is not configured!");
                            throw new BigQueryStorageWriteApiConnectException(tableName.getTable(), writeResult.getRowErrorsList());
                        }
                    } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(errorStatus.getMessage())) {
                        // Fail on non-retriable error
                        logger.error(errorMessage);
                        throw new BigQueryStorageWriteApiConnectException(errorMessage);
                    }
                    logger.warn(errorMessage + "Retrying...");
                    mostRecentException = new BigQueryStorageWriteApiConnectException(errorMessage);
                    attempt++;
                } else {
                    logger.debug("Call to write Api default stream completed after {} retries!", attempt);
                    return;
                }
            } catch (BigQueryStorageWriteApiConnectException exception) {
                throw exception;
            } catch (Exception e) {
                String message = e.getMessage();
                String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, message);
                if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(message)
                        && BigQueryStorageWriteApiErrorResponses.hasInvalidSchema(getRowErrorMapping(e).values())) {
                    logger.warn("Sent records schema does not match with table schema, will attempt to update schema");
                    if (!tableCreationOrUpdateAttempted) {
                        logger.info("Attempting to update table schema {} ...", tableName);
                        tableCreationOrUpdateAttempted = true;
                        attemptSchemaUpdate(TableNameUtils.tableId(tableName), getSinkRecords(rows));
                        // Table takes time to be available for after schema update
                        additionalWait = 30000; // 30 seconds
                        additionalRetries = 30;
                        logger.info("Schema update completed {} ...", tableName);
                    }
                } else if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(message)) {
                    mostRecentException = new BigQueryStorageWriteApiConnectException(tableName.getTable(), getRowErrorMapping(e));
                    if (getErrantRecordHandler().getErrantRecordReporter() != null) {
                        //Routes to DLQ
                        List<Object[]> filteredRecords = sendBadRecordsToDlqAndFilterGood(rows, getRowErrorMapping(e), mostRecentException);
                        if (filteredRecords.isEmpty()) {
                            logger.info("All records have been sent to Dlq.");
                            return;
                        } else {
                            rows = filteredRecords;
                            logger.debug("Sending {} filtered records to bigquery again", rows.size());
                            jsonArr = new JSONArray();
                            for (Object[] item : rows) {
                                jsonArr.put(item[1]);
                            }
                        }
                    } else {
                        // Fail if no DLQ
                        logger.warn("DLQ is not configured!");
                        throw new BigQueryStorageWriteApiConnectException(tableName.getTable(), getRowErrorMapping(e));
                    }
                } else if (BigQueryStorageWriteApiErrorResponses.isStreamClosed(errorMessage)) {
                    // Streams can get autoclosed if there occurs any issues, we should delete the cached stream
                    // so that a new one gets created on retry.
                    closeAndDelete(tableName.toString());
                } else if (BigQueryStorageWriteApiErrorResponses.isTableMissing(message) && getAutoCreateTables()) {
                    if (!tableCreationOrUpdateAttempted) {
                        logger.info("Attempting to create table {} ...", tableName);
                        tableCreationOrUpdateAttempted = true;
                        attemptTableCreation(TableNameUtils.tableId(tableName), getSinkRecords(rows));
                        // Table takes time to be available for after creation
                        additionalWait = 30000; // 30 seconds
                        additionalRetries = 30;
                        logger.info("Table creation completed {} ...", tableName);
                    }
                } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(message)) {
                    // Fail on non-retriable error
                    logger.error(errorMessage);
                    throw new BigQueryStorageWriteApiConnectException(errorMessage, e);
                }
                logger.warn(errorMessage + " Retrying...");
                mostRecentException = e;
                attempt++;
            }
        } while (attempt < (retry + additionalRetries));
        throw new BigQueryStorageWriteApiConnectException(
                String.format("Exceeded %s attempts to write to table %s ", (retry + additionalRetries), tableName),
                mostRecentException);
    }


    /**
     * Sends errant records to configured DLQ and returns remaining
     * @param input           List of <SinkRecord, JSONObject> input data
     * @param indexToErrorMap Map of record index to error received from api call
     * @param exception       locally built exception to be sent to DLQ topic
     * @return Returns list of good <Sink, JSONObject> filtered from input which needs to be retried. Append row does
     * not write partially even if there is a single failure, good data has to be retried
     */
    private List<Object[]> sendBadRecordsToDlqAndFilterGood(
            List<Object[]> input,
            Map<Integer, String> indexToErrorMap,
            Exception exception) {
        List<Object[]> filteredRecords = new ArrayList<>();
        Set<SinkRecord> recordsToDLQ = new TreeSet<>(Comparator.comparing(SinkRecord::kafkaPartition)
                .thenComparing(SinkRecord::kafkaOffset));

        for (int i = 0; i < input.size(); i++) {
            if (indexToErrorMap.containsKey(i)) {
                recordsToDLQ.add((SinkRecord) input.get(i)[0]);
            } else {
                filteredRecords.add(input.get(i));
            }
        }

        if (getErrantRecordHandler().getErrantRecordReporter() != null) {
            getErrantRecordHandler().sendRecordsToDLQ(recordsToDLQ, exception);
        }

        return filteredRecords;
    }

    /**
     * Converts Row Error to Map
     * @param rowErrors List of row errors
     * @return Returns Map with key as Row index and value as the Row Error Message
     */
    private Map<Integer, String> convertToMap(List<RowError> rowErrors) {
        Map<Integer, String> errorMap = new HashMap<>();

        rowErrors.forEach(rowError -> errorMap.put((int) rowError.getIndex(), rowError.getMessage()));

        return errorMap;
    }
}
