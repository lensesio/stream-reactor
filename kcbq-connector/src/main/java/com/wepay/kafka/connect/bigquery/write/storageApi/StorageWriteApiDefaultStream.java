package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.rpc.Status;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiErrorResponses;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An extension of {@link StorageWriteApiBase} which uses default streams to write data following at least once semantic
 */
public class StorageWriteApiDefaultStream extends StorageWriteApiBase {
    private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiDefaultStream.class);
    ConcurrentMap<String, JsonStreamWriter> tableToStream = new ConcurrentHashMap<>();

    public StorageWriteApiDefaultStream(int retry, long retryWait, BigQueryWriteSettings writeSettings, boolean autoCreateTables) throws IOException {
        super(retry, retryWait, writeSettings, autoCreateTables);
    }

    @Override
    public void preShutdown() {
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
     * Open a default stream on table if not already present
     * @param tableName The tablename on which stream has to be opened
     * @return JSONStreamWriter which would be used to write data to bigquery table
     */
    @VisibleForTesting
    JsonStreamWriter getDefaultStream(String tableName) {
        return tableToStream.computeIfAbsent(tableName, t -> {
            boolean tableCreationAttempted = false;
            int additionalRetriesForTableCreation = 0;
            Exception mostRecentException;
            int attempt = 0;
            do {
                try {
                    if (attempt > 0) {
                        waitRandomTime();
                    }
                    return JsonStreamWriter.newBuilder(t, getWriteClient()).build();
                } catch (Exception e) {
                    if (BigQueryStorageWriteApiErrorResponses.isTableMissing(e.getMessage())
                            && !tableCreationAttempted
                            && getAutoCreateTables()) {
                        logger.info("Attempting to create table {} ...", tableName);
                        tableCreationAttempted = true;
                        // Table takes time to be available for after creation
                        additionalRetriesForTableCreation = ADDITIONAL_RETRIES_TABLE_CREATE_UPDATE;
                        //TODO: Attempt to create table
                        logger.info("Table created {} ...", tableName);
                    } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(e.getMessage()) && !tableCreationAttempted) {
                        logger.error("Failed to create Default stream writer on table {}", tableName);
                        throw new BigQueryStorageWriteApiConnectException("Failed to create Default stream writer on table " + tableName, e);
                    }
                    logger.warn("Failed to create Default stream writer on table {} due to {}. Retry attempt {}...", tableName, e.getMessage(), attempt);
                    mostRecentException = e;
                    attempt++;
                }
            } while (attempt < (retry + additionalRetriesForTableCreation));
            throw new BigQueryStorageWriteApiConnectException(
                    String.format(
                            "Exceeded %s attempts to create Default stream writer on table %s ",
                            (retry + additionalRetriesForTableCreation), tableName
                    ), mostRecentException);
        });
    }

    /**
     * Calls AppendRows and handles exception if the ingestion fails
     * @param tableName  The table to write data to
     * @param rows       List of records in <{@link org.apache.kafka.connect.sink.SinkRecord}, {@link org.json.JSONObject}>
     *                   format. JSONObjects would be sent to api. SinkRecords are requireed for DLQ routing
     * @param streamName The stream to use to write table to table. This will be DEFAULT always.
     */
    @Override
    public void appendRows(TableName tableName, List<Object[]> rows, String streamName) {
        JSONArray jsonArr = new JSONArray();
        JsonStreamWriter writer = getDefaultStream(tableName.toString());

        rows.forEach(item -> jsonArr.put(item[1]));

        logger.debug("Sending {} records to write Api default stream on {} ...", rows.size(), tableName);
        int attempt = 0;
        Exception mostRecentException = null;
        boolean tableCreationAttempted = false;
        int additionalRetriesForTableCreation = 0;
        do {
            try {
                if (attempt > 0) {
                    waitRandomTime();
                }
                logger.trace("Sending records to Storage API writer...");
                ApiFuture<AppendRowsResponse> response = writer.append(jsonArr);
                AppendRowsResponse writeResult = response.get();
                logger.trace("Received response from Storage API writer...");

                if (writeResult.hasUpdatedSchema()) {
                    logger.warn("Sent records schema does not match with table schema, will attempt to update schema");
                    //TODO: Update schema attempt Once
                } else if (writeResult.hasError()) {
                    Status errorStatus = writeResult.getError();
                    String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, errorStatus.getMessage());
                    if (BigQueryStorageWriteApiErrorResponses.isMalformedErrorCode(errorStatus.getCode())) {
                        // Fail on data error
                        throw new BigQueryStorageWriteApiConnectException(tableName.getTable(), writeResult.getRowErrorsList());
                        //TODO: DLQ handling
                    } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(errorStatus.getMessage())) {
                        // Fail on non-retriable error
                        logger.error(errorMessage);
                        throw new BigQueryStorageWriteApiConnectException(errorMessage);
                    }
                    logger.warn(errorMessage + " Retry attempt " + attempt);
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
                if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(e)) {
                    // Fail on data error
                    throw new BigQueryStorageWriteApiConnectException(tableName.getTable(), getRowErrorMapping(e));
                    //TODO: DLQ handling
                } else if (BigQueryStorageWriteApiErrorResponses.isTableMissing(message) && !tableCreationAttempted && getAutoCreateTables()) {
                    logger.info("Attempting to create table {} ...", tableName);
                    tableCreationAttempted = true;
                    // Table takes time to be available for after creation
                    additionalRetriesForTableCreation = ADDITIONAL_RETRIES_TABLE_CREATE_UPDATE;
                    //TODO: Attempt to create table
                    logger.info("Table created {} ...", tableName);
                } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(message) && !tableCreationAttempted) {
                    // Fail on non-retriable error
                    logger.error(errorMessage);
                    throw new BigQueryStorageWriteApiConnectException(errorMessage, e);
                }
                logger.warn(errorMessage + "Retry attempt " + attempt);
                mostRecentException = e;
                attempt++;
            }
        } while (attempt < (retry + additionalRetriesForTableCreation));
        throw new BigQueryStorageWriteApiConnectException(
                String.format("Exceeded %s attempts to write to table %s ", (retry + additionalRetriesForTableCreation), tableName),
                mostRecentException);
    }
}
