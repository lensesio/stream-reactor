package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.rpc.Status;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiErrorResponses;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * An extension of {@link StorageWriteApiBase} which uses default streams to write data following at least once semantic
 */
public class StorageWriteApiDefaultStream extends StorageWriteApiBase {
    private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiDefaultStream.class);
    ConcurrentMap<String, JsonStreamWriter> tableToStream = new ConcurrentHashMap<>();

    public StorageWriteApiDefaultStream(int retry, long retryWait, BigQueryWriteSettings writeSettings, boolean autoCreateTables) throws IOException {
        super(retry, retryWait, writeSettings, autoCreateTables);
    }

    /**
     * Open a default stream on table if not already present
     *
     * @param tableName The tablename on which stream has to be opened
     * @return JSONStreamWriter which would be used to write data to bigquery table
     */
    private JsonStreamWriter getDefaultStream(String tableName) {
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
                            && autoCreateTables) {
                        logger.info("Attempting to create table {} ...", tableName);
                        tableCreationAttempted = true;
                        // Table takes time to be available for after creation
                        additionalRetriesForTableCreation = 30;
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
     *
     * @param tableName  The table to write data to
     * @param rows       The records to write
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
                ApiFuture<AppendRowsResponse> response = writer.append(jsonArr);
                AppendRowsResponse writeResult = response.get();

                if (writeResult.hasUpdatedSchema()) {
                    logger.warn("Sent records schema does not match with table schema, will attempt to update schema");
                    //TODO: Update schema attempt Once
                } else if (writeResult.hasError()) {
                    Status errorStatus = writeResult.getError();
                    if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(errorStatus.getMessage())) {
                        // Fail on non-retriable error
                        String message = String.format("Failed to write rows on table %s due to %s", tableName, errorStatus.getMessage());
                        logger.error(message);
                        throw new BigQueryStorageWriteApiConnectException(message);
                    } else if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(errorStatus.getCode())) {
                        // Fail on data error
                        throw new BigQueryStorageWriteApiConnectException(tableName.getTable(), writeResult.getRowErrorsList());
                        //TODO: DLQ handling
                    }
                    logger.warn("Failed to write rows on table {} due to {}. Retrying...", tableName, errorStatus.getMessage());
                    mostRecentException = new BigQueryStorageWriteApiConnectException(errorStatus.getMessage());
                    attempt++;
                } else {
                    logger.debug("Call to write Api default stream completed after {} retries!", attempt);
                    return;
                }
            } catch (Exception e) {
                String message = e.getMessage();
                if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(e)) {
                    // Fail on data error
                    throw new BigQueryStorageWriteApiConnectException(tableName.getTable(), getRowErrorMapping(e));
                    //TODO: DLQ handling
                } else if (BigQueryStorageWriteApiErrorResponses.isTableMissing(message) && !tableCreationAttempted && autoCreateTables) {
                    logger.info("Attempting to create table {} ...", tableName);
                    tableCreationAttempted = true;
                    // Table takes time to be available for after creation
                    additionalRetriesForTableCreation = 30;
                    //TODO: Attempt to create table
                    logger.info("Table created {} ...", tableName);
                } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(message) && !tableCreationAttempted) {
                    // Fail on non-retriable error
                    String msg = String.format("Failed to write rows on table %s due to %s", tableName, message);
                    if(message.contains("INTERNAL")) {
                        logger.info("finally");
                    }
                    logger.error(msg);
                    throw new BigQueryStorageWriteApiConnectException(msg, e);
                }
                logger.warn("Failed to write rows on table {} due to {}. Retrying...", tableName, message);
                mostRecentException = e;
                attempt++;
            }
        } while (attempt < (retry + additionalRetriesForTableCreation));
        throw new BigQueryStorageWriteApiConnectException(
                String.format("Exceeded configured %s attempts to write to table %s ", (retry + additionalRetriesForTableCreation), tableName),
                mostRecentException);
    }

}
