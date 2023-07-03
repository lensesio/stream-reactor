package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.rpc.Status;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;

import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiErrorResponses;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An extension of {@link StorageWriteApiBase} which uses default streams to write data following at least once semantic
 */
public class StorageWriteApiDefaultStream extends StorageWriteApiBase {
    private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiDefaultStream.class);
    ConcurrentMap<String, JsonStreamWriter> tableToStream = new ConcurrentHashMap<>();

    public StorageWriteApiDefaultStream(int retry,
                                        long retryWait,
                                        BigQueryWriteSettings writeSettings,
                                        boolean autoCreateTables,
                                        ErrantRecordHandler errantRecordHandler,
                                        SchemaManager schemaManager,
                                        boolean attemptSchemaUpdate) {
        super(retry, retryWait, writeSettings, autoCreateTables, errantRecordHandler, schemaManager, attemptSchemaUpdate);

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
     *
     * @param table The table on which stream has to be opened
     * @param rows  The input rows (would be sent while table creation to identify schema)
     * @return JSONStreamWriter which would be used to write data to bigquery table
     */
    @VisibleForTesting
    JsonStreamWriter getDefaultStream(TableName table, List<Object[]> rows) {
        String tableName = table.toString();
        return tableToStream.computeIfAbsent(tableName, t -> {
            StorageWriteApiRetryHandler retryHandler = new StorageWriteApiRetryHandler(table, getSinkRecords(rows), retry, retryWait);
            do {
                try {
                    return JsonStreamWriter.newBuilder(t, getWriteClient()).build();
                } catch (Exception e) {
                    String baseErrorMessage = String.format(
                            "Failed to create Default stream writer on table %s due to %s",
                            tableName,
                            e.getMessage());
                    retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(baseErrorMessage, e));
                    if (BigQueryStorageWriteApiErrorResponses.isTableMissing(e.getMessage()) && getAutoCreateTables()) {
                        retryHandler.attemptTableOperation(schemaManager::createTable);
                    } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(e.getMessage())) {
                        throw retryHandler.getMostRecentException();
                    }
                    logger.warn(baseErrorMessage + " Retry attempt {}...", retryHandler.getAttempt());
                }
            } while (retryHandler.mayBeRetry());
            throw new BigQueryStorageWriteApiConnectException(
                    String.format(
                            "Exceeded %s attempts to create Default stream writer on table %s ",
                            retryHandler.getAttempt(), tableName),
                    retryHandler.getMostRecentException());
        });
    }

    /**
     * Calls AppendRows and handles exception if the ingestion fails
     *
     * @param tableName  The table to write data to
     * @param rows       List of records in {@link org.apache.kafka.connect.sink.SinkRecord}, {@link org.json.JSONObject}
     *                   format. JSONObjects would be sent to api. SinkRecords are requireed for DLQ routing
     * @param streamName The stream to use to write table to table. This will be DEFAULT always.
     */
    @Override
    public void appendRows(TableName tableName, List<Object[]> rows, String streamName) {
        JSONArray jsonArr;
        StorageWriteApiRetryHandler retryHandler = new StorageWriteApiRetryHandler(tableName, getSinkRecords(rows), retry, retryWait);
        logger.debug("Sending {} records to write Api default stream on {} ...", rows.size(), tableName);

        do {
            try {
                jsonArr = new JSONArray();
                for (Object[] item : rows) {
                    jsonArr.put(item[1]);
                }
                logger.trace("Sending records to Storage API writer...");
                JsonStreamWriter writer = getDefaultStream(tableName, rows);
                ApiFuture<AppendRowsResponse> response = writer.append(jsonArr);
                AppendRowsResponse writeResult = response.get();
                logger.trace("Received response from Storage API writer...");

                if (writeResult.hasUpdatedSchema()) {
                    logger.warn("Sent records schema does not match with table schema, will attempt to update schema");
                    if(!canAttemptSchemaUpdate()) {
                        throw new BigQueryStorageWriteApiConnectException("Connector is not configured to perform schema updates.");
                    }
                    retryHandler.attemptTableOperation(schemaManager::updateSchema);
                } else if (writeResult.hasError()) {
                    Status errorStatus = writeResult.getError();
                    String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, errorStatus.getMessage());
                    retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(errorMessage));
                    if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(errorMessage)) {
                        rows = mayBeHandleDlqRoutingAndFilterRecords(rows, convertToMap(writeResult.getRowErrorsList()), tableName.getTable());
                        if (rows.isEmpty()) {
                            return;
                        }
                    } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(errorStatus.getMessage())) {
                        // Fail on non-retriable error
                        logger.error(errorMessage);
                        throw retryHandler.getMostRecentException();
                    }
                    logger.warn(errorMessage + " Retry attempt " + retryHandler.getAttempt());
                } else {
                    logger.debug("Call to write Api default stream completed after {} retries!", retryHandler.getAttempt());
                    return;
                }
            } catch (BigQueryStorageWriteApiConnectException exception) {
                throw exception;
            } catch (Exception e) {
                String message = e.getMessage();
                String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, message);
                retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(errorMessage, e));

                if (canAttemptSchemaUpdate()
                        && BigQueryStorageWriteApiErrorResponses.isMalformedRequest(message)
                        && BigQueryStorageWriteApiErrorResponses.hasInvalidSchema(getRowErrorMapping(e).values())) {
                    logger.warn("Sent records schema does not match with table schema, will attempt to update schema");
                    retryHandler.attemptTableOperation(schemaManager::updateSchema);
                } else if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(message)) {
                    rows = mayBeHandleDlqRoutingAndFilterRecords(rows, getRowErrorMapping(e), tableName.getTable());
                    if (rows.isEmpty()) {
                        return;
                    }
                } else if (BigQueryStorageWriteApiErrorResponses.isStreamClosed(message)) {
                    // Streams can get autoclosed if there occurs any issues, we should delete the cached stream
                    // so that a new one gets created on retry.
                    closeAndDelete(tableName.toString());
                } else if (BigQueryStorageWriteApiErrorResponses.isTableMissing(message) && getAutoCreateTables()) {
                    retryHandler.attemptTableOperation(schemaManager::createTable);
                } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(message)) {
                    // Fail on non-retriable error
                    throw retryHandler.getMostRecentException();
                }
                logger.warn(errorMessage + " Retry attempt " + retryHandler.getAttempt());
            }
        } while (retryHandler.mayBeRetry());
        throw new BigQueryStorageWriteApiConnectException(
                String.format("Exceeded %s attempts to write to table %s ", retryHandler.getAttempt(), tableName),
                retryHandler.getMostRecentException());
    }
}
