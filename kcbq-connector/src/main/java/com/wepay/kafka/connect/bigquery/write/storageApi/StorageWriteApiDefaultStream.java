package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.rpc.Status;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiErrorResponses;
import io.debezium.data.Json;
import org.apache.arrow.flatbuf.Int;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
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

    public StorageWriteApiDefaultStream(int retry, long retryWait, BigQueryWriteSettings writeSettings, boolean autoCreateTables, ErrantRecordHandler errantRecordHandler) throws IOException {
        super(retry, retryWait, writeSettings, autoCreateTables, errantRecordHandler);
    }

    /**
     * Open a default stream on table if not already present
     *
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

        for (Object[] item : rows) {
            jsonArr.put(item[1]);
        }

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
                if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(message)) {
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
                } else if (BigQueryStorageWriteApiErrorResponses.isTableMissing(message) && !tableCreationAttempted && getAutoCreateTables()) {
                    logger.info("Attempting to create table {} ...", tableName);
                    tableCreationAttempted = true;
                    // Table takes time to be available for after creation
                    additionalRetriesForTableCreation = 30;
                    //TODO: Attempt to create table
                    logger.info("Table created {} ...", tableName);
                } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(message) && !tableCreationAttempted) {
                    // Fail on non-retriable error
                    logger.error(errorMessage);
                    throw new BigQueryStorageWriteApiConnectException(errorMessage, e);
                }
                logger.warn(errorMessage + " Retrying...");
                mostRecentException = e;
                attempt++;
            }
        } while (attempt < (retry + additionalRetriesForTableCreation));
        throw new BigQueryStorageWriteApiConnectException(
                String.format("Exceeded %s attempts to write to table %s ", (retry + additionalRetriesForTableCreation), tableName),
                mostRecentException);
    }


    /**
     * Sends errant records to configured DLQ and returns remaining
     * @param input List of <SinkRecord, JSONObject> input data
     * @param indexToErrorMap Map of record index to error received from api call
     * @param exception locally built exception to be sent to DLQ topic
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
