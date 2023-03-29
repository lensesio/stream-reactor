package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import io.grpc.Status;
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

    public StorageWriteApiDefaultStream(int retry, long retryWait, BigQueryWriteSettings writeSettings) throws IOException {
        super(retry, retryWait, writeSettings);
    }

    private JsonStreamWriter getDefaultStream(String tableName) {
        return tableToStream.computeIfAbsent(tableName, t -> {
            try {
                return JsonStreamWriter.newBuilder(t, getWriteClient()).build();
            } catch (Exception e) {
                // filter exception to verify if table creation is needed and configured else fail
                logger.error("Failed to create Default stream writer on table {}", tableName);
                throw new BigQueryConnectException("Failed to create Default stream writer on table "+ tableName, e);
            }
        });
    }

    /**
     * Calls AppendRows and handles exception if the ingestion fails
     * @param tableName The table to write data to
     * @param rows The records to write
     * @param streamName The stream to use to write table to table. This will be DEFAULT always.
     */
    @Override
    public void appendRows(TableName tableName, List<Object[]> rows, String streamName) {
        JSONArray jsonArr = new JSONArray();
        JsonStreamWriter writer = getDefaultStream(tableName.toString());

        // put JSONObject into JsonArray
        rows.forEach(item -> jsonArr.put(item[1]));

        logger.debug("Sending {} records to write Api default stream on {} ...", rows.size(), tableName);
        try {
            ApiFuture<AppendRowsResponse> response = writer.append(jsonArr);
            AppendRowsResponse writeResult = response.get();

            if (writeResult.hasUpdatedSchema()) {
                logger.warn("Sent records schema does not match with table schema, will attempt to update schema");
                //TODO: Update schema attempt Once
            } else if (writeResult.hasError()) {
                // is malformed error -> check if DLQ configured, send to DLQ and retry remaining batch. If not, retry whole
                // is Aborted or Internal - retry
                if (writeResult.getRowErrorsCount() > 0) {
                    for (RowError error : writeResult.getRowErrorsList()) {
                        logger.error("Row " + error.getIndex() + " has error : " + error.getMessage());
                    }
                }
                //TODO: DLQ handling here
            } else {
                logger.debug("Call to write Api default stream completed without errors!");
            }
        } catch(Exception e) {
            // If non-retriable exception : Interrupt, Descriptor
            String message = "Failed to write rows to table "+ tableName.getTable() + " due to " + e.getMessage();
            logger.error(message);
            throw new BigQueryStorageWriteApiConnectException(message, e);

            // if retriable IO Exception
            // if execution exception
//            if (e instanceof  ExecutionException) {
//                //TODO: Exception handling here
//                logger.error(e.getCause().getMessage());
//                if (e.getCause() instanceof Exceptions.AppendSerializtionError) {
//                    Exceptions.AppendSerializtionError exception = (Exceptions.AppendSerializtionError) e.getCause();
//                    if (exception.getStatus().getCode().equals(Status.Code.INVALID_ARGUMENT)) {
//                        // User actionable error
//                        for (Map.Entry rowIndexToError : exception.getRowIndexToErrorMessage().entrySet()) {
//                            logger.error("User actionable error on : " + jsonArr.put(rowIndexToError.getKey())
//                                    + "  as the data hit an issue : " + rowIndexToError.getValue());
//                        }
//
//                    } else {
//                        logger.trace("Exception is not due to invalid argument");
//                        logger.error(exception.getStatus().getDescription());
//                        throw new RuntimeException(e);
//                    }
//                } else {
//                    logger.info("Exception is not of type Exceptions.AppendSerializtionError");
//                    logger.error(e.getCause().getMessage());
//                    throw new RuntimeException(e);
//                }
//            }

        }

//        catch (Descriptors.DescriptorValidationException | InterruptedException e) {
//            //TODO: Exception handling here
//            String message = "Failed to write rows to table "+ tableName.getTable() + " due to " + e.getMessage();
//            logger.error(message);
//            throw new BigQueryStorageWriteApiConnectException(message, e);
//        } catch (IOException ) {


       // }
    }

}
