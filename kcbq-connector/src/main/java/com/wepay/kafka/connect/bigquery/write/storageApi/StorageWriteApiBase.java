package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.List;
import java.util.Map;

/**
 * Base class which handles data ingestion to bigquery tables using different kind of streams
 */
public abstract class StorageWriteApiBase {
    Logger logger = LoggerFactory.getLogger(StorageWriteApiBase.class);
    private BigQueryWriteClient writeClient;
    protected final int retry;
    protected final long retryWait;
    private final boolean autoCreateTables;
    private final Random random;
    private final BigQueryWriteSettings writeSettings;

    static final int ADDITIONAL_RETRIES_TABLE_CREATE_UPDATE = 30;

    /**
     * @param retry         How many retries to make in the event of a retriable error.
     * @param retryWait     How long to wait in between retries.
     * @param writeSettings Write Settings for stream which carry authentication and other header information
     */
    public StorageWriteApiBase(int retry, long retryWait, BigQueryWriteSettings writeSettings, boolean autoCreateTables) {
        this.retry = retry;
        this.retryWait = retryWait;
        this.autoCreateTables = autoCreateTables;
        this.random = new Random();
        this.writeSettings = writeSettings;
        try {
            this.writeClient = getWriteClient();
        } catch (IOException e) {
            logger.error("Failed to create Big Query Storage Write API write client due to {}", e.getMessage());
            throw new BigQueryStorageWriteApiConnectException("Failed to create Big Query Storage Write API write client", e);
        }
    }

    /**
     * Handles required initialization steps and goes to append records to table
     * @param tableName  The table to write data to
     * @param rows       List of records in <{@link org.apache.kafka.connect.sink.SinkRecord}, {@link org.json.JSONObject}>
     *                   format. JSONObjects would be sent to api. SinkRecords are requireed for DLQ routing
     * @param streamName The stream to use to write table to table.
     */
    public void initializeAndWriteRecords(TableName tableName, List<Object[]> rows, String streamName) {
        verifyRows(rows);
        appendRows(tableName, rows, streamName);
    }

    /**
     * @param tableName  The table to write data to
     * @param rows       The records to write
     * @param streamName The stream to use to write table to table.
     */
    abstract public void appendRows(TableName tableName, List<Object[]> rows, String streamName);

    /**
     * Creates Storage Api write client which carries all write settings information
     * @return Returns BigQueryWriteClient object
     * @throws IOException
     */
    public BigQueryWriteClient getWriteClient() throws IOException {
        if(this.writeClient == null) {
            this.writeClient = BigQueryWriteClient.create(writeSettings);
        }
        return this.writeClient;
    }

    private void verifyRows(List<Object[]> rows) {
        rows.forEach(row -> {
            if (row == null || (row.length != 2)) {
                throw new BigQueryStorageWriteApiConnectException(String.format(
                        "Row verification failed for {}. Expected row with exactly 2 items", row.toString()));
            }
        });
    }

    /**
     * Verifies the exception object and returns row-wise error map
     * @param exception if the exception is not of expected type
     * @return Map of row index to error message detail
     */
    protected Map<Integer, String> getRowErrorMapping(Exception exception) {
        if (exception instanceof Exceptions.AppendSerializtionError) {
            return ((Exceptions.AppendSerializtionError) exception).getRowIndexToErrorMessage();
        } else {
            throw new BigQueryStorageWriteApiConnectException(
                    "Exception is not an instance of Exceptions.AppendSerializtionError", exception);
        }
    }

    /**
     * Wait at least {@link #retryWait}, with up to an additional 1 second of random jitter.
     * @throws InterruptedException if interrupted.
     */
    protected void waitRandomTime() throws InterruptedException {
        Thread.sleep(retryWait + random.nextInt(1000));
    }

    protected boolean getAutoCreateTables() {
        return this.autoCreateTables;
    }
}
