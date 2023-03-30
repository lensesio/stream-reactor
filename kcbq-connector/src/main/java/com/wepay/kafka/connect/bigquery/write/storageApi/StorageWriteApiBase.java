package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.*;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Base class which handles data ingestion to bigquery tables using different kind of streams
 */
public abstract class StorageWriteApiBase {
    Logger logger = LoggerFactory.getLogger(StorageWriteApiBase.class);
    private final BigQueryWriteClient writeClient;

    private final int retry;

    private final long retryWait;

    protected final boolean autoCreateTables;

    /**
     * @param retry             How many retries to make in the event of a 500/503 error.
     * @param retryWait         How long to wait in between retries.
     * @param writeSettings     Write Settings for stream which carry authentication and other header information
     */
    public StorageWriteApiBase(int retry, long retryWait, BigQueryWriteSettings writeSettings, boolean autoCreateTables) {
        this.retry = retry;
        this.retryWait = retryWait;
        this.autoCreateTables = autoCreateTables;
        try {
            this.writeClient = BigQueryWriteClient.create(writeSettings);
        } catch (IOException e) {
            logger.error("Failed to create Big Query Storage Write API write client due to {}", e.getMessage());
            throw new BigQueryStorageWriteApiConnectException("Failed to create Big Query Storage Write API write client", e);
        }

    }

    /**
     * Handles required initialization steps and goes to append records to table
     *
     * @param tableName  The table to write data to
     * @param rows       The records to write
     * @param streamName The stream to use to write table to table.
     */
    public void initializeAndWriteRecords(TableName tableName, List<Object[]> rows, String streamName) {
        // TODO: Streams are created on table. So table must be present. We will add a check here and attempt to create table and cache it
        appendRows(tableName, rows, streamName);
    }

    /**
     * @param tableName  The table to write data to
     * @param rows       The records to write
     * @param streamName The stream to use to write table to table.
     */
    abstract public void appendRows(TableName tableName, List<Object[]> rows, String streamName);

    public BigQueryWriteClient getWriteClient() {
        return this.writeClient;
    }
}
