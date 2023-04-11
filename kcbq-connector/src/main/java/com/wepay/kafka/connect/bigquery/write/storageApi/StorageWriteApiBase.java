package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Base class which handles data ingestion to bigquery tables using different kind of streams
 */
public abstract class StorageWriteApiBase {
    private final ErrantRecordHandler errantRecordHandler;
    private final SchemaManager schemaManager;
    Logger logger = LoggerFactory.getLogger(StorageWriteApiBase.class);
    private BigQueryWriteClient writeClient;

    protected final int retry;

    protected final long retryWait;

    private final boolean autoCreateTables;

    private final Random random;

    private final BigQueryWriteSettings writeSettings;

    /**
     * @param retry               How many retries to make in the event of a 500/503 error.
     * @param retryWait           How long to wait in between retries.
     * @param writeSettings       Write Settings for stream which carry authentication and other header information
     * @param autoCreateTables    boolean flag set if table should be created automatically
     * @param errantRecordHandler Used to handle errant records
     */
    public StorageWriteApiBase(int retry,
                               long retryWait,
                               BigQueryWriteSettings writeSettings,
                               boolean autoCreateTables,
                               ErrantRecordHandler errantRecordHandler,
                               SchemaManager schemaManager) {
        this.retry = retry;
        this.retryWait = retryWait;
        this.autoCreateTables = autoCreateTables;
        this.random = new Random();
        this.writeSettings = writeSettings;
        this.errantRecordHandler = errantRecordHandler;
        this.schemaManager = schemaManager;
        this.writeClient = getWriteClient();
    }

    /**
     * Handles required initialization steps and goes to append records to table
     * @param tableName  The table to write data to
     * @param rows       The records to write
     * @param streamName The stream to use to write table to table.
     */
    public void initializeAndWriteRecords(TableName tableName, List<Object[]> rows, String streamName) {
        // TODO: Streams are created on table. So table must be present. We will add a check here and attempt to create table and cache it
        appendRows(tableName, rows, streamName);
    }

    /**
     * Gets called on task.stop() and should have resource cleanup logic.
     */
    abstract public void shutdown();

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
    public BigQueryWriteClient getWriteClient() {
        if (this.writeClient == null) {
            try {
                this.writeClient = BigQueryWriteClient.create(writeSettings);
            } catch (IOException e) {
                logger.error("Failed to create Big Query Storage Write API write client due to {}", e.getMessage());
                throw new BigQueryStorageWriteApiConnectException("Failed to create Big Query Storage Write API write client", e);
            }
        }
        return this.writeClient;
    }

    /**
     * Verifies the exception object and returns row-wise error map
     * @param exception if the exception is not of expected type
     * @return Map of row index to error message detail
     */
    protected Map<Integer, String> getRowErrorMapping(Exception exception) {
        if (exception instanceof ExecutionException) {
            exception = (Exceptions.AppendSerializtionError) exception.getCause();
        }
        if (exception instanceof Exceptions.AppendSerializtionError) {
            return ((Exceptions.AppendSerializtionError) exception).getRowIndexToErrorMessage();
        } else {
            throw new BigQueryStorageWriteApiConnectException("Exception is not an instance of Exceptions.AppendSerializtionError");
        }
    }

    /**
     * Wait at least {@link #retryWait}, with up to an additional 1 second of random jitter.
     * @param additionalWait Any additional wait on top of user configured wait.
     *                      This is used while making updates to bigquery table as changes don't reflect immediately.
     * @throws InterruptedException if interrupted.
     */
    protected void waitRandomTime(int additionalWait) throws InterruptedException {
        Thread.sleep(retryWait + additionalWait + random.nextInt(1000));
    }

    protected boolean getAutoCreateTables() {
        return this.autoCreateTables;
    }

    protected ErrantRecordHandler getErrantRecordHandler() {
        return this.errantRecordHandler;

    }

    /**
     * Attempts to create table
     * @param tableId TableId of the table to be created
     * @param records List of records to get schema from
     */
    protected void attemptTableCreation(TableId tableId, List<SinkRecord> records) {
        try {
            schemaManager.createTable(tableId, records);
        } catch (BigQueryException exception) {
            throw new BigQueryStorageWriteApiConnectException(
                    "Failed to create table " + tableId, exception);
        }
    }

    /**
     * Attempts to update table schema
     * @param tableId TableId of the table where schema is to be updated
     * @param records List of records to get input schema from
     */
    protected void attemptSchemaUpdate(TableId tableId, List<SinkRecord> records) {
        try {
            schemaManager.updateSchema(tableId, records);
        } catch (BigQueryException exception) {
            throw new BigQueryStorageWriteApiConnectException(
                    "Failed to update table schema for: " + tableId, exception);
        }
    }
}
