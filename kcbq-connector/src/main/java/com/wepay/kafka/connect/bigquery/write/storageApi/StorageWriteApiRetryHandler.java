package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * A POJO to handle retries by the Storage Write Api
 */
public class StorageWriteApiRetryHandler {
    private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiRetryHandler.class);
    private boolean tableCreationOrUpdateAttempted;
    private int additionalRetries;
    private int additionalWait;
    private BigQueryStorageWriteApiConnectException mostRecentException;
    private TableName table;
    private List<SinkRecord> records;
    private static final int ADDITIONAL_RETRIES_TABLE_CREATE_UPDATE = 30;
    private static final int ADDITIONAL_RETRIES_WAIT_TABLE_CREATE_UPDATE = 30000;
    private int currentAttempt;
    private final Random random;
    private int userConfiguredRetry;
    private long userConfiguredRetryWait;

    public StorageWriteApiRetryHandler(TableName table, List<SinkRecord> records, int retry, long retryWait) {
        tableCreationOrUpdateAttempted = false;
        additionalRetries = 0;
        additionalWait = 0;
        mostRecentException = null;
        currentAttempt = 0;
        this.table = table;
        this.records = records;
        this.userConfiguredRetry = retry;
        this.userConfiguredRetryWait = retryWait;
        this.random = new Random();
    }

    public boolean isTableCreationOrUpdateAttempted() {
        return tableCreationOrUpdateAttempted;
    }

    public void setTableCreationOrUpdateAttempted(boolean tableCreationOrUpdateAttempted) {
        this.tableCreationOrUpdateAttempted = tableCreationOrUpdateAttempted;
    }

    public void setAdditionalRetriesAndWait() {
        this.additionalRetries = ADDITIONAL_RETRIES_TABLE_CREATE_UPDATE;
        this.additionalWait = ADDITIONAL_RETRIES_WAIT_TABLE_CREATE_UPDATE;
    }

    public BigQueryStorageWriteApiConnectException getMostRecentException() {
        return mostRecentException;
    }

    public void setMostRecentException(BigQueryStorageWriteApiConnectException mostRecentException) {
        this.mostRecentException = mostRecentException;
    }

    public int getAttempt() {
        return this.currentAttempt;
    }

    public TableId getTableId() {
        return TableNameUtils.tableId(table);
    }

    public void setTable(TableName table) {
        this.table = table;
    }

    public List<SinkRecord> getRecords() {
        return records;
    }

    public void setRecords(List<SinkRecord> records) {
        this.records = records;
    }

    private void waitRandomTime() throws InterruptedException {
        Thread.sleep(userConfiguredRetryWait + additionalWait + random.nextInt(1000));
    }

    public boolean mayBeRetry() {
        if (currentAttempt < (userConfiguredRetry + additionalRetries)) {
            currentAttempt++;
            try {
                waitRandomTime();
            } catch (InterruptedException e) {
                logger.warn("Thread interrupted while waiting for random time...");
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Attempts to create table
     * @param tableOperation lambda of the table operation to perform
     */
    public void attemptTableOperation(TableOperation tableOperation) {
        try {
            if (!isTableCreationOrUpdateAttempted()) {
                setTableCreationOrUpdateAttempted(true);
                tableOperation.performOperation(getTableId(), getRecords());
                // Table takes time to be available for after creation
                setAdditionalRetriesAndWait();
            } else {
                logger.info("Skipping multiple table creation attempts");
            }
        } catch (BigQueryException exception) {
            throw new BigQueryStorageWriteApiConnectException(
                    "Failed to create table " + getTableId(), exception);
        }
    }
}

interface TableOperation {
    void performOperation(TableId tableId, List<SinkRecord> records);
}
