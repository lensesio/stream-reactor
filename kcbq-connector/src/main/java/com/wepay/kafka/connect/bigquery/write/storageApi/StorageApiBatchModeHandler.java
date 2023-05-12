package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles all operations related to Batch Storage Write API
 */
public class StorageApiBatchModeHandler {

    private static final Logger logger = LoggerFactory.getLogger(StorageApiBatchModeHandler.class);
    private final StorageWriteApiApplicationStream streamApi;
    private List<String> tableNames;

    public StorageApiBatchModeHandler(StorageWriteApiApplicationStream streamApi, BigQuerySinkTaskConfig config) {
        this.streamApi = streamApi;
        this.tableNames = TableNameUtils.getAllTableNames(config);
    }

    /**
     * Used by the scheduler to create stream on all tables
     */
    public void createNewStream() {
        logger.trace("Storage Write API create stream attempt by scheduler");
        tableNames.forEach(this::createNewStreamForTable);
    }

    /**
     * Creates a new stream for given table if required.
     *
     * @param tableName Name of tha table in project/dataset/tablename format
     */
    private void createNewStreamForTable(String tableName) {
        if (streamApi.mayBeCreateStream(tableName, null)) {
            logger.debug("Created new stream for table " + tableName);
        } else {
            logger.debug("Not creating new stream for table " + tableName);
        }
    }

    /**
     * Saves the offsets assigned to a particular stream on a table. This is required to commit offsets sequentially
     * even if the execution takes place in parallel at different times.
     *
     * @param tableName Name of tha table in project/dataset/tablename format
     * @param rows      Records which would be written to table {tableName} sent to define schema if table creation is
     *                  attempted
     * @return Returns the streamName on which offsets are updated
     */
    public String updateOffsetsOnStream(
            String tableName,
            List<Object[]> rows) {
        logger.trace("Updating offsets on current stream of table {}", tableName);
        return this.streamApi.updateOffsetsOnStream(tableName, rows);
    }

    /**
     * Gets offsets which are committed on BigQuery table.
     *
     * @return Returns Map of topic, partition, offset mapping
     */
    public Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets() {
        logger.trace("Getting list of commitable offsets for batch mode...");
        return this.streamApi.getCommitableOffsets();
    }

}
