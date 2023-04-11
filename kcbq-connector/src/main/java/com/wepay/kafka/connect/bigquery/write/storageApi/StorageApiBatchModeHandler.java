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

public class StorageApiBatchModeHandler {

    private static final Logger logger = LoggerFactory.getLogger(StorageApiBatchModeHandler.class);
    private final StorageWriteApiApplicationStream streamApi;

    private List<String> tableNames;

    public StorageApiBatchModeHandler(StorageWriteApiApplicationStream streamApi, BigQuerySinkTaskConfig config) {
        this.streamApi = streamApi;
        addTableNames(config);
    }

    private void addTableNames(BigQuerySinkTaskConfig config) {
        String projectId = config.getString(BigQuerySinkTaskConfig.PROJECT_CONFIG);
        tableNames = config.getList(BigQuerySinkConfig.TOPICS_CONFIG)
                .stream()
                .map(topic -> {
                    String[] dataSetAndTopic = TableNameUtils.getDataSetAndTableName(config, topic);
                    return TableName.of(projectId, dataSetAndTopic[0], dataSetAndTopic[1]).toString();
                })
                .collect(Collectors.toList());
    }

    public String getCurrentStream(String tableName) {
        String streamName = this.streamApi.getCurrentStreamForTable(tableName);
        logger.debug("Current stream for table " + tableName + " is " + streamName);
        return streamName;
    }

    public void createNewStream() {
        logger.trace("Storage Write API create stream attempt by scheduler");
        tableNames.forEach(this::createNewStreamForTable);
    }

    private void createNewStreamForTable(String tableName) {
        if (streamApi.mayBeCreateStream(tableName)) {
            logger.debug("Created new stream for table " + tableName);
        } else {
            logger.debug("Not creating new stream for table " + tableName);
        }
    }

    public void updateOffsetsForStream(String tableName, String streamName, Map<TopicPartition, OffsetAndMetadata> offsetInfo) {
        this.streamApi.updateOffsetsForStream(tableName, streamName, offsetInfo);
    }

    public Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets() {
        return this.streamApi.getCommitableOffsets();
    }

}
