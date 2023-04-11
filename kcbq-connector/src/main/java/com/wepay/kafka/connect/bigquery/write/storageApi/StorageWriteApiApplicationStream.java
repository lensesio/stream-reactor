package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public abstract class StorageWriteApiApplicationStream extends StorageWriteApiBase {

    Logger logger = LoggerFactory.getLogger(StorageWriteApiApplicationStream.class);

    public StorageWriteApiApplicationStream(int retry,
                                            long retryWait,
                                            BigQueryWriteSettings writeSettings,
                                            boolean autoCreateTables,
                                            ErrantRecordHandler errantRecordHandler,
                                            SchemaManager schemaManager) {
        super(retry, retryWait, writeSettings, autoCreateTables, errantRecordHandler, schemaManager);
    }

    public abstract String getCurrentStreamForTable(String tableName);

    public abstract Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets();

    public void updateOffsetsForStream(String tableName, String streamName, Map<TopicPartition, OffsetAndMetadata> offsetInfo) {
        String msg = String.format("Stream %s does not support updating offsets on table %s", streamName, tableName);
        logger.error(msg);
        throw new BigQueryStorageWriteApiConnectException(msg);
    }

    public abstract boolean mayBeCreateStream(String tableName);

}
