package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Class which should be extended while working with Applciation Streams
 */
public abstract class StorageWriteApiApplicationStream extends StorageWriteApiBase {

    private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiApplicationStream.class);

    public StorageWriteApiApplicationStream(int retry,
                                            long retryWait,
                                            BigQueryWriteSettings writeSettings,
                                            boolean autoCreateTables,
                                            ErrantRecordHandler errantRecordHandler,
                                            SchemaManager schemaManager) {
        super(retry, retryWait, writeSettings, autoCreateTables, errantRecordHandler, schemaManager);
    }

    public abstract Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets();

    public abstract String updateOffsetsOnStream(String tableName, Map<TopicPartition, OffsetAndMetadata> offsetInfo);

    public abstract boolean mayBeCreateStream(String tableName);

}
