package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
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
                                            SchemaManager schemaManager,
                                            boolean attemptSchemaUpdate) {
        super(retry, retryWait, writeSettings, autoCreateTables, errantRecordHandler, schemaManager, attemptSchemaUpdate);
    }

    public abstract Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets();

    public abstract String updateOffsetsOnStream(String tableName, List<Object[]> rows);

    public abstract boolean mayBeCreateStream(String tableName, List<Object[]> rows);

    /**
     * This returns offset information of records
     * @param records List of {SinkRecord, JSONObject} items
     * @return Offsets of the SinkRecords in records list
     */
    protected Map<TopicPartition, OffsetAndMetadata> getOffsetFromRecords(List<Object[]> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        records.forEach(record -> {
            SinkRecord sr = (SinkRecord) record[0];
            offsets.put(new TopicPartition(sr.topic(), sr.kafkaPartition()), new OffsetAndMetadata(sr.kafkaOffset() + 1));
        });

        return offsets;
    }
}
