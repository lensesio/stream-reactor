package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Storage Write API writer that attempts to write all the rows it is given at once
 */
public class StorageWriteApiWriter implements Runnable {

    Logger logger = LoggerFactory.getLogger(StorageWriteApiWriter.class);
    private final StorageWriteApiBase streamWriter;
    private final TableName tableName;
    private final List<Object[]> records;
    private final String streamName;

    /**
     *
     * @param tableName The table to write the records to
     * @param streamWriter The stream writer to use - Default, Batch etc
     * @param records The records to write
     * @param streamName The stream to use while writing data
     */
    public StorageWriteApiWriter(TableName tableName, StorageWriteApiBase streamWriter, List<Object[]> records, String streamName) {

        this.streamWriter = streamWriter;
        this.records = records;
        this.tableName = tableName;
        this.streamName = streamName;
    }

    @Override
    public void run() {
        if(records.size() == 0) {
            logger.debug("There are no records, skipping...");
            return;
        }
        logger.debug("Putting {} records into {} stream", records.size(), streamName);
        streamWriter.initializeAndWriteRecords(tableName, records, streamName);
    }

    public static class Builder implements TableWriterBuilder {
        private final
        List<Object[]> records = new ArrayList<>();
        private final RecordConverter<Map<String, Object>> recordConverter;
        private final BigQuerySinkTaskConfig config;
        private final TableName tableName;
        private final StorageWriteApiBase streamWriter;
        public Builder(StorageWriteApiBase streamWriter,
                                      TableName tableName,
                                      RecordConverter<Map<String, Object>> storageApiRecordConverter,
                                      BigQuerySinkTaskConfig config) {
            this.streamWriter = streamWriter;
            this.tableName = tableName;
            this.recordConverter = storageApiRecordConverter;
            this.config = config;
        }

        /**
         * Captures actual record and corresponding JSONObject converted record
         * @param sinkRecord The actual records
         */
        @Override
        public void addRow(SinkRecord sinkRecord, TableId tableId) {
            records.add(new Object[]{sinkRecord, convertRecord(sinkRecord)});
        }

        /**
         * Converts SinkRecord to JSONObject to be sent to BQ Streams
         * @param record which is to be converted
         * @return converted record as JSONObject
         */
        private JSONObject convertRecord(SinkRecord record) {
            Map<String, Object> convertedRecord = recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);

            config.getKafkaDataFieldName().ifPresent(
                    fieldName -> convertedRecord.put(fieldName, KafkaDataBuilder.buildKafkaDataRecord(record))
            );

            config.getKafkaKeyFieldName().ifPresent(fieldName -> {
                Map<String, Object> keyData = recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY);
                convertedRecord.put(fieldName, keyData);
            });

            Map<String, Object> result = config.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)
                    ? FieldNameSanitizer.replaceInvalidKeys(convertedRecord)
                    : convertedRecord;

            return new JSONObject(result);
        }

        /**
         * @return Builds Storage write API writer which would do actual data ingestion using streams
         */
        @Override
        public Runnable build() {
            return new StorageWriteApiWriter(tableName, streamWriter, records, null);
        }
    }
}
