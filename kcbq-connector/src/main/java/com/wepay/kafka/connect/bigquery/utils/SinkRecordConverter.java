/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.utils;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.MergeQueries;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A class for converting a {@link SinkRecord SinkRecord} to {@link InsertAllRequest.RowToInsert BigQuery row}
 */
public class SinkRecordConverter {
    private static final Logger logger = LoggerFactory.getLogger(SinkRecordConverter.class);

    private final BigQuerySinkTaskConfig config;
    private final MergeBatches mergeBatches;
    private final MergeQueries mergeQueries;

    private final RecordConverter<Map<String, Object>> recordConverter;
    private final long mergeRecordsThreshold;
    private final boolean useMessageTimeDatePartitioning;
    private final boolean usePartitionDecorator;


    public SinkRecordConverter(BigQuerySinkTaskConfig config,
                               MergeBatches mergeBatches, MergeQueries mergeQueries) {
        this.config = config;
        this.mergeBatches = mergeBatches;
        this.mergeQueries = mergeQueries;

        this.recordConverter = config.getRecordConverter();
        this.mergeRecordsThreshold = config.getLong(config.MERGE_RECORDS_THRESHOLD_CONFIG);
        this.useMessageTimeDatePartitioning =
            config.getBoolean(config.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG);
        this.usePartitionDecorator =
            config.getBoolean(config.BIGQUERY_PARTITION_DECORATOR_CONFIG);

    }

    public InsertAllRequest.RowToInsert getRecordRow(SinkRecord record, TableId table) {
        Map<String, Object> convertedRecord = config.isUpsertDeleteEnabled()
            ? getUpsertDeleteRow(record, table)
            : getRegularRow(record);

        Map<String, Object> result = config.getBoolean(config.SANITIZE_FIELD_NAME_CONFIG)
            ? FieldNameSanitizer.replaceInvalidKeys(convertedRecord)
            : convertedRecord;

        return InsertAllRequest.RowToInsert.of(getRowId(record), result);
    }

    private Map<String, Object> getUpsertDeleteRow(SinkRecord record, TableId table) {
        // Unconditionally allow tombstone records if delete is enabled.
        Map<String, Object> convertedValue = config.getBoolean(config.DELETE_ENABLED_CONFIG) && record.value() == null
            ? null
            : recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);

        if (convertedValue != null) {
            config.getKafkaDataFieldName().ifPresent(
                fieldName -> convertedValue.put(fieldName, KafkaDataBuilder.buildKafkaDataRecord(record))
            );
        }

        Map<String, Object> result = new HashMap<>();
        long totalBatchSize = mergeBatches.addToBatch(record, table, result);
        if (mergeRecordsThreshold != -1 && totalBatchSize >= mergeRecordsThreshold) {
            logger.debug("Triggering merge flush for table {} since the size of its current batch has "
                    + "exceeded the configured threshold of {}}",
                table, mergeRecordsThreshold);
            mergeQueries.mergeFlush(table);
        }

        Map<String, Object> convertedKey = recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY);
        if (convertedKey == null) {
            throw new ConnectException("Record keys must be non-null when upsert/delete is enabled");
        }

        result.put(MergeQueries.INTERMEDIATE_TABLE_KEY_FIELD_NAME, convertedKey);
        result.put(MergeQueries.INTERMEDIATE_TABLE_VALUE_FIELD_NAME, convertedValue);
        result.put(MergeQueries.INTERMEDIATE_TABLE_ITERATION_FIELD_NAME, totalBatchSize);
        if (usePartitionDecorator && useMessageTimeDatePartitioning) {
            if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
                throw new ConnectException(
                    "Message has no timestamp type, cannot use message timestamp to partition.");
            }
            result.put(MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME, record.timestamp());
        } else {
            // Provide a value for this column even if it's not used for partitioning in the destination
            // table, so that it can be used to deduplicate rows during merge flushes
            result.put(MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME, System.currentTimeMillis() / 1000);
        }

        return result;
    }

    private Map<String, Object> getRegularRow(SinkRecord record) {
        Map<String, Object> result = recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);

        config.getKafkaDataFieldName().ifPresent(
            fieldName -> result.put(fieldName, KafkaDataBuilder.buildKafkaDataRecord(record))
        );

        config.getKafkaKeyFieldName().ifPresent(fieldName -> {
            Map<String, Object> keyData = recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY);
            result.put(fieldName, keyData);
        });

        return result;
    }

    private String getRowId(SinkRecord record) {
        return String.format("%s-%d-%d",
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset());
    }
}
