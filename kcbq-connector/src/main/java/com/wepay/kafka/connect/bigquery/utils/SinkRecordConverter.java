package com.wepay.kafka.connect.bigquery.utils;

/*
 * Copyright 2016 WePay, Inc.
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

import com.google.cloud.bigquery.InsertAllRequest;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;
import java.util.Optional;

/**
 * A class for converting a {@link SinkRecord SinkRecord} to {@link InsertAllRequest.RowToInsert BigQuery row}
 */
public class SinkRecordConverter {
    private final RecordConverter<Map<String, Object>> recordConverter;
    private final boolean sanitizeFieldName;
    private final Optional<String> kafkaKeyFieldName;
    private final Optional<String> kafkaDataFieldName;

    public SinkRecordConverter(RecordConverter<Map<String, Object>> recordConverter, boolean sanitizeFieldName, Optional<String> kafkaKeyFieldName, Optional<String> kafkaDataFieldName) {
        this.recordConverter = recordConverter;
        this.sanitizeFieldName = sanitizeFieldName;
        this.kafkaKeyFieldName = kafkaKeyFieldName;
        this.kafkaDataFieldName = kafkaDataFieldName;
    }

    public InsertAllRequest.RowToInsert getRecordRow(SinkRecord record) {
        Map<String, Object> convertedRecord = recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);
        if (kafkaKeyFieldName.isPresent()) {
            convertedRecord.put(kafkaKeyFieldName.get(), recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY));
        }
        if (kafkaDataFieldName.isPresent()) {
            convertedRecord.put(kafkaDataFieldName.get(), KafkaDataBuilder.buildKafkaDataRecord(record));
        }
        if (sanitizeFieldName) {
            convertedRecord = FieldNameSanitizer.replaceInvalidKeys(convertedRecord);
        }
        return InsertAllRequest.RowToInsert.of(getRowId(record), convertedRecord);
    }

    private String getRowId(SinkRecord record) {
        return String.format("%s-%d-%d",
                record.topic(),
                record.kafkaPartition(),
                record.kafkaOffset());
    }
}
