/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.gcp.pubsub.source.mapping;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import io.lenses.streamreactor.common.config.base.intf.Converter;
import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubMessageData;
import lombok.AllArgsConstructor;

/**
 * SourceRecordConverter is responsible for converting PubSubMessageData to SourceRecord.
 * It uses the MappingConfig to generate the key and value for the SourceRecord.
 */
@AllArgsConstructor
public class SourceRecordConverter extends Converter<PubSubMessageData, SourceRecord> {

  private final MappingConfig mappingConfig;

  @Override
  protected SourceRecord convert(final PubSubMessageData source) throws ConfigException {
    return new SourceRecord(
        source.getSourcePartition().toMap(),
        source.getSourceOffset().toMap(),
        source.getTargetTopicName(),
        getKeySchema(),
        getKey(source),
        getValueSchema(),
        getValue(source)
    );
  }

  private Object getValue(final PubSubMessageData source) {
    return mappingConfig.getValueMapper().mapValue(source);
  }

  private Schema getValueSchema() {
    return mappingConfig.getValueMapper().getSchema();
  }

  private Object getKey(final PubSubMessageData source) {
    return mappingConfig.getKeyMapper().mapKey(source);
  }

  private Schema getKeySchema() {
    return mappingConfig.getKeyMapper().getSchema();
  }

}
