/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.reporting.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.apache.kafka.common.TopicPartition;

/**
 * Represents a reporting record that contains data specific to a connector.
 *
 * @param <C> the type of connector-specific record data
 */
@AllArgsConstructor
@Getter
@Data
public class ReportingRecord<C extends ConnectorSpecificRecordData> {

  private TopicPartition topicPartition;
  private Long offset;
  private Long timestamp;
  private String endpoint;
  private String payload;

  private C connectorSpecific;

}
