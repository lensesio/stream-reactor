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
package io.lenses.streamreactor.connect.reporting.model.generic;

import cyclops.data.tuple.Tuple2;
import io.lenses.streamreactor.connect.reporting.ReportingMessagesConfig;
import io.lenses.streamreactor.connect.reporting.model.RecordReport;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

@AllArgsConstructor
@Getter
@Data
public class ReportingRecord implements RecordReport {

  private TopicPartition topicPartition;
  private Long offset;
  private Long timestamp;
  private String endpoint;
  private String payload;
  private List<Tuple2<String, String>> headers;

  @Override
  public Optional<ProducerRecord<byte[], String>> produceReportRecord(ReportingMessagesConfig messagesConfig) {
    return ProducerRecordConverter.convert(this, messagesConfig);
  }
}
