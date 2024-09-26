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
package io.lenses.streamreactor.connect.reporting.model;

import io.lenses.streamreactor.connect.reporting.ReportingMessagesConfig;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface RecordReport {

  /**
   * Converts sinkRecord to Report that can be then sent to report topic.
   *
   * @param messagesConfig parameters for ProducerRecord
   * @return Optional with ProducerRecord that indicated the Report or empty
   *         if we couldn't translate originalRecord
   */
  Optional<ProducerRecord<byte[], String>> produceReportRecord(ReportingMessagesConfig messagesConfig);

}
