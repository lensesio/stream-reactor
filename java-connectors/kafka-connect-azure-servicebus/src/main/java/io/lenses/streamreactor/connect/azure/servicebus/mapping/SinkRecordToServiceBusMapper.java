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
package io.lenses.streamreactor.connect.azure.servicebus.mapping;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusMessage;
import io.lenses.streamreactor.connect.azure.servicebus.sink.ServiceBusMessageWrapper;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Class that maps {@link SinkRecord}s to Service Bus format.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SinkRecordToServiceBusMapper {

  private static final String BINARY_TYPE = "binary";

  /**
   * Maps {@link SinkRecord} to Service Bus format.
   * 
   * @param sinkRecord sink record to map
   * @return message in {@link ServiceBusMessageWrapper} format
   */
  public static ServiceBusMessageWrapper mapToServiceBus(SinkRecord sinkRecord) {
    Optional<ServiceBusMessage> payload = mapPayload(sinkRecord);
    return new ServiceBusMessageWrapper(payload.orElse(null), sinkRecord.originalTopic(), sinkRecord
        .originalKafkaPartition(),
        sinkRecord.originalKafkaOffset());
  }

  private static Optional<ServiceBusMessage> mapPayload(SinkRecord sinkRecord) {
    return Optional.ofNullable(sinkRecord.value())
        .map(val -> {
          ServiceBusMessage serviceBusMessage =
              new ServiceBusMessage(BinaryData.fromObject(val))
                  .setContentType(BINARY_TYPE);

          if (Schema.STRING_SCHEMA.equals(sinkRecord.keySchema())) {
            serviceBusMessage.setMessageId((String) sinkRecord.key());
          }

          return serviceBusMessage;
        });
  }

}
