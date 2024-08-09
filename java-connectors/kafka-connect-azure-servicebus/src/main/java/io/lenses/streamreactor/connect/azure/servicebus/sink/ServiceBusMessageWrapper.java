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
package io.lenses.streamreactor.connect.azure.servicebus.sink;

import com.azure.messaging.servicebus.ServiceBusMessage;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Class that contains transformed {@link ServiceBusMessage} as well as its original metadata (e.g. topic and partition)
 */
@AllArgsConstructor
@Getter
public class ServiceBusMessageWrapper {

  private final ServiceBusMessage serviceBusMessage;
  private final String originalTopic;
  private final Integer originalKafkaPartition;
  private final long originalKafkaOffset;

  public Optional<ServiceBusMessage> getServiceBusMessage() {
    return Optional.ofNullable(serviceBusMessage);
  }
}
