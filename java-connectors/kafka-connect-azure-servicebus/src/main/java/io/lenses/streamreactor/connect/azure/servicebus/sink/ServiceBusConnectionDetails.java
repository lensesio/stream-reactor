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

import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusType;
import java.util.Map;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Represents connection details for Service Bus connection.
 */
@AllArgsConstructor
@Getter
public class ServiceBusConnectionDetails {

  private final String connectionString;
  private final String serviceBusName;
  private final ServiceBusType serviceBusType;
  private final Consumer<Map<TopicPartition, OffsetAndMetadata>> updateOffsetFunction;
  private final String originalKafkaTopicName;
  private final boolean batchEnabled;
}
