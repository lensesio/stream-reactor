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

import static io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusType.QUEUE;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder.ServiceBusSenderClientBuilder;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusMessageBatch;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import cyclops.control.Try;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * A facade between {@link ServiceBusSenderClient}s and rest of the connector.
 */
@AllArgsConstructor
public class ServiceBusSenderFacade {

  private static final Long NO_OFFSET = Long.MIN_VALUE;
  private final Map<Integer, AtomicLong> partitionCommittedOffsets = new ConcurrentHashMap<>();
  private final Consumer<Map<TopicPartition, OffsetAndMetadata>> commitOffsetFunction;
  private final String originalKafkaTopicName;
  private final ServiceBusSenderClient sender;
  private final Boolean batchEnabled;

  /**
   * Constructs Facade from {@link ServiceBusConnectionDetails} object.
   *
   * @param serviceBusConnectionDetails connection details.
   */
  public static ServiceBusSenderFacade fromConnectionDetails(ServiceBusConnectionDetails serviceBusConnectionDetails) {
    return new ServiceBusSenderFacade(serviceBusConnectionDetails.getUpdateOffsetFunction(),
        serviceBusConnectionDetails.getOriginalKafkaTopicName(),
        initializeServiceBusClient(serviceBusConnectionDetails), serviceBusConnectionDetails.isBatchEnabled());
  }

  /**
   * Initializes offset for specific {@link TopicPartition}.
   * 
   * @param tp topic and partition object
   */
  public void initializePartition(TopicPartition tp) {
    partitionCommittedOffsets.computeIfAbsent(tp.partition(), tpart -> new AtomicLong(NO_OFFSET));
  }

  /**
   * Closes sender.
   */
  public void close() {
    sender.close();
  }

  /**
   * Method which fetches {@link ServiceBusMessageBatch} from sender and packs messages to it.
   * If it successfully manages to send them it then commits offset for topic and partition.
   *
   * @param serviceBusMessages messages to send.
   * @return Optional with {@link ServiceBusException} if sending fails.
   */
  public Optional<ServiceBusException> sendMessages(Collection<ServiceBusMessageWrapper> serviceBusMessages) {
    Map<Integer, Long> highestOffsetsPerPartitions = calculateHighestOffsetsPerPartitions(serviceBusMessages);
    Optional<ServiceBusException> sendExceptions;

    if (batchEnabled) {
      ServiceBusMessageBatch senderMessageBatch = createMessageBatch(serviceBusMessages);
      sendExceptions = submitBatch(senderMessageBatch);
    } else {
      sendExceptions = sendMessagesSeparately(serviceBusMessages);
    }

    commitPartitionOffsets(highestOffsetsPerPartitions);

    return sendExceptions;
  }

  private Optional<ServiceBusException> sendMessagesSeparately(
      Collection<ServiceBusMessageWrapper> serviceBusMessages) {
    List<ServiceBusMessage> messages =
        serviceBusMessages.stream()
            .map(ServiceBusMessageWrapper::getServiceBusMessage)
            .flatMap(Optional::stream)
            .collect(Collectors.toUnmodifiableList());

    return messages.isEmpty() ? Optional.empty() : Try.runWithCatch(() -> sender.sendMessages(messages),
        ServiceBusException.class)
        .failureGet().toOptional();
  }

  private Optional<ServiceBusException> submitBatch(ServiceBusMessageBatch senderMessageBatch) {
    if (senderMessageBatch.getCount() > 0) {
      return Try.runWithCatch(() -> sender.sendMessages(senderMessageBatch), ServiceBusException.class)
          .failureGet().toOptional();
    } else {
      return Optional.empty();
    }
  }

  private static Map<Integer, Long> calculateHighestOffsetsPerPartitions(
      Collection<ServiceBusMessageWrapper> serviceBusMessages) {
    return serviceBusMessages.stream()
        .map(msg -> new SimpleEntry<>(msg.getOriginalKafkaPartition(), msg.getOriginalKafkaOffset()))
        .collect(Collectors.groupingBy(
            Entry::getKey,
            Collectors.collectingAndThen(
                Collectors.maxBy(Comparator.comparingLong(Entry::getValue)),
                optionalEntry -> optionalEntry.map(Entry::getValue).orElse(null)
            ))
        );
  }

  private ServiceBusMessageBatch createMessageBatch(Collection<ServiceBusMessageWrapper> serviceBusMessages) {
    ServiceBusMessageBatch senderMessageBatch = sender.createMessageBatch();
    serviceBusMessages.stream().map(ServiceBusMessageWrapper::getServiceBusMessage)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(senderMessageBatch::tryAddMessage);

    return senderMessageBatch;
  }

  private static ServiceBusSenderClient initializeServiceBusClient(
      ServiceBusConnectionDetails serviceBusConnectionDetails) {
    ServiceBusSenderClientBuilder builder =
        new ServiceBusClientBuilder()
            .connectionString(serviceBusConnectionDetails.getConnectionString())
            .sender();

    setQueueOrTopicNameFunction.accept(builder, serviceBusConnectionDetails);

    return builder.buildClient();
  }

  private void commitPartitionOffsets(Map<Integer, Long> highestOffsetsInBatchPerPartition) {
    Map<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<>(highestOffsetsInBatchPerPartition.size());
    highestOffsetsInBatchPerPartition.forEach((key, value) -> offsetsMap.put(new TopicPartition(originalKafkaTopicName,
        key), new OffsetAndMetadata(value)));

    commitOffsetFunction.accept(offsetsMap);
  }

  private static final BiConsumer<ServiceBusSenderClientBuilder, ServiceBusConnectionDetails> setQueueOrTopicNameFunction =
      (builder, connectionDetails) -> {
        Consumer<String> fnSetQueueOrTopic =
            QUEUE.equals(connectionDetails.getServiceBusType()) ? builder::queueName : builder::topicName;
        fnSetQueueOrTopic.accept(connectionDetails.getServiceBusName());
      };
}
