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

import com.azure.messaging.servicebus.ServiceBusException;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusConfigConstants;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusSinkConfig;
import io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusSinkMapping;
import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusKcqlProperties;
import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusType;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;

/**
 * Class that is a bridge between ServiceBusSenderFacade (and so-called senders to Service Bus) and the Task class.
 */
@Slf4j
public class TaskToSenderBridge {

  private final Map<String, ServiceBusSenderFacade> serviceBusSendersStore;
  private final Consumer<Map<TopicPartition, OffsetAndMetadata>> updateOffsetFunction;
  private final Map<String, ServiceBusSinkMapping> serviceBusSinkMappings;
  private final AzureServiceBusSinkConfig config;
  private final int maxNumberOfRetries;
  private final int retryTimeoutInMillis;

  /**
   * Instantiates TaskToSenderBridge.
   * 
   * @param config                 sink connector configuration
   * @param serviceBusSendersStore map of topic to {@link ServiceBusSenderFacade} that services it
   * @param updateOffsetFunction   function to call when updating committed offsets
   * @param serviceBusSinkMappings mappings between Kafka topics and service buses from KCQL
   */
  TaskToSenderBridge(AzureServiceBusSinkConfig config, Map<String, ServiceBusSenderFacade> serviceBusSendersStore,
      Consumer<Map<TopicPartition, OffsetAndMetadata>> updateOffsetFunction,
      Map<String, ServiceBusSinkMapping> serviceBusSinkMappings) {
    this.serviceBusSendersStore = serviceBusSendersStore;
    this.updateOffsetFunction = updateOffsetFunction;
    this.serviceBusSinkMappings = serviceBusSinkMappings;
    this.config = config;
    int numberOfRetriesFromConf = config.getInt(AzureServiceBusConfigConstants.MAX_NUMBER_OF_RETRIES);
    int retryTimeoutFromConf = config.getInt(AzureServiceBusConfigConstants.TIMEOUT_BETWEEN_RETRIES);

    maxNumberOfRetries =
        numberOfRetriesFromConf > 0 ? numberOfRetriesFromConf
            : AzureServiceBusConfigConstants.MAX_NUMBER_OF_RETRIES_DEFAULT;
    retryTimeoutInMillis =
        retryTimeoutFromConf > 0 ? retryTimeoutFromConf
            : AzureServiceBusConfigConstants.TIMEOUT_BETWEEN_RETRIES_DEFAULT;
  }

  /**
   * Initializes senders for specified {@link TopicPartition}s.
   * 
   * @param partitions topic+partitions collection
   * @return Optional of {@link ConfigException}s (if they happen)
   */
  public Optional<ConfigException> initializeSenders(Collection<TopicPartition> partitions) {
    String connectionString = config.getString(AzureServiceBusConfigConstants.CONNECTION_STRING);

    Set<String> missingMappings =
        findMissingMappings(partitions).stream()
            .map(topic -> String.format("Necessary KCQL Mapping for topic %s is not found. Connector is exiting.",
                topic))
            .collect(Collectors.toUnmodifiableSet());

    if (!missingMappings.isEmpty()) {
      return Optional.of(new ConfigException("Configuration Exception:" +
          String.join(System.lineSeparator(), missingMappings)));
    } else {
      initializeSendersForNewTopics(partitions, connectionString);
      return Optional.empty();
    }
  }

  private void initializeSendersForNewTopics(Collection<TopicPartition> partitions, String connectionString) {
    partitions.stream().filter(tp -> serviceBusSendersStore.get(tp.topic()) == null)
        .forEach(tp -> {
          ServiceBusSinkMapping mappingForTopic = serviceBusSinkMappings.get(tp.topic());

          initializeSender(new ServiceBusConnectionDetails(connectionString, mappingForTopic.getOutputServiceBusName(),
              ServiceBusType.fromString(mappingForTopic.getProperties()
                  .get(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName())), updateOffsetFunction,
              tp.topic(), getBatchEnabled(mappingForTopic)));
          serviceBusSendersStore.get(mappingForTopic.getInputKafkaTopic()).initializePartition(tp);
        });
  }

  private static boolean getBatchEnabled(ServiceBusSinkMapping mappingForTopic) {
    return Boolean.FALSE.toString().equalsIgnoreCase(mappingForTopic.getProperties()
        .get(ServiceBusKcqlProperties.BATCH_ENABLED.getPropertyName()));
  }

  private Set<String> findMissingMappings(Collection<TopicPartition> partitions) {
    Set<String> partitionMappings = partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());
    Set<String> kcqlMappings =
        serviceBusSinkMappings.values().stream()
            .map(ServiceBusSinkMapping::getInputKafkaTopic).collect(Collectors.toSet());
    return partitionMappings.stream().filter(mapping -> !kcqlMappings.contains(mapping))
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Method groups messages by their original Kafka topic then attempts to send it via their respective
   * {@link ServiceBusSenderFacade}s. If sending a message collection doesn't complete successfully it then
   * tries to retry based on properties timeout and maximum number of retries.
   * 
   * @param serviceBusMessages collection of {@link ServiceBusMessageWrapper}s
   * @return list of exceptions that method couldn't handle.
   */
  public List<ServiceBusSendingException> sendMessages(Collection<ServiceBusMessageWrapper> serviceBusMessages) {
    Map<String, List<ServiceBusMessageWrapper>> messagesByKafkaTopic =
        serviceBusMessages.stream()
            .collect(Collectors.groupingBy(ServiceBusMessageWrapper::getOriginalTopic));

    return messagesByKafkaTopic.entrySet().stream()
        .flatMap(entry -> {
          String topicName = entry.getKey();
          List<ServiceBusMessageWrapper> messages = entry.getValue();
          AtomicInteger tries = new AtomicInteger(0);
          Optional<ServiceBusException> serviceBusException;

          do {
            serviceBusException = serviceBusSendersStore.get(topicName).sendMessages(messages);
            if (serviceBusException.isEmpty()) {
              return Stream.empty();
            }
            log.info("Waiting before next retry for {} messages for {} topic", messages.size(), topicName);
            coolDownBeforeRetry(retryTimeoutInMillis);
          } while (tries.incrementAndGet() < maxNumberOfRetries);

          return serviceBusException.map(ex -> new ServiceBusSendingException("Number of retries exhausted. Cause:", ex)
          ).stream();
        })
        .collect(Collectors.toList());
  }

  private static void coolDownBeforeRetry(int millis) {
    try {
      TimeUnit.MILLISECONDS.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public void closeSenderClients() {
    serviceBusSendersStore.forEach((k, sender) -> sender.close());
  }

  private void initializeSender(ServiceBusConnectionDetails serviceBusConnectionDetails) {
    serviceBusSendersStore.put(serviceBusConnectionDetails.getOriginalKafkaTopicName(),
        ServiceBusSenderFacade.fromConnectionDetails(serviceBusConnectionDetails));
  }

}
