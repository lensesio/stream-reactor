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

import static io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow;

import cyclops.control.Either;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.collections.TopicPartitionOffsetAndMetadataStorage;
import io.lenses.streamreactor.common.exception.ConnectorStartupException;
import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusConfigConstants;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusSinkConfig;
import io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusSinkMapping;
import io.lenses.streamreactor.connect.azure.servicebus.mapping.SinkRecordToServiceBusMapper;
import io.lenses.streamreactor.connect.azure.servicebus.util.KcqlConfigBusMapper;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 * Task class for Azure Service Bus Sink Connector.
 */
@Slf4j
public class AzureServiceBusSinkTask extends SinkTask {

  private final JarManifest jarManifest;
  private TaskToSenderBridge taskToReceiverBridge;
  private final Consumer<Map<TopicPartition, OffsetAndMetadata>> flushFunction =
      this::updateOffsets;

  private final TopicPartitionOffsetAndMetadataStorage offsetStorage;

  public AzureServiceBusSinkTask() {
    this.jarManifest =
        unpackOrThrow(JarManifest
            .produceFromClass(getClass())
        );
    this.offsetStorage = new TopicPartitionOffsetAndMetadataStorage();
  }

  @Override
  public void start(Map<String, String> props) {
    Map<String, ServiceBusSinkMapping> serviceBusSinkMappings =
        unpackOrThrow(transformToMappings(props.get(AzureServiceBusConfigConstants.KCQL_CONFIG)));

    initialize(new TaskToSenderBridge(new AzureServiceBusSinkConfig(props),
        new ConcurrentHashMap<>(), flushFunction, serviceBusSinkMappings));
  }

  void initialize(TaskToSenderBridge taskToReceiverBridge) {
    this.taskToReceiverBridge = taskToReceiverBridge;
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    List<ServiceBusMessageWrapper> recordCompositeList =
        records.stream().map(SinkRecordToServiceBusMapper::mapToServiceBus).collect(Collectors.toUnmodifiableList());
    taskToReceiverBridge.sendMessages(recordCompositeList).forEach(
        exception -> {
          throw exception;
        }
    );
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    taskToReceiverBridge.initializeSenders(partitions).ifPresent(ex -> {
      throw ex;
    });
  }

  @Override
  public void stop() {
    log.info("Stopping {}", getClass().getSimpleName());
    taskToReceiverBridge.closeSenderClients();
    log.info("Stopped {}", getClass().getSimpleName());
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return offsetStorage.checkAgainstProcessedOffsets(currentOffsets);
  }

  void updateOffsets(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    offsetStorage.updateOffsets(currentOffsets);
  }

  private Either<ConnectorStartupException, Map<String, ServiceBusSinkMapping>> transformToMappings(String kcqlString) {
    return KcqlConfigBusMapper.mapKcqlsFromConfig(kcqlString, false)
        .map(mappings -> mappings.stream().collect(Collectors.toUnmodifiableMap(Kcql::getSource,
            mapping -> new ServiceBusSinkMapping(mapping.getSource(), mapping.getTarget(),
                mapping.getProperties()))));
  }
}
