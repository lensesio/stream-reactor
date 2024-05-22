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
package io.lenses.streamreactor.connect.azure.servicebus.source;

import static java.util.Optional.ofNullable;

import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusConfigConstants;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusSourceConfig;
import io.lenses.streamreactor.connect.azure.servicebus.util.KcqlConfigBusMapper;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * Implementation of {@link SourceTask} for Microsoft Azure EventHubs.
 */
@Slf4j
public class AzureServiceBusSourceTask extends SourceTask {

  private final JarManifest jarManifest;
  private TaskToReceiverBridge taskToReceiverBridge;

  public AzureServiceBusSourceTask() {
    this.jarManifest = new JarManifest(getClass().getProtectionDomain().getCodeSource().getLocation());
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    int recordsQueueDefaultSize =
        new AzureServiceBusSourceConfig(props)
            .getInt(AzureServiceBusConfigConstants.TASK_RECORDS_QUEUE_SIZE);
    String connectionString = props.get(AzureServiceBusConfigConstants.CONNECTION_STRING);
    List<Kcql> kcqls =
        KcqlConfigBusMapper.mapKcqlsFromConfig(props.get(AzureServiceBusConfigConstants.KCQL_CONFIG));

    OffsetStorageReader offsetStorageReader =
        ofNullable(this.context).flatMap(
            context -> ofNullable(context.offsetStorageReader())).orElseThrow();

    ArrayBlockingQueue<ServiceBusMessageHolder> recordsQueue =
        new ArrayBlockingQueue<>(recordsQueueDefaultSize);

    TaskToReceiverBridge serviceBusReceiverBridge =
        new TaskToReceiverBridge(connectionString, kcqls, recordsQueue, offsetStorageReader);

    initialize(serviceBusReceiverBridge);
  }

  void initialize(TaskToReceiverBridge serviceBusReceiverFacade) {
    taskToReceiverBridge = serviceBusReceiverFacade;
  }

  @Override
  public List<SourceRecord> poll() {
    List<SourceRecord> poll =
        taskToReceiverBridge.poll();
    return poll.isEmpty() ? null : poll;
  }

  @Override
  public void commitRecord(SourceRecord committedRecord, RecordMetadata metadata) {
    taskToReceiverBridge.commitRecordInServiceBus(committedRecord);
  }

  @Override
  public void stop() {
    log.info("Stopping {}", getClass().getSimpleName());
    taskToReceiverBridge.closeReceivers();
  }
}
