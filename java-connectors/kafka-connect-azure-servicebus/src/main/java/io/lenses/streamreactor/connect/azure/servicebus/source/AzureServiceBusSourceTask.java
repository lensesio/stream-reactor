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

import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusSourceConfig;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * Implementation of {@link SourceTask} for Microsoft Azure EventHubs.
 */
@Slf4j
public class AzureServiceBusSourceTask extends SourceTask {

  private static final int RECORDS_QUEUE_DEFAULT_SIZE = 20;

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
    OffsetStorageReader offsetStorageReader =
        ofNullable(this.context).flatMap(
            context -> ofNullable(context.offsetStorageReader())).orElseThrow();
    AzureServiceBusSourceConfig azureServiceBusSourceConfig = new AzureServiceBusSourceConfig(props);

    ArrayBlockingQueue<SourceRecord> recordsQueue =
        new ArrayBlockingQueue<>(RECORDS_QUEUE_DEFAULT_SIZE);

    TaskToReceiverBridge serviceBusReceiverBridge =
        new TaskToReceiverBridge(props, recordsQueue, offsetStorageReader);

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
  public void stop() {
    log.info("Stopping {}", getClass().getSimpleName());
    taskToReceiverBridge.closeReceivers();
  }
}
