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

import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusConfigConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * Bridge between Receivers and Connector's Task for Azure Service Bus.
 */
@Slf4j
public class TaskToReceiverBridge {

  private final BlockingQueue<SourceRecord> recordsQueue;
  private final ServiceBusPartitionOffsetProvider offsetProvider;
  private List<ServiceBusReceiverFacade> receivers;

  /**
   * Creates Bridge between Receivers and Connector's Task for Azure Service Bus.
   * 
   * @param properties          all properties for {@link org.apache.kafka.connect.source.SourceTask}.
   * @param recordsQueue        records queue used to store received messages.
   * @param offsetStorageReader offset storage reader from Task.
   */
  public TaskToReceiverBridge(Map<String, String> properties,
      BlockingQueue<SourceRecord> recordsQueue, OffsetStorageReader offsetStorageReader) {
    this.recordsQueue = recordsQueue;
    this.offsetProvider = new ServiceBusPartitionOffsetProvider(offsetStorageReader);

    initiateReceivers(properties, recordsQueue);
  }

  /**
   * Creates Bridge between Receivers and Connector's Task for Azure Service Bus.
   *
   * @param recordsQueue        records queue used to store received messages.
   * @param offsetStorageReader offset storage reader from Task.
   * @param receivers           list of {@link ServiceBusReceiverFacade} receivers.
   */
  TaskToReceiverBridge(BlockingQueue<SourceRecord> recordsQueue,
      OffsetStorageReader offsetStorageReader, List<ServiceBusReceiverFacade> receivers) {
    this.recordsQueue = recordsQueue;
    this.offsetProvider = new ServiceBusPartitionOffsetProvider(offsetStorageReader);
    this.receivers = receivers;
  }

  private void initiateReceivers(Map<String, String> properties,
      BlockingQueue<SourceRecord> recordsQueue) {
    String connectionString = properties.get(AzureServiceBusConfigConstants.CONNECTION_STRING);
    List<Kcql> kcqls = Kcql.parseMultiple(properties.get(AzureServiceBusConfigConstants.KCQL_CONFIG));
    receivers = new ArrayList<>(kcqls.size());

    kcqls.stream().forEach(kcql -> receivers.add(new ServiceBusReceiverFacade(kcql, recordsQueue, connectionString)));

  }

  /**
   * Unsubscribes from all subscriptions and closes the Receivers.
   */
  public void closeReceivers() {
    receivers.forEach(ServiceBusReceiverFacade::unsubscribeAndClose);
  }

  /**
   * Polls for Consumer Records from the queue.
   * 
   * @return List of {@link SourceRecord} or empty list if no new messages received.
   */
  public List<SourceRecord> poll() {
    List<SourceRecord> sourceRecords = new ArrayList<>(recordsQueue.size());

    recordsQueue.drainTo(sourceRecords);

    return sourceRecords;
  }
}
