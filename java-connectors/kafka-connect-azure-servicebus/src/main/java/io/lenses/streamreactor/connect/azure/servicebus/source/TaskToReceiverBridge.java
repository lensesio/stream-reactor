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
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * Bridge between Receivers and Connector's Task for Azure Service Bus.
 */
@Slf4j
public class TaskToReceiverBridge {

  private static final int INITIAL_RECORDS_TO_COMMIT_SIZE = 500;
  private final BlockingQueue<ServiceBusMessageHolder> recordsQueue;
  private final ServiceBusPartitionOffsetProvider offsetProvider;
  private final ExecutorService sentMessagesExecutors;
  private Map<String, ServiceBusReceiverFacade> receivers;
  private final Map<String, ServiceBusMessageHolder> recordsToCommitMap;

  /**
   * Creates Bridge between Receivers and Connector's Task for Azure Service Bus.
   * 
   * @param properties          all properties for {@link org.apache.kafka.connect.source.SourceTask}.
   * @param recordsQueue        records queue used to store received messages.
   * @param offsetStorageReader offset storage reader from Task.
   */
  public TaskToReceiverBridge(Map<String, String> properties,
      BlockingQueue<ServiceBusMessageHolder> recordsQueue, OffsetStorageReader offsetStorageReader) {
    this.recordsQueue = recordsQueue;
    this.offsetProvider = new ServiceBusPartitionOffsetProvider(offsetStorageReader);
    String connectionString = properties.get(AzureServiceBusConfigConstants.CONNECTION_STRING);
    List<Kcql> kcqls = Kcql.parseMultiple(properties.get(AzureServiceBusConfigConstants.KCQL_CONFIG));
    sentMessagesExecutors = Executors.newFixedThreadPool(kcqls.size() * 2);
    recordsToCommitMap = new ConcurrentHashMap<>(INITIAL_RECORDS_TO_COMMIT_SIZE);

    initiateReceivers(recordsQueue, kcqls, connectionString);
  }

  /**
   * Creates Bridge between Receivers and Connector's Task for Azure Service Bus.
   *
   * @param recordsQueue          records queue used to store received messages.
   * @param offsetStorageReader   offset storage reader from Task.
   * @param receivers             map of {@link ServiceBusReceiverFacade} receivers.
   * @param sentMessagesExecutors {@link ExecutorService} that handles sent messages.
   */
  TaskToReceiverBridge(BlockingQueue<ServiceBusMessageHolder> recordsQueue,
      OffsetStorageReader offsetStorageReader, Map<String, ServiceBusReceiverFacade> receivers,
      ExecutorService sentMessagesExecutors) {
    this.recordsQueue = recordsQueue;
    this.offsetProvider = new ServiceBusPartitionOffsetProvider(offsetStorageReader);
    this.sentMessagesExecutors = sentMessagesExecutors;
    this.receivers = receivers;
    recordsToCommitMap = new ConcurrentHashMap<>(INITIAL_RECORDS_TO_COMMIT_SIZE);
  }

  private void initiateReceivers(BlockingQueue<ServiceBusMessageHolder> recordsQueue,
      List<Kcql> kcqls, String connectionString) {
    receivers = new ConcurrentHashMap<>(kcqls.size());

    kcqls.stream().forEach(kcql -> {
      String receiverId = ServiceBusReceiverFacade.class.getSimpleName() + UUID.randomUUID();
      receivers.put(receiverId, new ServiceBusReceiverFacade(kcql, recordsQueue, connectionString, receiverId));
    });
  }

  /**
   * Unsubscribes from all subscriptions and closes the Receivers.
   */
  public void closeReceivers() {
    receivers.values().forEach(ServiceBusReceiverFacade::unsubscribeAndClose);
  }

  /**
   * Polls for Consumer Records from the queue.
   * 
   * @return List of {@link SourceRecord} or empty list if no new messages received.
   */
  public List<SourceRecord> poll() {
    List<ServiceBusMessageHolder> recordsFromQueue = new ArrayList<>(recordsQueue.size());

    recordsQueue.drainTo(recordsFromQueue);

    List<SourceRecord> recordsToSend =
        recordsFromQueue.stream()
            .map(messageHolder -> {
              recordsToCommitMap.put(messageHolder.getOriginalRecord().getMessageId(), messageHolder);
              return messageHolder.getTranslatedRecord();
            }).collect(Collectors.toList());
    return recordsToSend;
  }

  void commitRecordInServiceBus(SourceRecord sourceRecord) {
    String messageId = (String) sourceRecord.key();
    sentMessagesExecutors.submit(new SentMessagesHandler(recordsToCommitMap.get(messageId)));
    recordsToCommitMap.remove(messageId);
  }

  @AllArgsConstructor
  private class SentMessagesHandler implements Runnable {

    private final ServiceBusMessageHolder sentMessage;

    @Override
    public void run() {
      receivers.get(sentMessage.getReceiverId()).complete(sentMessage.getOriginalRecord());
    }
  }
}
