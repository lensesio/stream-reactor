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

/**
 * Bridge between Receivers and Connector's Task for Azure Service Bus.
 */
@Slf4j
public class TaskToReceiverBridge {

  private static final int INITIAL_RECORDS_TO_COMMIT_SIZE = 500;
  private static final String FACADE_CLASS_SIMPLE_NAME = ServiceBusReceiverFacade.class.getSimpleName();
  private final BlockingQueue<ServiceBusMessageHolder> recordsQueue;
  private final ExecutorService sentMessagesExecutors;
  private Map<String, ServiceBusReceiverFacade> receivers;
  private final Map<String, ServiceBusMessageHolder> recordsToCommitMap;

  /**
   * Creates Bridge between Receivers and Connector's Task for Azure Service Bus.
   *
   * @param connectionString Service Bus connection string
   * @param kcqls            list of KCQLs to initiate Receivers for.
   * @param recordsQueue     records queue used to store received messages.
   */
  public TaskToReceiverBridge(String connectionString, List<Kcql> kcqls,
      BlockingQueue<ServiceBusMessageHolder> recordsQueue) {
    this.recordsQueue = recordsQueue;
    sentMessagesExecutors = Executors.newFixedThreadPool(kcqls.size() * 2);
    recordsToCommitMap = new ConcurrentHashMap<>(INITIAL_RECORDS_TO_COMMIT_SIZE);

    initiateReceivers(recordsQueue, kcqls, connectionString);
  }

  /**
   * Creates Bridge between Receivers and Connector's Task for Azure Service Bus.
   *
   * @param recordsQueue          records queue used to store received messages.
   * @param receivers             map of {@link ServiceBusReceiverFacade} receivers.
   * @param sentMessagesExecutors {@link ExecutorService} that handles sent messages.
   */
  TaskToReceiverBridge(BlockingQueue<ServiceBusMessageHolder> recordsQueue,
      Map<String, ServiceBusReceiverFacade> receivers,
      ExecutorService sentMessagesExecutors) {
    this.recordsQueue = recordsQueue;
    this.sentMessagesExecutors = sentMessagesExecutors;
    this.receivers = receivers;
    recordsToCommitMap = new ConcurrentHashMap<>(INITIAL_RECORDS_TO_COMMIT_SIZE);
  }

  private void initiateReceivers(BlockingQueue<ServiceBusMessageHolder> recordsQueue,
      List<Kcql> kcqls, String connectionString) {
    receivers = new ConcurrentHashMap<>(kcqls.size());

    kcqls.forEach(kcql -> {
      String receiverId = FACADE_CLASS_SIMPLE_NAME + UUID.randomUUID();
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

    return recordsFromQueue.stream()
        .map(messageHolder -> {
          recordsToCommitMap.put(messageHolder.getOriginalRecord().getMessageId(), messageHolder);
          return messageHolder.getTranslatedRecord();
        }).collect(Collectors.toList());
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
