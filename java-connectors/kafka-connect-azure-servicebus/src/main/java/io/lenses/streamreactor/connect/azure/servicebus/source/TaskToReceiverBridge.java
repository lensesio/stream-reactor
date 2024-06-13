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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Bridge between Receivers and Connector's Task for Azure Service Bus.
 */
@Slf4j
public class TaskToReceiverBridge {

  private static final int INITIAL_RECORDS_TO_COMMIT_SIZE = 500;
  private final BlockingQueue<ServiceBusMessageHolder> recordsQueue;
  private final ExecutorService sentMessagesExecutors;
  private final Map<String, ServiceBusReceiverFacade> receivers;
  private final Map<String, ServiceBusMessageHolder> recordsToCommitMap;

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
