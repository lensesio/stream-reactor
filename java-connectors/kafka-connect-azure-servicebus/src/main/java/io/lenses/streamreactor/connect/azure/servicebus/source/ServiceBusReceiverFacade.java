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

import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusToSourceRecordMapper.mapSingleServiceBusMessage;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder.ServiceBusReceiverClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;

import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.connect.azure.servicebus.source.ServiceBusPartitionOffsetProvider.AzureServiceBusOffsetMarker;
import io.lenses.streamreactor.connect.azure.servicebus.source.ServiceBusPartitionOffsetProvider.AzureServiceBusPartitionKey;
import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusKcqlProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;

/**
 * Class that maps given KCQL query to specific Azure Service Bus Receiver and allows to control it.
 */
@Slf4j
public class ServiceBusReceiverFacade {

  private static final String TOPIC_TYPE = "TOPIC";
  private static final String QUEUE_TYPE = "QUEUE";
  private static final int FIVE_SECONDS_TIMEOUT = 5;
  private final Kcql kcql;
  private final BlockingQueue<ServiceBusMessageHolder> recordsQueue;
  private final String connectionString;
  @Getter
  private final String receiverId;
  private Disposable subscription;
  private ServiceBusReceiverAsyncClient receiver;

  /**
   * Creates a Facade between {@link ServiceBusReceiverAsyncClient} (that fetches records from Azure Service Bus) and
   * {@link TaskToReceiverBridge} that holds the facades.
   *
   * @param kcql             KCQL query for particular mapping
   * @param recordsQueue     {@link java.util.concurrent.BlockingQueue} implementation that holds records
   *                         from Service Bus
   * @param connectionString Connection String
   * @param receiverId       Receiver ID
   */
  public ServiceBusReceiverFacade(Kcql kcql, BlockingQueue<ServiceBusMessageHolder> recordsQueue,
      String connectionString, String receiverId) {
    this.kcql = kcql;
    this.recordsQueue = recordsQueue;
    this.connectionString = connectionString;
    this.receiverId = receiverId;

    instantiateReceiverFromKcql();
  }

  ServiceBusReceiverFacade(Kcql kcql, BlockingQueue<ServiceBusMessageHolder> recordsQueue,
      String connectionString, String receiverId, ServiceBusReceiverAsyncClient receiver) {
    this.kcql = kcql;
    this.recordsQueue = recordsQueue;
    this.connectionString = connectionString;
    this.receiverId = receiverId;
    this.receiver = receiver;

  }

  private void instantiateReceiverFromKcql() {
    Map<String, String> kcqlProperties = kcql.getProperties();
    String subscriptionName = kcqlProperties.get(ServiceBusKcqlProperties.SUBSCRIPTION_NAME.getPropertyName());
    String busType = kcqlProperties.get(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName());

    ServiceBusReceiverClientBuilder serviceBusReceiverClientBuilder =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver();

    String outputTopic = kcql.getTarget();
    String inputBus = kcql.getSource();

    if (TOPIC_TYPE.equalsIgnoreCase(busType)) {
      serviceBusReceiverClientBuilder.topicName(inputBus);
      serviceBusReceiverClientBuilder.subscriptionName(subscriptionName);
    } else if (QUEUE_TYPE.equalsIgnoreCase(busType)) {
      serviceBusReceiverClientBuilder.queueName(inputBus);
    }

    receiver = serviceBusReceiverClientBuilder.buildAsyncClient();

    subscription =
        receiver.receiveMessages()
            .doOnNext(onSuccessfulMessage(receiverId, recordsQueue, inputBus, outputTopic))
            .doOnError(onError(receiverId))
            .subscribe();
  }

  /**
   * Disposes of subscription and closes the Receiver.
   */
  public void unsubscribeAndClose() {
    try {
      subscription.dispose();
      receiver.close();
    } catch (Exception e) {
      log.error("{} - error while closing: {}", receiverId, e.getMessage());
    }
  }

  private static Consumer<Throwable> onError(String receiverId) {
    return error -> log.warn("{} - Error occurred while receiving message: ", receiverId, error);
  }

  void complete(ServiceBusReceivedMessage serviceBusMessage) {
    receiver.complete(serviceBusMessage);
  }

  private static Consumer<ServiceBusReceivedMessage> onSuccessfulMessage(
      String receiverId, BlockingQueue<ServiceBusMessageHolder> recordsQueue,
      String inputBus, String outputTopic) {
    return message -> {
      long sequenceNumber = message.getSequenceNumber();
      AzureServiceBusOffsetMarker offsetMarker =
          new AzureServiceBusOffsetMarker(sequenceNumber);
      AzureServiceBusPartitionKey partitionKey =
          new AzureServiceBusPartitionKey(inputBus, message.getPartitionKey());

      try {
        SourceRecord sourceRecord = mapSingleServiceBusMessage(message, outputTopic, partitionKey, offsetMarker);
        ServiceBusMessageHolder serviceBusMessageHolder =
            new ServiceBusMessageHolder(message, sourceRecord, receiverId);
        boolean offer = false;
        while (!offer) {
          offer = recordsQueue.offer(serviceBusMessageHolder, FIVE_SECONDS_TIMEOUT, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        log.warn("{} has been interrupted on offering", receiverId);
      }
    };
  }

}
