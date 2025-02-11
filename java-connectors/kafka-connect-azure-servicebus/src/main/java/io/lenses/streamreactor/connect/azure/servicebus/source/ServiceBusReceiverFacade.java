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

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder.ServiceBusReceiverClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusKcqlProperties;
import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusType;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusToSourceRecordMapper.mapSingleServiceBusMessage;

/**
 * Class that maps given KCQL query to specific Azure Service Bus Receiver and allows to control it.
 */
@Slf4j
public class ServiceBusReceiverFacade {

  private static final int FIVE_SECONDS_TIMEOUT = 5;
  @Getter
  private final String receiverId;
  private final Disposable subscription;
  private final ServiceBusReceiverAsyncClient serviceBusReceiverAsyncClient;
  private final MessageAck messageAck;

  /**
   * Creates a Facade between {@link ServiceBusReceiverAsyncClient} (that fetches records from Azure Service Bus) and
   * {@link TaskToReceiverBridge} that holds the facades.
   *
   * @param receiverId                    Receiver ID
   * @param serviceBusReceiverAsyncClient {@link ServiceBusReceiverAsyncClient} that fetches records from Azure Service
   *                                      Bus
   * @param onMessage                     Consumer that processes received message
   * @param onError                       Consumer that processes error
   * @param messageAck                    Acknowledgement of the message
   */
  public ServiceBusReceiverFacade(
      @NonNull String receiverId,
      @NonNull ServiceBusReceiverAsyncClient serviceBusReceiverAsyncClient,
      @NonNull Consumer<ServiceBusReceivedMessage> onMessage,
      @NonNull Consumer<Throwable> onError,
      @NonNull MessageAck messageAck) {
    this.receiverId = receiverId;
    this.serviceBusReceiverAsyncClient = serviceBusReceiverAsyncClient;
    this.subscription =
        serviceBusReceiverAsyncClient.receiveMessages()
            .doOnNext(onMessage)
            .doOnError(onError)
            .subscribe();
    this.messageAck = messageAck;
  }

  public static ServiceBusReceiverAsyncClient buildAsyncClient(@NonNull Kcql kcql,
      @NonNull String connectionString,
      int prefetchCount) {
    Map<String, String> kcqlProperties = kcql.getProperties();
    String subscriptionName = kcqlProperties.get(ServiceBusKcqlProperties.SUBSCRIPTION_NAME.getPropertyName());
    ServiceBusType busType =
        ServiceBusType.fromString(kcqlProperties.get(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName()));

    ServiceBusReceiverClientBuilder serviceBusReceiverClientBuilder =
        new ServiceBusClientBuilder()
            .connectionString(connectionString)
            .receiver();

    String inputBus = kcql.getSource();

    if (ServiceBusType.TOPIC.equals(busType)) {
      serviceBusReceiverClientBuilder.topicName(inputBus);
      serviceBusReceiverClientBuilder.subscriptionName(subscriptionName);
    } else if (ServiceBusType.QUEUE.equals(busType)) {
      serviceBusReceiverClientBuilder.queueName(inputBus);
    }

    serviceBusReceiverClientBuilder.receiveMode(ServiceBusReceiveMode.PEEK_LOCK);
    serviceBusReceiverClientBuilder.prefetchCount(prefetchCount);
    serviceBusReceiverClientBuilder.disableAutoComplete();
    return serviceBusReceiverClientBuilder.buildAsyncClient();
  }

  /**
   * Disposes of subscription and closes the Receiver.
   */
  public void unsubscribeAndClose() {
    try {
      subscription.dispose();
    } catch (Exception e) {
      log.error("{} - error while disposing: {}", receiverId, e.getMessage());
    }
    try {
      serviceBusReceiverAsyncClient.close();
    } catch (Exception e) {
      log.error("{} - error while closing: {}", receiverId, e.getMessage());
    }
  }

  public static Consumer<Throwable> onError(String receiverId,
      AtomicReference<Throwable> serviceBusReceiverError) {
    return error -> {
      serviceBusReceiverError.set(error);
      log.warn("{} - Error occurred while receiving message: ", receiverId, error);
    };
  }

  void complete(ServiceBusReceivedMessage serviceBusMessage) {
    log.debug("Completing message with id {}", serviceBusMessage.getMessageId());
    Mono<Void> complete = serviceBusReceiverAsyncClient.complete(serviceBusMessage);
    this.messageAck.acknowledge(complete, serviceBusMessage.getMessageId());
  }

  public static Consumer<ServiceBusReceivedMessage> onSuccessfulMessage(
      String receiverId,
      BlockingQueue<ServiceBusMessageHolder> recordsQueue,
      String inputBus,
      String outputTopic) {
    return message -> {
      SourceRecord sourceRecord = mapSingleServiceBusMessage(message, outputTopic);
      ServiceBusMessageHolder serviceBusMessageHolder =
          new ServiceBusMessageHolder(message, sourceRecord, receiverId);
      boolean offer = false;
      while (!offer) {
        try {
          offer = recordsQueue.offer(serviceBusMessageHolder, FIVE_SECONDS_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

}
