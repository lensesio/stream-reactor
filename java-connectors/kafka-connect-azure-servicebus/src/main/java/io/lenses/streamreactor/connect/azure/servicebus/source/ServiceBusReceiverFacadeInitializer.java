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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import io.lenses.kcql.Kcql;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import reactor.util.retry.RetrySpec;

/**
 * Class that initializes {@link ServiceBusReceiverFacade}s.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ServiceBusReceiverFacadeInitializer {

  private static final String FACADE_CLASS_SIMPLE_NAME = ServiceBusReceiverFacade.class.getSimpleName();

  /**
   * Initializes map of {@link ServiceBusReceiverFacade}s from given input.
   *
   * @param recordsQueue     Queue that receiver can write records to.
   * @param kcqls            {@link Kcql}s with mappings.
   * @param connectionString ServiceBus connection string.
   * @return map of receiverIDs to {@link ServiceBusReceiverFacade} object.
   */
  static Map<String, ServiceBusReceiverFacade> initializeReceiverFacades(
      BlockingQueue<ServiceBusMessageHolder> recordsQueue,
      List<Kcql> kcqls,
      String connectionString,
      int prefetchCount,
      int maxCompleteRetries,
      Duration minBackoffCompleteRetries,
      AtomicReference<Throwable> serviceBusReceiverError
  ) {
    return kcqls.stream()
        .map(kcql -> {
          final ServiceBusReceiverAsyncClient serviceBusReceiverAsyncClient =
              ServiceBusReceiverFacade.buildAsyncClient(kcql, connectionString, prefetchCount);
          final String asyncClientId = FACADE_CLASS_SIMPLE_NAME + UUID.randomUUID();
          final MessageAck messageAck =
              new RetryMessageAck(RetrySpec.backoff(maxCompleteRetries, minBackoffCompleteRetries),
                  RetryMessageAck.logSuccess(),
                  RetryMessageAck.logError());
          return new ServiceBusReceiverFacade(
              asyncClientId,
              serviceBusReceiverAsyncClient,
              ServiceBusReceiverFacade.onSuccessfulMessage(asyncClientId, recordsQueue, kcql.getSource(), kcql
                  .getTarget()),
              ServiceBusReceiverFacade.onError(asyncClientId, serviceBusReceiverError),
              messageAck);
        })
        .collect(Collectors.toMap(ServiceBusReceiverFacade::getReceiverId, e -> e));
  }
}
