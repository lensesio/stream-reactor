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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

public class ServiceBusReceiverMapper {

  private ServiceBusReceiverMapper() {
  }

  private static final String FACADE_CLASS_SIMPLE_NAME = ServiceBusReceiverFacade.class.getSimpleName();

  static Map<String, ServiceBusReceiverFacade> mapReceivers(
      BlockingQueue<ServiceBusMessageHolder> recordsQueue,
      List<Kcql> kcqls,
      String connectionString
  ) {
    return kcqls.stream().map(kcql -> new ServiceBusReceiverFacade(kcql, recordsQueue, connectionString,
        FACADE_CLASS_SIMPLE_NAME + UUID.randomUUID()))
        .collect(Collectors.toMap(
            ServiceBusReceiverFacade::getReceiverId, e -> e
        ));

  }
}
