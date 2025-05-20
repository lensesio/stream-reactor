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

import reactor.core.publisher.Mono;

/**
 * Interface for acknowledging a ServiceBus messages.
 */
public interface MessageAck {

  /**
   * Acknowledge the message.
   *
   * @param mono  Mono created from async client complete method.
   * @param msgId ServiceBus message ID.
   */
  void acknowledge(Mono<Void> mono, String msgId);
}
