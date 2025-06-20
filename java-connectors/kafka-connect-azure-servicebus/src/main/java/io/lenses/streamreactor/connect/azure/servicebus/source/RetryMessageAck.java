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

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.util.retry.RetryBackoffSpec;

import java.util.function.Consumer;

/**
 * Provides a mechanism for executing a reactive operation with retry logic on failure.
 * Specifically designed to acknowledge a ServiceBus message and retry the acknowledgment if it fails.
 */

@Slf4j
public class RetryMessageAck implements MessageAck {

  private final RetryBackoffSpec retrySpec;
  private final Consumer<String> onSuccess;
  private final Consumer<Throwable> onError;

  public RetryMessageAck(RetryBackoffSpec retrySpec,
      Consumer<String> onSuccess,
      Consumer<Throwable> onError) {
    this.retrySpec = retrySpec;
    this.onSuccess = onSuccess;
    this.onError = onError;
  }

  public void acknowledge(Mono<Void> mono, String msgId) {
    acknowledgeMono(mono, msgId)
        .subscribe();
  }

  public Mono<Void> acknowledgeMono(Mono<Void> mono, String msgId) {
    return mono
        .retryWhen(this.retrySpec)
        .doOnError(onError)
        .doOnSuccess(v -> onSuccess.accept(msgId));
  }

  public static Consumer<String> logSuccess() {
    return msgId -> log.debug("Message acknowledged successfully. Message ID: {}", msgId);
  }

  public static Consumer<Throwable> logError() {
    return throwable -> log.error("Error acknowledging message", throwable);
  }

  /**
   * Create a new instance of RetryMessageAck with the given RetryBackoffSpec and defaulting to logging success and
   * error messages.
   * 
   * @param retrySpec
   * @return
   */
  public static RetryMessageAck create(RetryBackoffSpec retrySpec) {
    return new RetryMessageAck(retrySpec, logSuccess(), logError());
  }
}
