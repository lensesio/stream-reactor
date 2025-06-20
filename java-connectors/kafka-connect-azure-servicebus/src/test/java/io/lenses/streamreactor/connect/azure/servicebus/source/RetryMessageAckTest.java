/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.RetryBackoffSpec;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryMessageAckTest {

  @Test
  void callsTheCompleteMethodOnSuccessfulMono() {
    AtomicReference<String> successMsgId = new AtomicReference<>();
    AtomicReference<Throwable> serviceBusReceiverError = new AtomicReference<>();
    final RetryMessageAck retryMessageAck =
        new RetryMessageAck(RetryBackoffSpec.backoff(
            3, Duration.ofMillis(100)),
            successMsgId::set,
            serviceBusReceiverError::set);

    Mono<Void> successfulMono = Mono.empty();

    StepVerifier.create(retryMessageAck.acknowledgeMono(successfulMono, "msgId"))
        .verifyComplete();

    assertEquals("msgId", successMsgId.get(), "onSuccess should be called with the message ID on success");
    assertNull(serviceBusReceiverError.get(), "onError should not be called on success");
  }

  @Test
  void callsTheErrorMethodOnFailedMono() {
    AtomicReference<String> successMsgId = new AtomicReference<>();
    AtomicReference<Throwable> serviceBusReceiverError = new AtomicReference<>();
    final RetryMessageAck retryMessageAck =
        new RetryMessageAck(RetryBackoffSpec.backoff(
            3, Duration.ofMillis(10)),
            successMsgId::set,
            serviceBusReceiverError::set);

    Mono<Void> failedMono = Mono.error(new RuntimeException("Failed"));

    StepVerifier.create(retryMessageAck.acknowledgeMono(failedMono, "msgId"))
        .expectErrorSatisfies(throwable -> {
          assertEquals("reactor.core.Exceptions$RetryExhaustedException", throwable.getClass().getName(),
              "Exception should be RetryExhaustedException after retries");
          assertTrue(throwable.getMessage().contains("Retries exhausted: 3/3"),
              "Exception message should indicate retries exhausted");
        })
        .verify();

    assertNull(successMsgId.get(), "onSuccess should not be called on error");
    assertNotNull(serviceBusReceiverError.get(), "onError should be called on error");
    assertEquals("reactor.core.Exceptions$RetryExhaustedException", serviceBusReceiverError.get().getClass().getName(),
        "onError should receive RetryExhaustedException");
    assertTrue(serviceBusReceiverError.get().getMessage().contains("Retries exhausted: 3/3"),
        "onError exception message should indicate retries exhausted");
  }
}
