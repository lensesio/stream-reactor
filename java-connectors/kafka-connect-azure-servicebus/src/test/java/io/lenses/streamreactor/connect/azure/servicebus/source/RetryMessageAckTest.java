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

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.util.retry.RetryBackoffSpec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class RetryMessageAckTest {

  @Test
  void callsTheCompleteMethodOnSuccessfulMono() {
    // given
    CountDownLatch successLatch = new CountDownLatch(1);
    AtomicReference<Throwable> serviceBusReceiverError = new AtomicReference<>();
    final RetryMessageAck retryMessageAck =
        new RetryMessageAck(RetryBackoffSpec.backoff(
            3, Duration.ofMillis(100)), v -> {
              successLatch.countDown();
            }, serviceBusReceiverError::set);

    Mono<Void> successfulMono = Mono.empty();
    retryMessageAck.acknowledge(successfulMono, "msgId");
    try {
      successLatch.await(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertEquals(0, successLatch.getCount());
  }

  @Test
  void callsTheErrorMethodOnFailedMono() {
    CountDownLatch successLatch = new CountDownLatch(1);
    AtomicReference<Throwable> serviceBusReceiverError = new AtomicReference<>();
    final RetryMessageAck retryMessageAck =
        new RetryMessageAck(RetryBackoffSpec.backoff(
            3, Duration.ofMillis(100)), v -> {
              System.out.println("Success");
              successLatch.countDown();
            }, serviceBusReceiverError::set);

    Mono<Void> failedMono = Mono.error(new RuntimeException("Failed"));
    retryMessageAck.acknowledge(failedMono, "msgId");
    try {
      successLatch.await(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertEquals(1, successLatch.getCount());

    Throwable throwable = serviceBusReceiverError.get();
    assertNotNull(throwable);
    assertEquals("reactor.core.Exceptions$RetryExhaustedException", throwable.getClass().getName());
    assertTrue(throwable.getMessage().contains("Retries exhausted: 3/3"));
  }

}
