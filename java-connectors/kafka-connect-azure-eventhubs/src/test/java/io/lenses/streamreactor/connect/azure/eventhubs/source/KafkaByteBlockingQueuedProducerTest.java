/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.azure.eventhubs.source;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.lenses.streamreactor.connect.azure.eventhubs.config.SourceDataType.KeyValueTypes;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.internal.util.collections.Sets;

class KafkaByteBlockingQueuedProducerTest {

  private static final String CLIENT_ID = "clientId";
  private static Consumer consumer = mock(Consumer.class);

  KafkaByteBlockingQueuedProducer testObj = new KafkaByteBlockingQueuedProducer(
      mock(TopicPartitionOffsetProvider.class), mock(BlockingQueue.class),
      consumer, KeyValueTypes.DEFAULT_TYPES,
      CLIENT_ID, Sets.newSet("topic"), false);

  @Test
  void closeShouldBeDelegatedToKafkaConsumer() {
    //given
    Duration tenSeconds = Duration.of(10, ChronoUnit.SECONDS);

    //when
    testObj.stop(tenSeconds);

    //then
    verify(consumer).close(eq(tenSeconds));
  }
}