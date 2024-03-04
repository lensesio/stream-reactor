package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Test;

class BlockingQueuedKafkaProducerTest {

  private static final String CLIENT_ID = "clientId";
  private static Consumer consumer = mock(Consumer.class);

  BlockingQueuedKafkaProducer testObj = new BlockingQueuedKafkaProducer(mock(BlockingQueue.class),
      consumer, CLIENT_ID, "topic");

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