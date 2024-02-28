package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Test;

class BlockingQueuedKafkaConsumerTest {

  private static final String CLIENT_ID = "clientId";
  private static Consumer consumer = mock(Consumer.class);

  BlockingQueuedKafkaConsumer testObj = new BlockingQueuedKafkaConsumer(mock(BlockingQueue.class),
      consumer, CLIENT_ID);

  @Test
  void subscribeShouldBeDelegatedToKafkaConsumer() {
    //given
    String someTopic = "some-topic";
    List<String> topics = singletonList(someTopic);

    //when
    testObj.subscribe(topics);

    //then
    verify(consumer).subscribe(eq(topics), any(AzureConsumerRebalancerListener.class));

  }

  @Test
  void closeShouldBeDelegatedToKafkaConsumer() {
    //given
    Duration tenSeconds = Duration.of(10, ChronoUnit.SECONDS);

    //when
    testObj.close(tenSeconds);

    //then
    verify(consumer).close(eq(tenSeconds));
  }
}