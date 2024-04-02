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

class KafkaByteBlockingQueuedProducerTest {

  private static final String CLIENT_ID = "clientId";
  private static Consumer consumer = mock(Consumer.class);

  KafkaByteBlockingQueuedProducer testObj = new KafkaByteBlockingQueuedProducer(
      mock(TopicPartitionOffsetProvider.class), mock(BlockingQueue.class),
      consumer, KeyValueTypes.DEFAULT_TYPES,
      CLIENT_ID, "topic", false);

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