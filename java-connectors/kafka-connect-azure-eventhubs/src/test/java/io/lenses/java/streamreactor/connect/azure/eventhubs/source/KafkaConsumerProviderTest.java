package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

class KafkaConsumerProviderTest {

  private ListAppender<ILoggingEvent> logWatcher;

  @BeforeEach
  void setup() {
    logWatcher = new ListAppender<>();
    logWatcher.start();
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(KafkaConsumerProvider.class)).addAppender(logWatcher);
  }

  @AfterEach
  void teardown() {
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(KafkaConsumerProvider.class)).detachAndStopAllAppenders();
  }

  @Test
  void whenConstructorInvokedWithParameters_ThenMockKafkaConsumerShouldBeCreatedAndLogged(){
    //given
    KafkaConsumerProvider testObj = new KafkaConsumerProvider();

    //when
    BlockingQueuedKafkaConsumer consumer;
    try(MockedConstruction<KafkaConsumer> mockKafkaConsumer = Mockito.mockConstruction(KafkaConsumer.class)){
      consumer = testObj.createConsumer(new Properties(), new ArrayBlockingQueue<>(1));
    }

    //then
    assertNotNull(consumer);
    assertEquals(1, logWatcher.list.size());
    assertEquals("Attempting to create Client with Id:KafkaEventHubConsumer#0", logWatcher.list.get(0).getFormattedMessage());
  }
}