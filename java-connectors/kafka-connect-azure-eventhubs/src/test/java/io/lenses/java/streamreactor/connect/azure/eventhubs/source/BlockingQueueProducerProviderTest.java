package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig.getPrefixedKafkaConsumerConfigKey;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

class BlockingQueueProducerProviderTest {

  private ListAppender<ILoggingEvent> logWatcher;

  @BeforeEach
  void setup() {
    logWatcher = new ListAppender<>();
    logWatcher.start();
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(BlockingQueueProducerProvider.class)).addAppender(logWatcher);
  }

  @AfterEach
  void teardown() {
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(BlockingQueueProducerProvider.class)).detachAndStopAllAppenders();
  }

  @Test
  void whenConstructorInvokedWithParameters_ThenMockKafkaConsumerShouldBeCreatedAndLogged(){
    //given
    AzureEventHubsConfig azureConfigMock = mock(AzureEventHubsConfig.class);
    BlockingQueueProducerProvider testObj = new BlockingQueueProducerProvider();

    //when
    BlockingQueuedKafkaProducer consumer;
    try(MockedConstruction<KafkaConsumer> mockKafkaConsumer = Mockito.mockConstruction(KafkaConsumer.class)){
      consumer = testObj.createProducer(azureConfigMock, new ArrayBlockingQueue<>(1));
    }

    //then
    verify(azureConfigMock).getString(AzureEventHubsConfigConstants.CONNECTOR_NAME);
    verify(azureConfigMock).getString(getPrefixedKafkaConsumerConfigKey(GROUP_ID_CONFIG));
    verify(azureConfigMock, times(2)).getClass(anyString());
    assertNotNull(consumer);
    assertEquals(1, logWatcher.list.size());
    assertTrue(logWatcher.list.get(0).getFormattedMessage().startsWith("Attempting to create Client with Id"));
  }
}