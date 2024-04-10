package io.lenses.streamreactor.connect.azure.eventhubs.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsSourceConfig;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigException;
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
  void whenConstructorInvokedWithoutOffsetParameter_ThenConfigExceptionIsThrown(){
    //given
    AzureEventHubsSourceConfig azureConfigMock = mock(AzureEventHubsSourceConfig.class);
    TopicPartitionOffsetProvider mockedOffsetProvider = mock(TopicPartitionOffsetProvider.class);


    //when
    BlockingQueueProducerProvider testObj = new BlockingQueueProducerProvider(
        mockedOffsetProvider);
    ConfigException configException;
    try(MockedConstruction<KafkaConsumer> ignored = Mockito.mockConstruction(KafkaConsumer.class)){
      configException = assertThrows(ConfigException.class, () -> {
        testObj.createProducer(azureConfigMock, new ArrayBlockingQueue<>(1),
            new HashMap<>());
      });
    }


    //then
    assertEquals("Invalid value null for configuration connect.eventhubs.source.default.offset: "
            + "allowed values are: earliest/latest", configException.getMessage());
  }

  @Test
  void whenConstructorInvokedWithParameters_ThenMockKafkaConsumerShouldBeCreatedAndLogged(){
    //given
    String earliestOffset = "earliest";
    TopicPartitionOffsetProvider mockedOffsetProvider = mock(TopicPartitionOffsetProvider.class);

    AzureEventHubsSourceConfig azureConfigMock = mock(AzureEventHubsSourceConfig.class);
    when(azureConfigMock.getString(AzureEventHubsConfigConstants.CONSUMER_OFFSET)).thenReturn(
        earliestOffset);
    when(azureConfigMock.getString(AzureEventHubsConfigConstants.KCQL_CONFIG))
        .thenReturn("insert into output select * from input");

    //when
    BlockingQueueProducerProvider testObj = new BlockingQueueProducerProvider(
        mockedOffsetProvider);
    KafkaByteBlockingQueuedProducer consumer;
    try(MockedConstruction<KafkaConsumer> ignored = Mockito.mockConstruction(KafkaConsumer.class)){
      consumer = testObj.createProducer(azureConfigMock, new ArrayBlockingQueue<>(1),
          new HashMap<>());
    }

    //then
    verify(azureConfigMock).getString(AzureEventHubsConfigConstants.CONNECTOR_NAME);
    assertNotNull(consumer);
    assertEquals(1, logWatcher.list.size());
    assertTrue(logWatcher.list.get(0).getFormattedMessage().startsWith("Attempting to create Client with Id"));
  }
}