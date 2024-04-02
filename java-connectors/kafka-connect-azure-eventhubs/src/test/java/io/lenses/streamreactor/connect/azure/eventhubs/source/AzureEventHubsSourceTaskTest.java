package io.lenses.streamreactor.connect.azure.eventhubs.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsSourceConfig;
import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

class AzureEventHubsSourceTaskTest {

  private AzureEventHubsSourceTask testObj;
  private ListAppender<ILoggingEvent> logWatcher;
  private JarManifest mockedJarManifest = mock(JarManifest.class);

  @BeforeEach
  void setup() {
    mockedJarManifest = mock(JarManifest.class);
    testObj = new AzureEventHubsSourceTask(mockedJarManifest);
    logWatcher = new ListAppender<>();
    logWatcher.start();
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AzureEventHubsSourceTask.class)).addAppender(logWatcher);
  }

  @AfterEach
  void teardown() {
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AzureEventHubsSourceTask.class)).detachAndStopAllAppenders();
  }

  @Test
  void stopShouldCallStopOnController() {
    //given
    Duration thirtySeconds = Duration.ofSeconds(30);
    EventHubsKafkaConsumerController mockedController = Mockito.mock(EventHubsKafkaConsumerController.class);
    AzureEventHubsSourceConfig azureEventHubsSourceConfig = mock(AzureEventHubsSourceConfig.class);
    when(azureEventHubsSourceConfig.getInt(AzureEventHubsConfigConstants.CONSUMER_CLOSE_TIMEOUT))
        .thenReturn(thirtySeconds.toSecondsPart());
    testObj.initialize(mockedController, azureEventHubsSourceConfig);

    //when
    testObj.stop();

    //then
    verify(mockedController, times(1)).close(thirtySeconds);
  }

  @Test
  void initializeShouldLog() {
    //given
    EventHubsKafkaConsumerController mockedController = Mockito.mock(EventHubsKafkaConsumerController.class);
    AzureEventHubsSourceConfig azureEventHubsSourceConfig = mock(AzureEventHubsSourceConfig.class);
    testObj.initialize(mockedController, azureEventHubsSourceConfig);

    //when
    testObj.stop();

    //then
    assertEquals(1, logWatcher.list.size());
    assertEquals("AzureEventHubsSourceTask initialised.", logWatcher.list.get(0).getFormattedMessage());
  }

  @Test
  void pollShouldCallPollOnControllerAndReturnNullIfListIsEmpty() throws InterruptedException {
    //given
    AzureEventHubsSourceConfig azureEventHubsSourceConfig = mock(AzureEventHubsSourceConfig.class);
    EventHubsKafkaConsumerController mockedController = Mockito.mock(EventHubsKafkaConsumerController.class);
    testObj.initialize(mockedController, azureEventHubsSourceConfig);
    when(mockedController.poll(any(Duration.class))).thenReturn(Collections.emptyList());

    //when
    List<SourceRecord> poll = testObj.poll();

    //then
    assertNull(poll);
  }

  @Test
  void pollShouldCallPollOnControllerAndReturnListThatHasElements() throws InterruptedException {
    //given
    AzureEventHubsSourceConfig azureEventHubsSourceConfig = mock(AzureEventHubsSourceConfig.class);
    EventHubsKafkaConsumerController mockedController = Mockito.mock(EventHubsKafkaConsumerController.class);
    testObj.initialize(mockedController, azureEventHubsSourceConfig);
    SourceRecord mockedRecord = mock(SourceRecord.class);
    List<SourceRecord> sourceRecords = Collections.singletonList(mockedRecord);
    when(mockedController.poll(any(Duration.class))).thenReturn(sourceRecords);

    //when
    List<SourceRecord> poll = testObj.poll();

    //then
    assertNotNull(poll);
    assertIterableEquals(sourceRecords, poll);
  }

  @Test
  void getVersionShouldDelegateToJarManifestGetVersion() {
    //given
    AzureEventHubsSourceConfig azureEventHubsSourceConfig = mock(AzureEventHubsSourceConfig.class);
    EventHubsKafkaConsumerController mockedController = Mockito.mock(EventHubsKafkaConsumerController.class);
    testObj.initialize(mockedController, azureEventHubsSourceConfig);
    final String SOME_VERSION = "SOME_VERSION";
    when(mockedJarManifest.getVersion()).thenReturn(SOME_VERSION);

    //when
    String version = testObj.version();

    //then
    assertEquals(SOME_VERSION, version);
    verify(mockedJarManifest, atMostOnce()).getVersion();
  }
}