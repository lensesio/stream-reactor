package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

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
import io.lenses.java.streamreactor.common.util.JarManifest;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class AzureEventHubsSourceTaskTest {

  private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.of(30, ChronoUnit.SECONDS);
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
    EventHubsKafkaConsumerController mockedController = mock(EventHubsKafkaConsumerController.class);
    testObj.initialize(mockedController, null);

    //when
    testObj.stop();

    //then
    verify(mockedController, times(1)).close(DEFAULT_CLOSE_TIMEOUT);
  }

  @Test
  void initializeShouldLogAndSubscribeToTopic() {
    //given
    String topic = "topic";
    EventHubsKafkaConsumerController mockedController = mock(EventHubsKafkaConsumerController.class);
    testObj.initialize(mockedController, topic);

    //when
    testObj.stop();

    //then
    assertEquals(1, logWatcher.list.size());
    assertEquals("AzureEventHubsSourceTask initialising.", logWatcher.list.get(0).getFormattedMessage());
    verify(mockedController, times(1)).subscribeToTopic(topic);
  }

  @Test
  void pollShouldCallPollOnControllerAndReturnNullIfListIsEmpty() throws InterruptedException {
    //given
    EventHubsKafkaConsumerController mockedController = mock(EventHubsKafkaConsumerController.class);
    testObj.initialize(mockedController, null);
    when(mockedController.poll(any(Duration.class))).thenReturn(Collections.emptyList());

    //when
    List<SourceRecord> poll = testObj.poll();

    //then
    assertNull(poll);
  }

  @Test
  void pollShouldCallPollOnControllerAndReturnListThatHasElements() throws InterruptedException {
    //given
    EventHubsKafkaConsumerController mockedController = mock(EventHubsKafkaConsumerController.class);
    testObj.initialize(mockedController, null);
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
    EventHubsKafkaConsumerController mockedController = mock(EventHubsKafkaConsumerController.class);
    testObj.initialize(mockedController, null);
    final String SOME_VERSION = "SOME_VERSION";
    when(mockedJarManifest.getVersion()).thenReturn(SOME_VERSION);

    //when
    String version = testObj.version();

    //then
    assertEquals(SOME_VERSION, version);
    verify(mockedJarManifest, atMostOnce()).getVersion();
  }
}