package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EventHubsKafkaConsumerControllerTest { //TODO clean up this class

  //private static final String CONNECTOR_NAME = "name";
  //private static final String SOME_NAME = "somename";
  //private AzureEventHubsConfig hubsConfig;

  private ArrayBlockingQueue<ConsumerRecords<String, String>> recordsQueue;

  private EventHubsKafkaConsumerController testObj;


  @BeforeEach
  void setUp() {
    //hubsConfig = mock(AzureEventHubsConfig.class);
    recordsQueue = new ArrayBlockingQueue<>(10);
  }

  //@Test
  //void createShouldFetchRelevantPropertiesAndCallForConsumerCreation() { //TODO remove
  //  //given
  //  mockPropertiesReturn();
  //  List<String> bootstrapServers = Collections.singletonList("some.server:9393");
  //  String groupId = "group.id";
  //  Password jaas = new Password("jaas.pass");
  //  String saasMechanism = "saasMechanism";
  //  String secProtocol = "secProtocol";
  //  String eventHubName = "eventHubTopic";
  //  Class stringDeserializer = StringDeserializer.class;
  //
  //  //when
  //  testObj = new EventHubsKafkaConsumerController(hubsConfig, consumerProvider, recordsQueue);
  //
  //  //then
  //  verify(consumerProvider).createProducer(any(AzureEventHubsConfig.class), eq(recordsQueue));
  //}

  //private void mockPropertiesReturn() {
  //  List<String> bootstrapServers = Collections.singletonList("some.server:9393");
  //  String groupId = "group.id";
  //  String eventHubName = "eventHubTopic";
  //  Password jaas = new Password("jaas.pass");
  //  String saasMechanism = "saasMechanism";
  //  String secProtocol = "secProtocol";
  //  Class stringDeserializer = StringDeserializer.class;
  //  when(this.hubsConfig.getList(
  //      getPrefixedKafkaConsumerConfigKey(BOOTSTRAP_SERVERS_CONFIG))).thenReturn(bootstrapServers);
  //  when(this.hubsConfig.getString(
  //      AzureEventHubsConfigConstants.EVENTHUB_NAME)).thenReturn(eventHubName);
  //  when(this.hubsConfig.getString(
  //      getPrefixedKafkaConsumerConfigKey(GROUP_ID_CONFIG))).thenReturn(groupId);
  //  when(this.hubsConfig.getPassword(
  //      getPrefixedKafkaConsumerConfigKey(SaslConfigs.SASL_JAAS_CONFIG))).thenReturn(jaas);
  //  when(this.hubsConfig.getString(
  //      getPrefixedKafkaConsumerConfigKey(SaslConfigs.SASL_MECHANISM))).thenReturn(saasMechanism);
  //  when(this.hubsConfig.getString(
  //      getPrefixedKafkaConsumerConfigKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG))).thenReturn(secProtocol);
  //  when(this.hubsConfig.getClass(
  //      getPrefixedKafkaConsumerConfigKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))).thenReturn(stringDeserializer);
  //  when(this.hubsConfig.getClass(
  //      getPrefixedKafkaConsumerConfigKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG))).thenReturn(stringDeserializer);
  //}

  @Test
  void pollShouldPollQueueAndReturnSourceRecords() throws InterruptedException {
    //given
    Duration duration = Duration.of(2, ChronoUnit.SECONDS);
    BlockingQueuedKafkaProducer mockedBlockingProducer = mock(
        BlockingQueuedKafkaProducer.class);
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue);
    ConsumerRecord consumerRecord = mock(ConsumerRecord.class);
    List<ConsumerRecord<String, String>> consumerRecordList = Collections.singletonList(consumerRecord);
    ConsumerRecords mockedRecords = mock(ConsumerRecords.class);
    when(mockedRecords.count()).thenReturn(consumerRecordList.size());
    when(mockedRecords.iterator()).thenReturn(consumerRecordList.iterator());
    Headers headersMock = mock(Headers.class);
    List<Header> emptyHeaderList = new ArrayList<>();
    when(headersMock.iterator()).thenReturn(emptyHeaderList.iterator());
    when(consumerRecord.headers()).thenReturn(headersMock);
    recordsQueue.put(mockedRecords);

    //when

    List<SourceRecord> sourceRecords = testObj.poll(duration);

    //then
    verify(mockedBlockingProducer).start();
    assertNotNull(mockedRecords);
    assertEquals(1, sourceRecords.size());
  }

  @Test
  void closeShouldCloseTheProducer() {
    //given
    Duration duration = Duration.of(2, ChronoUnit.SECONDS);
    BlockingQueuedKafkaProducer mockedBlockingProducer = mock(
        BlockingQueuedKafkaProducer.class);
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue);

    //when
    testObj.close(duration);

    //then
    verify(mockedBlockingProducer).stop(duration);
  }
}