/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.reporting;

import cyclops.control.Option;
import io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst;
import io.lenses.streamreactor.connect.reporting.model.RecordConverter;
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReportSenderTest {

  private static final TopicPartition topicPartition = new TopicPartition("myTopic", 5);
  private static final ReportingMessagesConfig reportTopic = new ReportingMessagesConfig("test-topic", Option.of(1));

  @Mock
  private ReportHolder<TestConnectorSpecificRecordDataData> mockReportHolder;

  @Mock
  private ScheduledFuture<RecordMetadata> future;

  @Mock
  private Producer<byte[], String> mockProducer;
  @Mock
  private ScheduledExecutorService mockExecutorService;
  @Mock
  private ReportingRecord<TestConnectorSpecificRecordDataData> mockReportingRecord;
  @Mock
  private RecordConverter<TestConnectorSpecificRecordDataData> recordConverter;

  @Mock
  private ProducerRecord<byte[], String> producerRecord;

  private final Map<String, Object> senderConfig = Map.of(ReportProducerConfigConst.TOPIC, "test-topic");

  @InjectMocks
  private ReportSender<TestConnectorSpecificRecordDataData> reportSender;

  @Test
  void testEnqueue() {
    reportSender.enqueue(mockReportingRecord);

    verify(mockReportHolder, times(1)).enqueueReport(mockReportingRecord);
  }

  @Test
  void testStart() {

    when(mockReportHolder.pollReport()).thenReturn(Option.of(mockReportingRecord));
    when(recordConverter.convert(mockReportingRecord)).thenReturn(Option.of(producerRecord));
    when(mockProducer.send(producerRecord)).thenReturn(future);

    reportSender.start();

    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService, times(1)).scheduleWithFixedDelay(runnableCaptor.capture(), eq(0L), eq(50L), eq(
        TimeUnit.MILLISECONDS));

    // Execute the captured runnable
    Runnable capturedRunnable = runnableCaptor.getValue();
    capturedRunnable.run();

    verify(mockReportHolder, times(1)).pollReport();
    verify(mockProducer, times(1)).send(producerRecord);
  }

  @Test
  void testClose() throws InterruptedException {
    reportSender.close();

    verify(mockExecutorService, times(1)).awaitTermination(500, TimeUnit.MILLISECONDS);
    verify(mockProducer, times(1)).close(Duration.ofMillis(500));
  }

  @Test
  void testAddExtraConfig() {
    String reportingClientId = "test-client-id";

    Map<String, Object> newConfig = ReportSender.addExtraConfig(senderConfig, reportingClientId);

    assertEquals(reportingClientId, newConfig.get(ProducerConfig.CLIENT_ID_CONFIG));
    assertEquals(ByteArraySerializer.class.getName(), newConfig.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    assertEquals(StringSerializer.class.getName(), newConfig.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));

    assertThatMapIsImmutable(newConfig);

  }

  private static void assertThatMapIsImmutable(Map<String, Object> newConfig) {
    assertThrows(UnsupportedOperationException.class, () -> newConfig.put("newKey", "newValue"));
  }
}
