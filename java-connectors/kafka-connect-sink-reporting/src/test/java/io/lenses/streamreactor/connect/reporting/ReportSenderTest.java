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
import io.lenses.streamreactor.connect.reporting.model.ProducerRecordConverter;
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord;
import lombok.val;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReportSenderTest {

  private static final TopicPartition topicPartition = new TopicPartition("myTopic", 5);
  private static final ReportingMessagesConfig reportTopic = new ReportingMessagesConfig("test-topic", Option.of(1));

  private ReportSender reportSender;
  private ReportHolder mockReportHolder;
  private Producer<byte[], String> mockProducer;
  private ScheduledExecutorService mockExecutorService;
  private ReportingRecord mockReportingRecord;
  private Map<String, Object> senderConfig;

  @BeforeEach
  void setUp() {
    val converter = new ProducerRecordConverter();
    mockReportHolder = mock(ReportHolder.class);
    mockProducer = mock(Producer.class);
    mockExecutorService = mock(ScheduledExecutorService.class);
    mockReportingRecord = mock(ReportingRecord.class);
    when(mockReportingRecord.getTopicPartition()).thenReturn(topicPartition);
    when(mockReportingRecord.getError()).thenReturn(Option.none());
    reportSender =
        new ReportSender(converter, "test-client-id", mockReportHolder, mockProducer, mockExecutorService, reportTopic);

    senderConfig = new HashMap<>();
    senderConfig.put(ReportProducerConfigConst.TOPIC, "test-topic");

  }

  @Test
  void testEnqueue() {
    reportSender.enqueue(mockReportingRecord);

    verify(mockReportHolder, times(1)).enqueueReport(mockReportingRecord);
  }

  @Test
  void testStart() {
    when(mockReportHolder.pollReport()).thenReturn(Option.of(mockReportingRecord));

    reportSender.start();

    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockExecutorService, times(1)).scheduleWithFixedDelay(runnableCaptor.capture(), eq(0L), eq(1L), eq(
        TimeUnit.SECONDS));

    // Execute the captured runnable
    Runnable capturedRunnable = runnableCaptor.getValue();
    capturedRunnable.run();

    verify(mockReportHolder, times(1)).pollReport();
    verify(mockProducer, times(1)).send(any(ProducerRecord.class));
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
