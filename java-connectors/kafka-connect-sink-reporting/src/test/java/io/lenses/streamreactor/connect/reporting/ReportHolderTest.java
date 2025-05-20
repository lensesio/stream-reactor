/*
 * Copyright 2017-2025 Lenses.io Ltd
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
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.lenses.streamreactor.test.utils.OptionValues.getValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReportHolderTest {

  private static final int DEFAULT_OFFER_TIME_MILLIS = 300;
  private static final int DEFAULT_POLL_TIME_MILLIS = 100;

  @Test
  void enqueueReport() throws InterruptedException {
    //given
    BlockingQueue queue = mock(BlockingQueue.class);
    ReportingRecord report = mock(ReportingRecord.class);

    //when
    ReportHolder reportHolder = new ReportHolder(queue);
    reportHolder.enqueueReport(report);

    //then
    verify(queue).offer(report, DEFAULT_OFFER_TIME_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  void pollReportShouldCallPollOnQueue() throws InterruptedException {
    //given
    BlockingQueue queue = mock(BlockingQueue.class);
    ReportingRecord reportingRecord = mock(ReportingRecord.class);
    when(queue.poll(DEFAULT_POLL_TIME_MILLIS, TimeUnit.MILLISECONDS)).thenReturn(reportingRecord);

    //when
    ReportHolder reportHolder = new ReportHolder(queue);
    Option<ReportingRecord> recordReport = reportHolder.pollReport();

    //then
    assertTrue(recordReport.isPresent());
    assertEquals(reportingRecord, getValue(recordReport));
    verify(queue).poll(DEFAULT_POLL_TIME_MILLIS, TimeUnit.MILLISECONDS);
  }
}
