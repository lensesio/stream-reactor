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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.lenses.streamreactor.connect.reporting.model.RecordReport;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ReportHolderTest {

  private static final int DEFAULT_OFFER_TIME_MILLIS = 300;
  private static final int DEFAULT_POLL_TIME_MILLIS = 100;

  @Test
  void enqueueReport() throws InterruptedException {
    //given
    BlockingQueue queue = mock(BlockingQueue.class);
    RecordReport report = mock(RecordReport.class);

    //when
    ReportHolder reportHolder = new ReportHolder(queue);
    reportHolder.enqueueReport(report);

    //then
    verify(queue).offer(report, DEFAULT_OFFER_TIME_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Test
  void pollReport() throws InterruptedException {
    //given
    BlockingQueue queue = mock(BlockingQueue.class);

    //when
    ReportHolder reportHolder = new ReportHolder(queue);
    reportHolder.pollReport();

    //then
    verify(queue).poll(DEFAULT_POLL_TIME_MILLIS, TimeUnit.MILLISECONDS);
  }
}
