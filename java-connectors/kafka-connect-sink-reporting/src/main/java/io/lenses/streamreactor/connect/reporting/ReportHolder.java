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

import cyclops.control.Try;
import io.lenses.streamreactor.connect.reporting.exceptions.ReportingException;
import io.lenses.streamreactor.connect.reporting.model.RecordReport;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ReportHolder {

  private static final int DEFAULT_QUEUES_SIZE = 1000;
  private static final int DEFAULT_OFFER_TIME_MILLIS = 300;
  private static final int DEFAULT_POLL_TIME_MILLIS = 100;
  private final BlockingQueue<RecordReport> reportsToSend;

  public ReportHolder(BlockingQueue<RecordReport> recordReportsQueue) {
    this.reportsToSend =
        Optional.ofNullable(recordReportsQueue)
            .orElse(new ArrayBlockingQueue<>(DEFAULT_QUEUES_SIZE));
  }

  /**
   * Offers Report to be queued for ReportSender to send. Since reporting is non-critical operation,
   * if it fails, the connector just leaves it.
   */
  public void enqueueReport(RecordReport recordReport) {
    Try.withCatch(() -> reportsToSend.offer(recordReport, DEFAULT_OFFER_TIME_MILLIS, TimeUnit.MILLISECONDS));
  }

  /**
   * Polls for report to send.
   * 
   * @return RecordReport instance or null if the specified waiting time elapses before an element is available
   */
  public RecordReport pollReport() {
    try {
      return reportsToSend.poll(DEFAULT_POLL_TIME_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new ReportingException("InterruptedException happened:", e);
    }
  }

}
