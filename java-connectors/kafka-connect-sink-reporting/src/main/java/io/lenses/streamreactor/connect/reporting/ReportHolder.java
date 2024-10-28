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
import cyclops.control.Try;
import io.lenses.streamreactor.connect.reporting.model.ConnectorSpecificRecordData;
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.function.Function.identity;

/**
 * @param <C> the type of connector-specific record data
 */
@AllArgsConstructor
@Slf4j
public class ReportHolder<C extends ConnectorSpecificRecordData> {

  private static final int DEFAULT_OFFER_TIME_MILLIS = 300;
  private static final int DEFAULT_POLL_TIME_MILLIS = 100;

  private final BlockingQueue<ReportingRecord<C>> pendingReportsQueue;

  /**
   * Offers Report to be queued for ReportSender to send. Since reporting is non-critical operation,
   * if it fails, the connector just leaves it.
   */
  public void enqueueReport(ReportingRecord<C> recordReport) {
    Try.withCatch(() -> pendingReportsQueue.offer(recordReport, DEFAULT_OFFER_TIME_MILLIS, TimeUnit.MILLISECONDS));
  }

  /**
   * Polls for report to send.
   *
   * @return Option containing ReportingRecord instance or Option.None if the specified waiting time elapses before an
   *         element is available
   */
  public Option<ReportingRecord<C>> pollReport() {
    log.debug("Long polling for reports...");
    return Try.withCatch(
        () -> Option.ofNullable(pendingReportsQueue.poll(DEFAULT_POLL_TIME_MILLIS, TimeUnit.MILLISECONDS)),
        InterruptedException.class
    )
        .toOption()
        .peek(e -> log.debug("Report found in ReportHolder"))
        .flatMap(identity());
  }

}
