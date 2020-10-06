/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.write.batch;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.concurrent.CountDownLatch;

/**
 * A Runnable that counts down, and then waits for the countdown to be finished.
 */
public class CountDownRunnable implements Runnable {

  private CountDownLatch countDownLatch;

  public CountDownRunnable(CountDownLatch countDownLatch) {
    this.countDownLatch = countDownLatch;
  }

  @Override
  public void run() {
    countDownLatch.countDown();
    try {
      /*
       * Hog this thread until ALL threads are finished counting down.  This is needed so that
       * this thread doesn't start processing another countdown. If countdown tasks are holding onto
       * all the threads, then we know that nothing that went in before the countdown is still
       * processing.
       */
      countDownLatch.await();
    } catch (InterruptedException err) {
      throw new ConnectException("Thread interrupted while waiting for countdown.", err);
    }
  }
}
