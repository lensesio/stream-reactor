package com.wepay.kafka.connect.bigquery.write.batch;

/*
 * Copyright 2016 WePay, Inc.
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


import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ThreadPoolExecutor for writing Rows to BigQuery.
 *
 * <p>Keeps track of the number of threads actively writing for each topic.
 * Keeps track of the number of failed threads in each batch of requests.
 */
public class KCBQThreadPoolExecutor extends ThreadPoolExecutor {

  private static final Logger logger = LoggerFactory.getLogger(KCBQThreadPoolExecutor.class);


  private ConcurrentHashMap.KeySetView<Throwable, Boolean> encounteredErrors =
      ConcurrentHashMap.newKeySet();

  /**
   * @param config the {@link BigQuerySinkTaskConfig}
   * @param workQueue the queue for storing tasks.
   */
  public KCBQThreadPoolExecutor(BigQuerySinkTaskConfig config,
                                BlockingQueue<Runnable> workQueue) {
    super(config.getInt(BigQuerySinkTaskConfig.THREAD_POOL_SIZE_CONFIG),
          config.getInt(BigQuerySinkTaskConfig.THREAD_POOL_SIZE_CONFIG),
          // the following line is irrelevant because the core and max thread counts are the same.
          1, TimeUnit.SECONDS,
          workQueue);
  }

  @Override
  protected void afterExecute(Runnable runnable, Throwable throwable) {
    super.afterExecute(runnable, throwable);

    // Skip interrupted exceptions, as they are thrown by design on task shutdown
    if (throwable != null && !(throwable instanceof ExpectedInterruptException)) {
      logger.error("Task failed with {} error: {}",
                   throwable.getClass().getName(),
                   throwable.getMessage());
      logger.debug("Error Task Stacktrace:", throwable);
      encounteredErrors.add(throwable);
    }
  }

  /**
   * Wait for all the currently queued tasks to complete, and then return.
   *
   * @throws BigQueryConnectException if any of the tasks failed.
   * @throws InterruptedException if interrupted while waiting.
   */
  public void awaitCurrentTasks() throws InterruptedException, BigQueryConnectException {
    /*
     * create CountDownRunnables equal to the number of threads in the pool and add them to the
     * queue. Then wait for all CountDownRunnables to complete. This way we can be sure that all
     * tasks added before this method was called are complete.
     */
    int maximumPoolSize = getMaximumPoolSize();
    CountDownLatch countDownLatch = new CountDownLatch(maximumPoolSize);
    for (int i = 0; i < maximumPoolSize; i++) {
      execute(new CountDownRunnable(countDownLatch));
    }
    countDownLatch.await();
    maybeThrowEncounteredErrors();
  }

  /**
   * Immediately throw an exception if any unrecoverable errors were encountered by any of the write
   * tasks.
   *
   * @throws BigQueryConnectException if any of the tasks failed.
   */
  public void maybeThrowEncounteredErrors() {
    if (encounteredErrors.size() > 0) {
      String errorString = createErrorString(encounteredErrors);
      throw new BigQueryConnectException("Some write threads encountered unrecoverable errors: "
          + errorString + "; See logs for more detail");
    }
  }

  private static String createErrorString(Collection<Throwable> errors) {
    return errors.stream()
                 .map(Objects::toString)
                 .collect(Collectors.joining(", "));
  }
}
