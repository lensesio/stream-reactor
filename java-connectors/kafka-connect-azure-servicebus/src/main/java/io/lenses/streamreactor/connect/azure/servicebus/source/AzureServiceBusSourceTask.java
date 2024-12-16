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
package io.lenses.streamreactor.connect.azure.servicebus.source;

import static io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow;

import cyclops.control.Option;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusConfigConstants;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusSourceConfig;
import io.lenses.streamreactor.connect.azure.servicebus.util.KcqlConfigBusMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/**
 * Implementation of {@link SourceTask} for Microsoft Azure EventHubs.
 */
@Slf4j
public class AzureServiceBusSourceTask extends SourceTask {

  private final JarManifest jarManifest;
  private TaskToReceiverBridge taskToReceiverBridge;
  private final AtomicReference<Throwable> serviceBusReceiverError = new AtomicReference<>();
  private long sleepOnEmptyPoll = 100;

  public AzureServiceBusSourceTask() {
    this.jarManifest = unpackOrThrow(JarManifest.produceFromClass(getClass()));
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    final AzureServiceBusSourceConfig config = new AzureServiceBusSourceConfig(props);
    int recordsQueueSize = config.getInt(AzureServiceBusConfigConstants.TASK_RECORDS_QUEUE_SIZE);
    if (recordsQueueSize < 1) {
      throw new ConfigException("Records queue size must be greater than 0");
    }
    sleepOnEmptyPoll = config.getLong(AzureServiceBusConfigConstants.SOURCE_SLEEP_ON_EMPTY_POLL_MS);
    if (sleepOnEmptyPoll < 0) {
      throw new ConfigException("Sleep on empty poll must be greater than or equal to 0");
    }
    String connectionString = config.getPassword(AzureServiceBusConfigConstants.CONNECTION_STRING).value();
    List<Kcql> kcqls =
        KcqlConfigBusMapper.mapKcqlsFromConfig(props.get(AzureServiceBusConfigConstants.KCQL_CONFIG), true)
            .fold(ex -> {
              throw ex;
            }, Function.identity());

    final ArrayBlockingQueue<ServiceBusMessageHolder> recordsQueue =
        new ArrayBlockingQueue<>(recordsQueueSize);

    final int prefetchCount = config.getInt(AzureServiceBusConfigConstants.SOURCE_PREFETCH_COUNT);
    if (prefetchCount < 1) {
      throw new ConfigException("Prefetch count must be greater than 0");
    }
    final int maxCompleteRetries = config.getInt(AzureServiceBusConfigConstants.SOURCE_MAX_COMPLETE_RETRIES);
    if (maxCompleteRetries < 1) {
      throw new ConfigException("Max complete retries must be greater than 0");
    }
    final int completeMinFirstRetryBackoff =
        config.getInt(AzureServiceBusConfigConstants.SOURCE_MIN_BACKOFF_COMPLETE_RETRIES_MS);
    if (completeMinFirstRetryBackoff < 0) {
      throw new ConfigException("Min backoff complete retries must be greater than or equal to 0");
    }
    final Map<String, ServiceBusReceiverFacade> receiversMap =
        ServiceBusReceiverFacadeInitializer.initializeReceiverFacades(recordsQueue, kcqls, connectionString,
            prefetchCount, maxCompleteRetries, Duration.ofMillis(completeMinFirstRetryBackoff),
            serviceBusReceiverError);

    TaskToReceiverBridge serviceBusReceiverBridge =
        new TaskToReceiverBridge(recordsQueue, receiversMap);

    initialize(serviceBusReceiverBridge);
  }

  void initialize(TaskToReceiverBridge taskToReceiverBridge) {
    this.taskToReceiverBridge = taskToReceiverBridge;
  }

  @Override
  public List<SourceRecord> poll() {
    Option.ofNullable(serviceBusReceiverError.get())
        .peek(e -> {
          throw new ConnectException("Error in Service Bus Receiver", e);
        });
    List<SourceRecord> poll =
        taskToReceiverBridge.poll();
    if (poll.isEmpty()) {
      try {
        Thread.sleep(sleepOnEmptyPoll);
      } catch (InterruptedException e) {
        log.trace("Error while sleeping between polling of records", e);
      }
      return null;
    }
    log.debug("Polled {} records", poll.size());
    return poll;
  }

  @Override
  public void commitRecord(SourceRecord committedRecord, RecordMetadata metadata) {
    taskToReceiverBridge.commitRecordInServiceBus(committedRecord, metadata);
  }

  @Override
  public void stop() {
    log.info("Stopping {}", getClass().getSimpleName());
    taskToReceiverBridge.closeReceivers();
  }
}
