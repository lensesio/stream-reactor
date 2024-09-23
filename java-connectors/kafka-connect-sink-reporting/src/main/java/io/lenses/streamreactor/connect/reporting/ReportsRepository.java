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
import io.lenses.streamreactor.connect.reporting.model.SinkRecordRecordReport;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * TODO: see whether this class is needed.
 */
public class ReportsRepository {

  private static final int DEFAULT_QUEUES_SIZE = 1000;
  ArrayBlockingQueue<SinkRecordRecordReport> newReports;
  ArrayBlockingQueue<SinkRecordRecordReport> reportsToSend;

  public ReportsRepository(Integer recordsQueueSize) {
    int finalQueueSize = Objects.requireNonNullElse(recordsQueueSize, DEFAULT_QUEUES_SIZE);
    newReports = new ArrayBlockingQueue<>(finalQueueSize);
    reportsToSend = new ArrayBlockingQueue<>(finalQueueSize * 2);
  }

  public void updateRecordsStatus(Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
    Map<String, Map<Integer, Long>> topicPartitionOffsetMap =
        extractTopicPartitionOffsetMap(committedOffsets);

    List<SinkRecordRecordReport> toMove = new ArrayList<>(newReports.size());

    newReports.forEach(rr -> {
      SinkRecord sinkRecord = rr.getOriginalRecord();

      Long safeOffset =
          Optional.ofNullable(topicPartitionOffsetMap.get(sinkRecord.originalTopic()))
              .map(topicsMap -> topicsMap.get(sinkRecord.originalKafkaPartition())).orElse(0L);

      if (safeOffset > sinkRecord.originalKafkaOffset()) {
        toMove.add(rr);
      }
    });

    newReports.removeAll(toMove);

    reportsToSend.forEach(rr -> Try.withCatch(() -> reportsToSend.offer(rr, 1L, TimeUnit.SECONDS),
        InterruptedException.class)
        .forEachFailed(ie -> {
          throw new ReportingException("Interrupted Exception Happened during update", ie);
        }));
  }

  private Map<String, Map<Integer, Long>> extractTopicPartitionOffsetMap(
      Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
    Map<String, Map<Integer, Long>> topicPartitionOffsetMap = new HashMap<>();
    committedOffsets.forEach((tp, om) -> {
      Map<Integer, Long> topicMap = topicPartitionOffsetMap.getOrDefault(tp.topic(), new HashMap<>());
      Long topicPartitionOffset = topicMap.getOrDefault(tp.partition(), om.offset());
      topicMap.put(tp.partition(), topicPartitionOffset);
      topicPartitionOffsetMap.putIfAbsent(tp.topic(), topicMap);
    });

    return topicPartitionOffsetMap;
  }

  public Collection<SinkRecordRecordReport> drainReportsToSend(int batchSize) {
    ArrayList<SinkRecordRecordReport> reportsToReturn = new ArrayList<>(batchSize);
    this.reportsToSend.drainTo(reportsToReturn, batchSize);

    return reportsToReturn;
  }

}
