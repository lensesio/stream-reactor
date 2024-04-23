/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.azure.eventhubs.source;

import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsSourceConfig;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Interface of a class to produce BlockingQueueProducers.
 */
public interface ProducerProvider<K, V> {

  BlockingQueueProducer createProducer(AzureEventHubsSourceConfig azureEventHubsSourceConfig,
      BlockingQueue<ConsumerRecords<byte[], byte[]>> recordBlockingQueue,
      Map<String, String> inputToOutputTopics);
}
