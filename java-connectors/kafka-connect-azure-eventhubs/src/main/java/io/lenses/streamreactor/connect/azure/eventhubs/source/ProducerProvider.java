package io.lenses.streamreactor.connect.azure.eventhubs.source;

import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsSourceConfig;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Interface of a class to produce BlockingQueueProducers.
 */
public interface ProducerProvider<K, V> {

  BlockingQueueProducer createProducer(AzureEventHubsSourceConfig azureEventHubsSourceConfig,
      BlockingQueue<ConsumerRecords<K, V>> recordBlockingQueue);
}
