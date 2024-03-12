package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.SourceDataType.KeyValueTypes;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Interface of a class to produce BlockingQueueProducers.
 */
public interface ProducerProvider {

  BlockingQueueProducer createProducer(AzureEventHubsConfig azureEventHubsConfig,
      BlockingQueue<ConsumerRecords<Object, Object>> recordBlockingQueue,
      KeyValueTypes keyValueTypes);
}
