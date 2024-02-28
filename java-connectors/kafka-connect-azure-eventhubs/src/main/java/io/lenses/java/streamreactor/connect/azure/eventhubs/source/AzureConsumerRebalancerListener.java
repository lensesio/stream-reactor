package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import io.lenses.java.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureOffsetMarker;
import io.lenses.java.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureTopicPartitionKey;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * This class is an implementation of {@link ConsumerRebalanceListener} that can be used to provide
 * OnlyOnce support and seek consumers into relevant offsets if needed.
 */
@Slf4j
public class AzureConsumerRebalancerListener implements ConsumerRebalanceListener {

  Optional<TopicPartitionOffsetProvider> topicPartitionOffsetProvider;
  Consumer<?, ?> kafkaConsumer;

  public AzureConsumerRebalancerListener(
      Optional<TopicPartitionOffsetProvider> topicPartitionOffsetProvider,
      Consumer<?, ?> kafkaConsumer) {
    this.topicPartitionOffsetProvider = topicPartitionOffsetProvider;
    this.kafkaConsumer = kafkaConsumer;
  }

  public AzureConsumerRebalancerListener(Consumer<?, ?> kafkaConsumer) {
    this.topicPartitionOffsetProvider = TopicPartitionOffsetProvider.getInstance();
    this.kafkaConsumer = kafkaConsumer;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    // implementation not needed, offsets already committed
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    partitions.forEach(partition -> {

      if (topicPartitionOffsetProvider.isPresent()) {
        TopicPartitionOffsetProvider offsetProvider = topicPartitionOffsetProvider.get();
        AzureTopicPartitionKey partitionKey = new AzureTopicPartitionKey(
            partition.topic(), partition.partition());
        Optional<AzureOffsetMarker> partitionOffset = offsetProvider.getOffset(partitionKey);
        partitionOffset.ifPresentOrElse(
            offset -> kafkaConsumer.seek(partition, offset.getOffsetValue()),
            () -> kafkaConsumer.seekToBeginning(Collections.singletonList(partition)));
      } else {
        kafkaConsumer.seekToBeginning(Collections.singletonList(partition));
      }
      log.info("Subscribed to topic: {}", partition.topic());
    });
  }

}
