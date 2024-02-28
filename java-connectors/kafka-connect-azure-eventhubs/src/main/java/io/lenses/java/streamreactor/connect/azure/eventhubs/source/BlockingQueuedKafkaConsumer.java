package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Abstraction over Kafka {@link Consumer} class that wraps the Consumer into a thread and allows
 * it to output its records into a {@link BlockingQueue} shared with {@link EventHubsKafkaConsumerController}.
 */
@Slf4j
public class BlockingQueuedKafkaConsumer {

  private final BlockingQueue<ConsumerRecords<String, String>> recordsQueue;
  private final Consumer<String, String> consumer;
  private final String clientId;
  private boolean isPolling = false;

  /**
   * Class is a proxy that allows access to some methods of Kafka Consumer. It's main purpose is
   * to create a thread around the consumer and put consumer record into BlockingQueue. That is
   * where EventHubsKafkaConsumerController takes over the handling
   *
   * @param recordsQueue BlockingQueue to put records into
   * @param consumer Kafka Consumer
   * @param clientId consumer client id
   */
  public BlockingQueuedKafkaConsumer(BlockingQueue<ConsumerRecords<String, String>> recordsQueue,
      Consumer<String, String> consumer, String clientId) {
    this.recordsQueue = recordsQueue;
    this.consumer = consumer;
    this.clientId = clientId;
  }

  void startPolling(Duration pollDuration) {
    if (!isPolling) {
      Thread pollingThread = new Thread(new EventhubsPollingRunnable(pollDuration));

      pollingThread.start();
      isPolling = true;
    }
  }

  public void subscribe(List<String> strings) {
    consumer.subscribe(strings, new AzureConsumerRebalancerListener(consumer));
  }

  public void close(Duration timeoutDuration) {
    consumer.close(timeoutDuration);
  }

  private class EventhubsPollingRunnable implements Runnable {
    private final Duration pollDuration;

    public EventhubsPollingRunnable(Duration pollDuration) {
      this.pollDuration = pollDuration;
    }

    @Override
    public void run() {
      while (true) {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(pollDuration);
        if (consumerRecords != null && !consumerRecords.isEmpty()) {
          try {
            boolean offer = false;
            while (!offer) {
              offer = recordsQueue.offer(consumerRecords, 5, TimeUnit.SECONDS);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Kafka Consumer with clientId={} has been interrupted on offering", clientId);
          }
        }
      }
    }
  }
}
