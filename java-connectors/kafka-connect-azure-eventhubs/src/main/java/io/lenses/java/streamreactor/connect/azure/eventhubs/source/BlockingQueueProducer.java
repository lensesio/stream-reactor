package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import java.time.Duration;

/**
 * Interface of stoppable BockingQueue Producer
 */
public interface BlockingQueueProducer {

  /**
   * Method to start production to specified BlockingQueue.
   */
  void start();

  /**
   * Method to stop production to specified BlockingQueue.
   *
   * @param timeoutDuration maximum time to stop the Producer
   */
  void stop(Duration timeoutDuration);

}
