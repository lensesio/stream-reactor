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
package io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * LooselyBoundedQueue is an extension of {@link ConcurrentLinkedQueue} that introduces a maximum size limit.
 * Unlike a strictly bounded queue, it does not fail the {@link #add(Object)} or {@link #addAll(Collection)} operations
 * when this limit is exceeded. Instead, it logs a debug message when the queue size exceeds the specified maximum size.
 *
 * <p>This class is designed to be used in multi-threaded environments where it is more important to avoid the failure
 * of add operations than to strictly enforce a queue size limit. In such scenarios, failing the connector due to queue
 * overflow is undesirable as it can lead to disruptions in the data processing pipeline. Hence, this loosely bounded
 * approach allows for some flexibility by permitting the queue size to exceed the maximum limit while still providing
 * an indication (through logging) that the limit has been breached.</p>
 *
 * @param <X> the type of elements held in this queue
 */
@Slf4j
public class LooselyBoundedQueue<X> extends ConcurrentLinkedQueue<X> {

  private final int maxSize;

  /**
   * Constructs a new LooselyBoundedQueue with the specified maximum size.
   *
   * @param maxSize the maximum size of the queue
   */
  public LooselyBoundedQueue(int maxSize) {
    this.maxSize = maxSize;
  }

  /**
   * Checks if there is space available in the queue for the specified number of elements.
   *
   * @param count the number of elements proposed to be added to the queue
   * @return true if there is spare capacity for these elements, false otherwise
   */
  public boolean hasSpareCapacity(int count) {
    val newSize = getNewSize(count);
    return (newSize <= maxSize);
  }

  @Override
  public boolean add(X x) {
    logIfNoSpareCapacity(1);
    return super.add(x);
  }

  @Override
  public boolean addAll(Collection<? extends X> c) {
    logIfNoSpareCapacity(c.size());
    return super.addAll(c);
  }

  /**
   * Logs a debug message if adding the specified number of elements would exceed the queue's maximum size.
   *
   * @param numberOfElements the number of elements to be added
   */
  private void logIfNoSpareCapacity(int numberOfElements) {
    final var newSize = getNewSize(numberOfElements);
    if (newSize > maxSize) {
      log.debug("Queue will be full - {}/{}", newSize, maxSize);
    }
  }

  /**
   * Calculates the new size of the queue if the specified number of elements were added.
   *
   * @param numberOfElements the number of elements to be added
   * @return the new size of the queue
   */
  private int getNewSize(int numberOfElements) {
    return super.size() + numberOfElements;
  }

}
