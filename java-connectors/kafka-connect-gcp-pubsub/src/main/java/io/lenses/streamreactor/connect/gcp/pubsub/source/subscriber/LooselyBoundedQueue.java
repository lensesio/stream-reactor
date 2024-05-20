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

import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

/**
 * A LooselyBoundedQueue is a wrapper around a standard {@link Queue} implementation
 * that introduces a maximum size limit. However, unlike a strictly bounded queue,
 * it does not fail the {@link #add(Object)} or {@link #addAll(Collection)} operations
 * when this limit is exceeded. Instead, it logs a debug message when the queue size
 * exceeds the specified maximum size.
 *
 * <p>This class is designed to be used in multi-threaded environments where it is
 * more important to avoid the failure of add operations than to strictly enforce
 * a queue size limit. In such scenarios, failing the connector due to queue overflow
 * is undesirable as it can lead to disruptions in the data processing pipeline.
 * Hence, this loosely bounded approach allows for some flexibility by permitting
 * the queue size to exceed the maximum limit while still providing an indication
 * (through logging) that the limit has been breached.</p>
 *
 * @param <X> the type of elements held in this queue
 */
@Slf4j
public class LooselyBoundedQueue<X> implements Queue<X> {

  private final Queue<X> queue;
  private final int maxSize;

  /**
   * Constructs a new LooselyBoundedQueue with the specified backing queue and maximum size.
   *
   * @param queue   the backing queue
   * @param maxSize the maximum size of the queue
   */
  public LooselyBoundedQueue(Queue<X> queue, int maxSize) {
    this.queue = queue;
    this.maxSize = maxSize;
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return queue.contains(o);
  }

  @Override
  public Iterator<X> iterator() {
    return queue.iterator();
  }

  @Override
  public Object[] toArray() {
    return queue.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return queue.toArray(a);
  }

  @Override
  public boolean add(X x) {
    logIfNoSpareCapacity(1);
    return queue.add(x);
  }

  @Override
  public boolean addAll(Collection<? extends X> c) {
    logIfNoSpareCapacity(c.size());
    return queue.addAll(c);
  }

  @Override
  public boolean remove(Object o) {
    return queue.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return queue.containsAll(c);
  }

  public boolean hasSpareCapacity(int count) {
    val newSize = getNewSize(count);
    return (newSize <= maxSize);
  }

  private void logIfNoSpareCapacity(int numberOfElements) {
    final var newSize = getNewSize(numberOfElements);
    if (newSize > maxSize) {
      log.debug("Queue will be full - {}/{}", newSize, maxSize);
    }
  }

  private int getNewSize(int numberOfElements) {
    return queue.size() + numberOfElements;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return queue.removeAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return queue.retainAll(c);
  }

  @Override
  public void clear() {
    queue.clear();
  }

  @Override
  public boolean offer(X x) {
    return queue.offer(x);
  }

  @Override
  public X remove() {
    return queue.remove();
  }

  @Override
  public X poll() {
    return queue.poll();
  }

  @Override
  public X element() {
    return queue.element();
  }

  @Override
  public X peek() {
    return queue.peek();
  }

}
