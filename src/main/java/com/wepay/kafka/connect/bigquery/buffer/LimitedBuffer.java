package com.wepay.kafka.connect.bigquery.buffer;

/*
 * Copyright 2016 Wepay, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.util.LinkedList;
import java.util.List;

/**
 * A Buffer that holds a configurable maximum amount of elements. Once full, can buffer at most one
 * more list before throwing an exception.
 */
public class LimitedBuffer<E> implements Buffer<E> {
  private List<E> buffer;
  private List<E> excess;
  private final long maxBufferSize;

  /**
   * @param maxBufferSize The maximum number of elements to buffer at once before storing excess.
   */
  public LimitedBuffer(long maxBufferSize) {
    if (maxBufferSize <= 0) {
      throw new IllegalArgumentException("maxBufferSize must be a positive number");
    }
    buffer = new LinkedList<>();
    excess = null;
    this.maxBufferSize = maxBufferSize;
  }

  @Override
  public boolean hasAny() {
    return !buffer.isEmpty() || hasExcess();
  }

  @Override
  public boolean hasExcess() {
    return excess != null;
  }

  @Override
  public List<E> getAll() {
    List<E> result = buffer;
    buffer = new LinkedList<>();
    if (hasExcess()) {
      result.addAll(getExcess());
    }
    return result;
  }

  @Override
  public List<E> getExcess() {
    if (!hasExcess()) {
      throw new IllegalStateException("getExcess() invoked with no excess stored");
    }
    List<E> result = excess;
    excess = null;
    return result;
  }

  @Override
  public void buffer(List<E> elements) {
    if (hasExcess()) {
      throw new IllegalStateException("buffer() invoked with full buffer");
    }
    if (elements.size() + buffer.size() > maxBufferSize) {
      excess = elements;
    } else {
      buffer.addAll(elements);
    }
  }
}
