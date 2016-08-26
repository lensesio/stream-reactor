package com.wepay.kafka.connect.bigquery.buffer;

/*
 * Copyright 2016 WePay, Inc.
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


import java.util.List;

/**
 * A Buffer that can only buffer one collection at a time. If multiple calls to buffer() are made
 * without a call to either getAll() or getExcess() in between, an exception is thrown. The
 * advantage is that the only actual data that's ever stored/buffered is a single reference to
 * another already-created List.
 */
public class EmptyBuffer<E> implements Buffer<E> {
  private List<E> buffer = null;

  @Override
  public boolean hasAny() {
    return buffer != null && !buffer.isEmpty();
  }

  @Override
  public boolean hasExcess() {
    return hasAny();
  }

  @Override
  public List<E> getAll() {
    if (!hasAny()) {
      throw new IllegalStateException("getAll() invoked with empty buffer");
    }
    List<E> result = buffer;
    buffer = null;
    return result;
  }

  @Override
  public List<E> getExcess() {
    if (!hasExcess()) {
      throw new IllegalStateException("getExcess() invoked with empty buffer");
    }
    return getAll();
  }

  @Override
  public void buffer(List<E> elements) {
    if (hasExcess()) {
      throw new IllegalStateException("buffer() invoked with full buffer");
    }
    buffer = elements;
  }
}
