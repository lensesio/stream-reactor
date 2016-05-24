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
 * A Buffer that continually stores records until retrieved via getAll().
 */
public class UnlimitedBuffer<E> implements Buffer<E> {
  private List<E> buffer;

  public UnlimitedBuffer() {
    buffer = new LinkedList<>();
  }

  @Override
  public boolean hasAny() {
    return !buffer.isEmpty();
  }

  @Override
  public boolean hasExcess() {
    return false;
  }

  @Override
  public List<E> getAll() {
    List<E> result = buffer;
    buffer = new LinkedList<>();
    return result;
  }

  @Override
  public List<E> getExcess() {
    throw new UnsupportedOperationException("Cannot invoke getExcess() on unlimited buffer");
  }

  @Override
  public void buffer(List<E> elements) {
    buffer.addAll(elements);
  }
}
