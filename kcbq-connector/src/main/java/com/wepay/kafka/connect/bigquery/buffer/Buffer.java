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
 * A generic buffer class.
 * @param <E> The type of element to be buffered.
 */
public interface Buffer<E> {

  /**
   * @return Whether the buffer currently contains any elements.
   */
  boolean hasAny();

  /**
   * @return Whether the buffer currently contains any excess elements (if true, writes to the
   *     buffer should cease until they are collected).
   */
  boolean hasExcess();

  /**
   * @return All elements currently buffered. May throw an exception if no elements are currently
   *     buffered, so a call to hasAny() is recommended first.
   */
  List<E> getAll();

  /**
   * @return All excess elements currently in the buffer. May throw an exception if no excess
   *     elements are currently buffered, so a call to hasAny() is recommended first.
   */
  List<E> getExcess();

  /**
   * Insert the given elements into the buffer. May throw an exception if excess elements are
   * currently buffered, so a call to hasExcess() is recommended first.
   * @param elements The elements to insert.
   */
  void buffer(List<E> elements);
}
