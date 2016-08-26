package com.wepay.kafka.connect.bigquery.partition;

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


import java.util.Collections;
import java.util.List;

/**
 * A partitioner that doesn't divide its list at all.
 */
public class SinglePartitioner<E> implements Partitioner<E> {
  /**
   * @param elements The list of elements to partition.
   * @return A list containing the argument list.
   */
  @Override
  public List<List<E>> partition(List<E> elements) {
    return Collections.singletonList(elements);
  }
}
