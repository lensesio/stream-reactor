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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A partitioner that divides elements into a series of partitions such that two conditions are
 * achieved: firstly, the largest partition and the smallest partition differ in length by no more
 * one row, and secondly, no partition shall be larger in size than an amount specified during
 * instantiation.
 */
public class EqualPartitioner<E> implements Partitioner<E> {
  private final int maxPartitionSize;

  /**
   * @param maxPartitionSize The maximum size of a partition returned by a call to partition().
   */
  public EqualPartitioner(int maxPartitionSize) {
    if (maxPartitionSize <= 0) {
      throw new IllegalArgumentException("Maximum size of partition must be a positive number");
    }
    this.maxPartitionSize = maxPartitionSize;
  }

  /**
   * Take a list of elements, and divide it up into a list of sub-lists, where each sub-list
   * contains at most <i>maxPartitionSize</i> elements.
   *
   * @param elements The list of elements to partition.
   * @return A list of lists of elements, each of which varies in length by a maximum of one from
   *         every other.
   */
  @Override
  public List<List<E>> partition(List<E> elements) {
    // Handle the case where no partitioning is necessary
    if (elements.size() <= maxPartitionSize) {
      return Collections.singletonList(elements);
    }

    // Ceiling division
    int numPartitions = (elements.size() + maxPartitionSize - 1) / maxPartitionSize;
    int minPartitionSize = elements.size() / numPartitions;

    List<List<E>> partitions = new ArrayList<>(numPartitions);

    // The beginning of the next partition, as an index within <rows>
    int partitionStart = 0;
    for (int partition = 0; partition < numPartitions; partition++) {
      // If every partition were given <minPartitionSize> rows, there would be exactly
      // <numRows> % <numPartitions> rows left over.
      // As a result, the first (<numRows> % <numPartitions>) partitions are given
      // (<minPartitionSize> + 1) rows, and the rest are given <minPartitionSize> rows.
      int partitionSize =
          partition < elements.size() % numPartitions ? minPartitionSize + 1 : minPartitionSize;

      // The end of the next partition, within <rows>
      // IndexOutOfBoundsExceptions shouldn't occur here, but just to make sure, guarantee that the
      // end of the partition isn't past the end of the array of rows.
      int partitionEnd = Math.min(partitionStart + partitionSize, elements.size());

      // Add the next partition to the return value
      partitions.add(elements.subList(partitionStart, partitionEnd));

      partitionStart += partitionSize;
    }
    return partitions;
  }
}
