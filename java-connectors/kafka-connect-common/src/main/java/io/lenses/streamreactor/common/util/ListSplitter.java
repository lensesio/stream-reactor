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
package io.lenses.streamreactor.common.util;

import lombok.experimental.UtilityClass;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@UtilityClass
public class ListSplitter {

  /**
   * Splits the given list into {@code maxN} sublists of roughly equal size.
   * If the list cannot be divided evenly, the remaining elements are distributed
   * among the sublists so that the size difference between any two sublists is at most 1.
   *
   * @param list the list to be split
   * @param maxN the number of sublists to create
   * @param <T>  the type of elements in the list
   * @return a list of sublists, where each sublist contains a portion of the original list
   * @throws IllegalArgumentException if {@code maxN} is less than or equal to 0
   */
  public static <T> List<List<T>> splitList(List<T> list, int maxN) {
    if (maxN <= 0) {
      throw new IllegalArgumentException("Number of parts must be greater than zero.");
    }

    int totalSize = list.size();
    int partSize = totalSize / maxN;
    int remainder = totalSize % maxN;

    return IntStream.range(0, maxN)
        .mapToObj(i -> {
          int start = i * partSize + Math.min(i, remainder);
          int end = start + partSize + (i < remainder ? 1 : 0);
          return list.subList(start, Math.min(end, totalSize));
        })
        .filter(sublist -> !sublist.isEmpty())
        .collect(Collectors.toList());
  }
}
