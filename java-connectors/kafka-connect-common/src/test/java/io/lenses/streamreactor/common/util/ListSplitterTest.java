/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class ListSplitterTest {

  private final List<Integer> list = IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList());

  @Test
  void testSplitListIntoEqualParts() {
    List<List<Integer>> result = ListSplitter.splitList(list, 5);
    assertThat(result)
        .hasSize(5)
        .allMatch(sublist -> sublist.size() == 2);
  }

  @Test
  void testSplitListWithRemainder() {
    List<List<Integer>> result = ListSplitter.splitList(list, 3);
    assertThat(result)
        .hasSize(3)
        .containsAll(List.of(
            List.of(1, 2, 3, 4),
            List.of(5, 6, 7),
            List.of(8, 9, 10)
        )
        );
  }

  @Test
  void testSplitListSinglePart() {
    List<List<Integer>> result = ListSplitter.splitList(list, 1);
    assertThat(result)
        .hasSize(1)
        .containsExactly(list);
  }

  @Test
  void testSplitListMorePartsThanElements() {
    List<List<Integer>> result = ListSplitter.splitList(list, 12);
    assertThat(result)
        .hasSize(10)
        .doesNotContain(Collections.emptyList())
        .allMatch(sublist -> sublist.size() == 1);
  }

  @Test
  void testSplitEmptyList() {
    List<Integer> emptyList = Collections.emptyList();
    List<List<Integer>> result = ListSplitter.splitList(emptyList, 3);
    assertThat(result).isEmpty();
  }

  @Test
  void testSplitListInvalidParts() {
    Executable executable = () -> ListSplitter.splitList(list, 0);
    assertThrows(IllegalArgumentException.class, executable);
  }

  @Test
  void testListSmallerThanMaxNShouldProvideMaxNResults() {
    List<List<Integer>> result = ListSplitter.splitList(Collections.singletonList(1), 100);
    assertThat(result).hasSize(1).allMatch(l -> l.size() == 1);
  }
}
