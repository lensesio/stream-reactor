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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class ListSplitterTest {

  private final List<Integer> list = IntStream.range(1, 11).boxed().collect(Collectors.toList());

  @Test
  void testSplitListIntoEqualParts() {
    List<List<Integer>> result = ListSplitter.splitList(list, 5);
    assertEquals(5, result.size());
    for (List<Integer> sublist : result) {
      assertEquals(2, sublist.size());
    }
  }

  @Test
  void testSplitListWithRemainder() {
    List<List<Integer>> result = ListSplitter.splitList(list, 3);
    assertEquals(3, result.size());
    assertEquals(4, result.get(0).size());
    assertEquals(3, result.get(1).size());
    assertEquals(3, result.get(2).size());
  }

  @Test
  void testSplitListSinglePart() {
    List<List<Integer>> result = ListSplitter.splitList(list, 1);
    assertEquals(1, result.size());
    assertEquals(10, result.get(0).size());
  }

  @Test
  void testSplitListMorePartsThanElements() {
    List<List<Integer>> result = ListSplitter.splitList(list, 12);
    assertEquals(12, result.size());
    int nonEmptyLists = (int) result.stream().filter(sublist -> !sublist.isEmpty()).count();
    assertEquals(10, nonEmptyLists);
    for (List<Integer> sublist : result) {
      assertTrue(sublist.size() <= 1);
    }
  }

  @Test
  void testSplitEmptyList() {
    List<Integer> emptyList = Collections.emptyList();
    List<List<Integer>> result = ListSplitter.splitList(emptyList, 3);
    assertEquals(3, result.size());
    for (List<Integer> sublist : result) {
      assertTrue(sublist.isEmpty());
    }
  }

  @Test
  void testSplitListInvalidParts() {
    Executable executable = () -> ListSplitter.splitList(list, 0);
    assertThrows(IllegalArgumentException.class, executable);
  }
}
