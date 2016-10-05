package com.wepay.kafka.connect.bigquery.write.batch;

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


import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;

import org.junit.Assert;
import org.junit.Test;

import org.mockito.ArgumentMatcher;

import java.util.ArrayList;
import java.util.List;

public class DynamicBatchWriterTest {

  @Test
  @SuppressWarnings("unchecked")
  public void simpleTest() throws InterruptedException {
    // the starting size is exactly right.
    final int actualMaxSize = 1000;

    BigQueryWriter mockWriter = getMockWithMaxSize(actualMaxSize);

    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter();
    dynamicBatchWriter.init(mockWriter);

    writeAll(dynamicBatchWriter, actualMaxSize);
    Assert.assertEquals(1000, dynamicBatchWriter.getCurrentBatchSize());
    Assert.assertFalse(dynamicBatchWriter.isSeeking());

    writeAll(dynamicBatchWriter, 1200);
    verify(mockWriter, times(2)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(600)),
                                           anyObject(),
                                           anyObject());
  }

  private static final int TEST_DEFAULT_STARTING_SIZE = 500;

  @Test
  @SuppressWarnings("unchecked")
  public void increaseBatchSizeTest() throws InterruptedException {
    // the starting size is too small
    final int actualMaxSize = 1200;
    // the actual configured maxSize should end up being 1000,
    // even though we allow up to 1200 size batches here

    BigQueryWriter mockWriter = getMockWithMaxSize(actualMaxSize);

    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter,
                                                                   TEST_DEFAULT_STARTING_SIZE,
                                                                   true);

    writeAll(dynamicBatchWriter, 3500);
    // expected calls are:
    // 500 (success)
    // 1000 (success)
    // 2000 (failure)
    // 1000 (success)
    // 1000 (success)
    Assert.assertEquals(1000, dynamicBatchWriter.getCurrentBatchSize());
    Assert.assertFalse(dynamicBatchWriter.isSeeking());

    writeAll(dynamicBatchWriter, 1600);
    verify(mockWriter, times(2)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(800)),
                                           anyObject(),
                                           anyObject());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void decreaseBatchSizeTest() throws InterruptedException {
    // the starting size is too large
    final int actualMaxSize = 300;
    // the actual configured maxSize should end up being 250,
    // even though we allow up to 300 size batches here.

    BigQueryWriter mockWriter = getMockWithMaxSize(actualMaxSize);

    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter,
                                                                   TEST_DEFAULT_STARTING_SIZE,
                                                                   true);

    writeAll(dynamicBatchWriter, 750);
    // expected calls are:
    // 500 (failure)
    // 250 (success)
    // 500 (failure)
    // 250 (success)
    // 250 (success)
    Assert.assertEquals(250, dynamicBatchWriter.getCurrentBatchSize());
    Assert.assertFalse(dynamicBatchWriter.isSeeking());

    writeAll(dynamicBatchWriter, 400);
    verify(mockWriter, times(2)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(200)),
                                           anyObject(),
                                           anyObject());
  }

  @Test
  public void fullBatchSizeTest() throws InterruptedException {
    // size element list sent to writeAlls is smaller than batchSize
    BigQueryWriter mockWriter = getMockWithMaxSize(500);

    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter,
                                                                   TEST_DEFAULT_STARTING_SIZE,
                                                                   true);

    writeAll(dynamicBatchWriter, 400);

    Assert.assertEquals(500, dynamicBatchWriter.getCurrentBatchSize());
    Assert.assertFalse(dynamicBatchWriter.isSeeking());
  }

  @Test
  public void maxBatchSizeTest() throws InterruptedException {
    // somehow we are able to send up to 100,000 rows in a single request
    BigQueryWriter mockWriter = getMockWithMaxSize(200000);

    // shortcut: start batch size at 32,000 so we don't need to do a ton before we end up at 100k
    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter, 32000, true);

    writeAll(dynamicBatchWriter, 200000);

    Assert.assertEquals(100000, dynamicBatchWriter.getCurrentBatchSize());
    Assert.assertFalse(dynamicBatchWriter.isSeeking());
  }


  @Test
  @SuppressWarnings("unchecked")
  public void establishedFailure() throws InterruptedException {
    // test a failure during an establishedWriteAll
    // we error at anything above 300...
    BigQueryWriter mockWriter = getMockWithMaxSize(300);
    // but our dynamic batch writer is established at 500.
    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter,
                                                                   TEST_DEFAULT_STARTING_SIZE,
                                                                   false);

    writeAll(dynamicBatchWriter, 500);
    // expected calls are:
    // 500 (failure)
    // 250 (success)
    // 250 (success)
    verify(mockWriter, times(1)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(500)),
                                           anyObject(),
                                           anyObject());
    verify(mockWriter, times(2)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(250)),
                                           anyObject(),
                                           anyObject());
    Assert.assertEquals(250, dynamicBatchWriter.getCurrentBatchSize());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void establishedSuccesses() throws InterruptedException {
    // test a failure during an establishedWriteAll
    // we will only error at above 1100...
    BigQueryWriter mockWriter = getMockWithMaxSize(1100);
    // but we are established at 500.
    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter,
                                                                   TEST_DEFAULT_STARTING_SIZE,
                                                                   false);

    // 10 calls before batchSize increase
    for (int i = 0; i < 10; i++) {
      writeAll(dynamicBatchWriter, 1200);
    }
    verify(mockWriter, times(30)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(400)),
                                           anyObject(),
                                           anyObject());
    // verify we got up to 100 batch size
    Assert.assertEquals(1000, dynamicBatchWriter.getCurrentBatchSize());

    // actually write at 1000 batch size
    writeAll(dynamicBatchWriter, 1200);
    verify(mockWriter, times(2)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(600)),
                                           anyObject(),
                                           anyObject());
    // verify batch size hasn't changed
    Assert.assertEquals(1000, dynamicBatchWriter.getCurrentBatchSize());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void establishedUnevenBatches() throws InterruptedException {
    // uneven batches work as expected.
    BigQueryWriter mockWriter = getMockWithMaxSize(500);

    // start by establishing at 500, no seeking.
    DynamicBatchWriter dynamicBatchWriter = new DynamicBatchWriter(mockWriter,
                                                                   TEST_DEFAULT_STARTING_SIZE,
                                                                   false);

    writeAll(dynamicBatchWriter, 601);
    // expect batch sizes of 300, 301
    verify(mockWriter, times(1)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(300)),
                                                   anyObject(),
                                                   anyObject());
    verify(mockWriter, times(1)).writeRows(anyObject(),
                                           argThat(new ListIsExactly(301)),
                                                   anyObject(),
                                                   anyObject());
  }

  /**
   * Create and return a mock {@link BigQueryWriter} that will throw a batch size error for any
   * request that contains more than maxSize elements.
   * @param maxSize the maximum allowable size.
   */
  @SuppressWarnings("unchecked")
  private BigQueryWriter getMockWithMaxSize(int maxSize) throws InterruptedException {
    BigQueryWriter mockWriter = mock(BigQueryWriter.class);

    doThrow(new BigQueryException(400, null)).when(mockWriter)
        .writeRows(anyObject(),
                   argThat(new ListIsAtLeast(maxSize + 1)),
                   anyObject(),
                   anyObject());

    return mockWriter;
  }

  /**
   * Call writeAll with the given number of "elements".
   * @param dynamicBatchWriter the {@link DynamicBatchWriter}to use.
   * @param numElements the number of "elements" to "write"
   */
  private void writeAll(DynamicBatchWriter dynamicBatchWriter, int numElements)
      throws InterruptedException {
    TableId tableId = TableId.of("test_dataset", "test_table");
    List<InsertAllRequest.RowToInsert> elements = new ArrayList<>();
    for (int i = 0; i < numElements; i++) {
      elements.add(null);
    }
    dynamicBatchWriter.writeAll(tableId, elements, null, null);
  }

  private static class ListIsAtLeast extends ArgumentMatcher<List> {
    private int size;

    ListIsAtLeast(int size) {
      this.size = size;
    }

    @Override
    public boolean matches(Object argument) {
      if (argument instanceof List) {
        List list = (List) argument;
        return list.size() >= this.size;
      }
      return false;
    }
  }

  private static class ListIsExactly extends ArgumentMatcher<List> {
    private int size;

    ListIsExactly(int size) {
      this.size = size;
    }

    @Override
    public boolean matches(Object argument) {
      if (argument instanceof List) {
        List list = (List) argument;
        return list.size() == this.size;
      }
      return false;
    }
  }
}
