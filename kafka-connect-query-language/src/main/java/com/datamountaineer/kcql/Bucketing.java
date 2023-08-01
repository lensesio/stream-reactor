/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.kcql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Bucketing {
  private int bucketsNumber;
  private List<String> bucketNames = new ArrayList<String>();

  Bucketing(Collection<String> bucketNames) {
    if (bucketNames == null) {
      throw new IllegalArgumentException("Invalid bucketNames parameter");
    }
    for (final String bucketName : bucketNames) {
      if (bucketName == null || bucketName.trim().length() == 0) {
        throw new IllegalArgumentException("Iter parameter contains either null or empty value");
      }
      this.bucketNames.add(bucketName);
    }
  }

  public void addBucketName(final String bucketName) {
    if (bucketName == null || bucketName.trim().length() == 0) {
      throw new IllegalArgumentException("Iter parameter contains either null or empty value");
    }
    bucketNames.add(bucketName);
  }

  public Iterator<String> getBucketNames() {
    return bucketNames.iterator();
  }

  public int getBucketsNumber() {
    return bucketsNumber;
  }

  public void setBucketsNumber(int bucketsNumber) {
    this.bucketsNumber = bucketsNumber;
  }
}
