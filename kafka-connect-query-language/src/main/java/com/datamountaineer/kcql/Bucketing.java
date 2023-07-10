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
