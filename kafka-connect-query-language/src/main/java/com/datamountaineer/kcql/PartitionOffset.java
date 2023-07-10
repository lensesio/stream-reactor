package com.datamountaineer.kcql;


public class PartitionOffset {
  private final int partition;
  private final Long offset;

  public PartitionOffset(int partition) {
    this(partition, null);
  }

  public PartitionOffset(int partition, Long offset) {
    if(partition < 0){
      throw new IllegalArgumentException(String.format("partition is not valid:<%d>",  partition));
    }
    this.partition = partition;
    this.offset = offset;
  }


  public int getPartition() {
    return partition;
  }

  public Long getOffset() {
    return offset;
  }
}
