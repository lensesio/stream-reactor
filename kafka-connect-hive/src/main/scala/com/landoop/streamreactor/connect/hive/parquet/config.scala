package com.landoop.streamreactor.connect.hive.parquet

import org.apache.parquet.hadoop.metadata.CompressionCodecName

case class ParquetSourceConfig(projection: Seq[String] = Nil, dictionaryFiltering: Boolean = true)

case class ParquetSinkConfig(
  overwrite:        Boolean              = false,
  compressionCodec: CompressionCodecName = CompressionCodecName.SNAPPY,
  validation:       Boolean              = true,
  enableDictionary: Boolean              = true,
)
