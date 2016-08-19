package com.datamountaineer.streamreactor.socketstreamer.domain

import java.util.UUID

import com.datamountaineer.connector.config.{Config, PartitionOffset => KQLPartitionOffset}
import com.datamountaineer.streamreactor.socketstreamer.avro.{FieldsValuesExtractor, GenericRecordFieldsValuesExtractor}

import scala.collection.JavaConversions._

case class KafkaStreamingProps(topic: String,
                               group: String,
                               partitionOffset: Seq[PartitionOffset],
                               fieldsValuesExtractor: FieldsValuesExtractor)

object KafkaStreamingProps {
  def apply(query: String): KafkaStreamingProps = {
    val config = Config.parse(query)

    val extractor = GenericRecordFieldsValuesExtractor(config.isIncludeAllFields,
      config.getFieldAlias.map(fa => fa.getField.toUpperCase -> fa.getAlias).toMap)

    val kqlPartitionOffset = config.getPartitonOffset
    val partitionOffsets = if (kqlPartitionOffset == null) {
      Seq.empty[PartitionOffset]
    } else {
      kqlPartitionOffset.map { p => PartitionOffset(p.getPartition, Option(p.getOffset)) }
    }


    new KafkaStreamingProps(config.getSource,
      Option(config.getConsumerGroup).getOrElse(UUID.randomUUID().toString),
      partitionOffsets,
      extractor
    )
  }
}