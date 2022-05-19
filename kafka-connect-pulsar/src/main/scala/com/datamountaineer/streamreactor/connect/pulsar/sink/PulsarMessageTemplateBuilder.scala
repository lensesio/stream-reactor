package com.datamountaineer.streamreactor.connect.pulsar.sink

import com.datamountaineer.kcql.Field
import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.common.converters.FieldConverter
import com.datamountaineer.streamreactor.common.converters.ToJsonWithProjections
import com.datamountaineer.streamreactor.common.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.pulsar.config.PulsarSinkSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

import scala.annotation.nowarn
import scala.jdk.CollectionConverters.ListHasAsScala

case class PulsarMessageTemplateBuilder(settings: PulsarSinkSettings) extends StrictLogging with ErrorHandler {

  private val mappings: Map[String, Set[Kcql]] = settings.kcql.groupBy(k => k.getSource)

  @nowarn
  def extractMessageKey(record: SinkRecord, k: Kcql): Option[String] =
    if (k.getWithKeys != null && k.getWithKeys().size() > 0) {
      val parentFields = null

      // Get the fields to construct the key for pulsar
      val (partitionBy, schema, value) = if (k.getWithKeys != null && k.getWithKeys().size() > 0) {
        (k.getWithKeys.asScala.map(f => Field.from(f, f, parentFields)),
         if (record.key() != null) record.keySchema() else record.valueSchema(),
         if (record.key() != null) record.key() else record.value(),
        )
      } else {
        (Seq(Field.from("*", "*", parentFields)),
         if (record.key() != null) record.keySchema() else record.valueSchema(),
         if (record.key() != null) record.key() else record.value(),
        )
      }

      val keyFields = partitionBy.map(FieldConverter.apply)

      val jsonKey = ToJsonWithProjections(
        keyFields.toSeq,
        List.empty[Field].map(FieldConverter.apply),
        schema,
        value,
        k.hasRetainStructure,
      )

      Some(jsonKey.toString)
    } else {
      None
    }

  @nowarn
  def create(records: Iterable[SinkRecord]): Iterable[MessageTemplate] =
    // in KCQL
    records.flatMap { record =>
      getKcqlStatementsForTopic(record).map {
        k =>
          val pulsarTopic = k.getTarget

          //optimise this via a map
          val fields        = k.getFields.asScala.map(FieldConverter.apply)
          val ignoredFields = k.getIgnoredFields.asScala.map(FieldConverter.apply)
          //for all the records in the group transform

          val json = ToJsonWithProjections(
            fields.toSeq,
            ignoredFields.toSeq,
            record.valueSchema(),
            record.value(),
            k.hasRetainStructure,
          )

          val recordTime =
            if (record.timestamp() != null) record.timestamp().longValue() else System.currentTimeMillis()
          val msgValue: Array[Byte]    = json.toString.getBytes
          val msgKey:   Option[String] = extractMessageKey(record, k)

          MessageTemplate(pulsarTopic, msgKey, msgValue, recordTime)
      }
    }

  private def getKcqlStatementsForTopic(record: SinkRecord) = {
    val topic = record.topic()
    //get the kcql statements for this topic
    val kcqls = mappings(topic)
    kcqls
  }
}
