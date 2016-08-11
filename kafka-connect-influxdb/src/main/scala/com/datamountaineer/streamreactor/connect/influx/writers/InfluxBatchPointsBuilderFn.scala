package com.datamountaineer.streamreactor.connect.influx.writers

import java.util.concurrent.TimeUnit

import com.datamountaineer.streamreactor.connect.influx.config.InfluxSettings
import io.confluent.common.config.ConfigException
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.influxdb.InfluxDB.ConsistencyLevel
import org.influxdb.dto.{BatchPoints, Point}


object InfluxBatchPointsBuilderFn {
  def apply(records: Seq[SinkRecord],
            settings: InfluxSettings): BatchPoints = {
    val batchPoints = BatchPoints
      .database(settings.database)
      .tag("async", "true")
      .retentionPolicy("default")
      .consistency(ConsistencyLevel.ALL)
      .build()

    records.foreach(r => buildPoint(r, settings).map(batchPoints.point))
    batchPoints
  }

  private def buildPoint(record: SinkRecord, settings: InfluxSettings): Option[Point] = {
    require(record.value() != null && record.value().getClass == classOf[Struct],
      "The SinkRecord payload should be of type Struct")

    //we want to error if the topic hasn;t been
    val extractor = settings.fieldsExtractorMap(record.topic())
    val recordData = extractor.get(record.value.asInstanceOf[Struct])
    if (recordData.fields.nonEmpty) {
      val pointBuilder = Point.measurement(settings.topicToMeasurementMap.getOrElse(record.topic(), throw new ConfigException(s"No matching measurement for topic ${record.topic}")))
        .time(recordData.timestamp, TimeUnit.MILLISECONDS)

      recordData.fields
        .foldLeft(pointBuilder) {
          case (builder, (field, value: Long)) => builder.addField(field, value)
          case (builder, (field, value: Int)) => builder.addField(field, value)
          case (builder, (field, value: Byte)) => builder.addField(field, value)
          case (builder, (field, value: Short)) => builder.addField(field, value)
          case (builder, (field, value: Double)) => builder.addField(field, value)
          case (builder, (field, value: Float)) => builder.addField(field, value)
          case (builder, (field, value: Boolean)) => builder.addField(field, value)
          case (builder, (field, value: String)) => builder.addField(field, value)
          //we should never reach this since the extractor should not allow it
          case (builder, (field, value)) => sys.error(s"$value is not a valid type for InfluxDb.Allowed types:Boolean, Long, String, Double and Number")
        }
      Some(pointBuilder.build())
    }
    else None
  }
}
