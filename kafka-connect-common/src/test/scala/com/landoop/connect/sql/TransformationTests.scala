package com.landoop.connect.sql

import com.sksamuel.avro4s.{RecordFormat, SchemaFor, ToRecord}
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by stefan on 16/04/2017.
  */
class TransformationTests extends WordSpec with Matchers {
  "Transformation" should {
    "handle records for which we haven't specified SQL" in {
      val t = new Transformation[SinkRecord]
      t.configure(Map.empty[String, Any].asJava)

      val sr = new SinkRecord("topic1", 1, Schema.INT64_SCHEMA, 122, Schema.BYTES_SCHEMA, Array(1, 2, 3, 4), 1)
      val actual = t.apply(sr)
      actual shouldBe sr
    }

    "only apply the SQL to the registered topics for value only" in {
      val topic1 = "the_one_with_kcql"
      val topic2 = "the_one_without_kcql"

      val t = new Transformation[SinkRecord]
      t.configure(Map(
        Transformation.VALUE_SQL_CONFIG -> s"SELECT *, name as fieldName FROM $topic1 withstructure"
      ).asJava)

      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val avroData = new AvroData(4)
      val struct = avroData.toConnectData(SchemaFor[Pizza](), record).value.asInstanceOf[Struct]

      val sr1 = new SinkRecord(topic1, 1, struct.schema(), struct, struct.schema(), struct, 991)
      val sr2 = new SinkRecord(topic2, 1, struct.schema(), struct, struct.schema(), struct, 128)

      val newSr2 = t.apply(sr2)
      newSr2 shouldBe sr2

      val newSr1 = t.apply(sr1)
      newSr1.topic shouldBe sr1.topic
      newSr1.kafkaPartition() shouldBe sr1.kafkaPartition()
      newSr1.kafkaOffset() shouldBe sr1.kafkaOffset()

      val newpepperoni = LocalPizzaS(Seq(LocalIngredientS("pepperoni", 12, 4.4), LocalIngredientS("onions", 1, 0.4)), false, false, 98, "pepperoni")
      newSr1.key() shouldBe struct
      newSr1.keySchema() shouldBe struct.schema()
      compare(newSr1.value().asInstanceOf[Struct], newpepperoni)
    }

    "only apply the SQL to the registered topics for key only" in {
      val topic1 = "the_one_with_sql"
      val topic2 = "the_one_without_sql"

      val t = new Transformation[SinkRecord]
      t.configure(Map(
        Transformation.KEY_SQL_CONFIG -> s"SELECT *, name as fieldName FROM $topic1 withstructure"
      ).asJava)

      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val avroData = new AvroData(4)
      val struct = avroData.toConnectData(SchemaFor[Pizza](), record).value.asInstanceOf[Struct]

      val sr1 = new SinkRecord(topic1, 1, struct.schema(), struct, struct.schema(), struct, 991)
      val sr2 = new SinkRecord(topic2, 1, struct.schema(), struct, struct.schema(), struct, 128)

      val newSr2 = t.apply(sr2)
      newSr2 shouldBe sr2

      val newSr1 = t.apply(sr1)
      newSr1.topic shouldBe sr1.topic
      newSr1.kafkaPartition() shouldBe sr1.kafkaPartition()
      newSr1.kafkaOffset() shouldBe sr1.kafkaOffset()

      val newpepperoni = LocalPizzaS(Seq(LocalIngredientS("pepperoni", 12, 4.4), LocalIngredientS("onions", 1, 0.4)), false, false, 98, "pepperoni")
      newSr1.value() shouldBe struct
      newSr1.valueSchema() shouldBe struct.schema()
      compare(newSr1.key().asInstanceOf[Struct], newpepperoni)
    }

  }

  private def compare[T](actual: Struct, t: T)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T]) = {
    val avroData = new AvroData(4)
    val expectedSchema = avroData.toConnectSchema(schemaFor()).toString
      .replace("LocalPizzaS", "Pizza")
      .replace("LocalIngredientS", "Ingredient")

    actual.schema.toString shouldBe expectedSchema

    val expectedRecord = avroData.toConnectData(schemaFor(), toRecord.apply(t)).value()

    actual.toString shouldBe expectedRecord.toString
  }
}
