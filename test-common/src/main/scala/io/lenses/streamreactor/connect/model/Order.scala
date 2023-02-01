package io.lenses.streamreactor.connect.model

import com.sksamuel.avro4s.SchemaFor
import com.sksamuel.avro4s.ToRecord
import com.sksamuel.avro4s.{ Encoder => AvroEncoder }
import io.circe._
import io.circe.generic.semiauto._
import org.apache.avro.generic.GenericRecord

import scala.beans.BeanProperty

case class Order(
  @BeanProperty id:      Int,
  @BeanProperty product: String,
  @BeanProperty price:   Double,
  @BeanProperty qty:     Int,
  @BeanProperty created: String,
) {

  def toRecord(order: Order): GenericRecord = {
    val orderSchema      = SchemaFor.apply[Order]
    implicit val encoder = AvroEncoder[Order].withSchema(orderSchema)
    ToRecord.apply[Order].to(order)
  }

}

object Order {

  implicit val orderEncoder: Encoder.AsObject[Order] = deriveEncoder[Order]
  implicit val orderDecoder: Decoder[Order]          = deriveDecoder[Order]

  def apply(id: Int, product: String, price: Double, quantity: Int): Order =
    Order(id, product, price, quantity, null)
}
