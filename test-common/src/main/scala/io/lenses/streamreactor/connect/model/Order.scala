package io.lenses.streamreactor.connect.model

import scala.beans.BeanProperty

case class Order(
  @BeanProperty id:      Int,
  @BeanProperty product: String,
  @BeanProperty price:   Double,
  @BeanProperty qty:     Int,
  @BeanProperty created: String = null,
)
