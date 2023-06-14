package io.lenses.streamreactor.connect.testcontainers.connect

trait CnfVal[T <: Any] {
  def get: T
}

case class StringCnfVal(s: String) extends CnfVal[String] {
  override def get: String = s
}

case class IntCnfVal(s: Int) extends CnfVal[Int] {
  override def get: Int = s
}

case class LongCnfVal(s: Long) extends CnfVal[Long] {
  override def get: Long = s
}

case class BooleanCnfVal(s: Boolean) extends CnfVal[Boolean] {
  override def get: Boolean = s
}
