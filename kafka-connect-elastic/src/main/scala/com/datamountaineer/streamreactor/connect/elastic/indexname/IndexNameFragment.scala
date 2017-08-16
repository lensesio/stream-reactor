package com.datamountaineer.streamreactor.connect.elastic.indexname

import java.time.Clock
import java.time.LocalDateTime._
import java.time.format.DateTimeFormatter._

object ClockProvider {
  val ClockInstance: Clock = Clock.systemUTC()
}

sealed trait IndexNameFragment {
  def getFragment: String
}

case class TextFragment(text: String) extends IndexNameFragment {
  override def getFragment: String = text
}

case class DateTimeFragment(dateTimeFormat: String, clock: Clock = ClockProvider.ClockInstance) extends IndexNameFragment {
  override def getFragment: String = s"${now(clock).format(ofPattern(dateTimeFormat))}"
}
object DateTimeFragment {
  val OpeningChar = '{'
  val ClosingChar = '}'
}
