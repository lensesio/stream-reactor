package io.lenses.streamreactor.connect.mqtt.source

import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.Formats
import org.json4s.NoTypeHints

object JacksonJson {

  //implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  /*def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }*/

  def toJson[T <: AnyRef](value: T): String =
    write(value)
}
