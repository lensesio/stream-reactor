package com.datamountaineer.streamreactor.connect.influx

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}

object JacksonJson {
  val mapper = {
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.setSerializationInclusion(Include.NON_NULL)
    mapper.setSerializationInclusion(Include.NON_EMPTY)
    mapper
  }

  def toJson[T](value: T): String = mapper.writeValueAsString(value)

  def asJson[T](value: T): JsonNode = mapper.valueToTree(value)

}

