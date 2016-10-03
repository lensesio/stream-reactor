package com.datamountaineer.streamreactor.connect.mongodb

import java.util

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import org.bson.Document

import scala.collection.JavaConversions._

object JsonNodeToMongoDocument {

  def fromJson(json: String, key: String): Document = {
    Document.parse(json).append("_id", key)
  }

  def apply(json: JsonNode, key: String): Document = {

    def convert(fieldName: String, json: JsonNode, document: Document): Document = {
      if(json.isInstanceOf[NullNode]) document.append(fieldName, null)
      else
      {
        val value = json match {
          case _: ShortNode => json.shortValue()
          case _: IntNode => json.intValue()
          case _: TextNode => json.textValue()
          case _: LongNode => json.longValue()
          case _: FloatNode => json.floatValue()
          case _: DoubleNode => json.doubleValue()
          case _: BooleanNode => json.booleanValue()
          case _: DecimalNode => json.decimalValue()
          case _: BinaryNode => json.binaryValue()
          case _: ArrayNode =>
            val array = new util.ArrayList[Document]
            for {n <- json} {
              val nestedDocument = new Document()
              val k = n.fields().foldLeft(nestedDocument) { case (d, entry) =>
                convert(entry.getKey, entry.getValue, d)
              }
              array.add(k)
            }

            array
          case _: ObjectNode =>
            val nestedDocument = new Document()
            json.fields().foldLeft(nestedDocument) { case (d, entry) =>
              convert(entry.getKey, entry.getValue, d)
            }
        }
        document.append(fieldName, value)
      }
    }

    json match {
      case objNode: ObjectNode =>
        objNode.fields().foldLeft(new Document()) { case (d, e) =>
          convert(e.getKey, e.getValue, d)
        }.append("_id", key)
      case other => throw new IllegalArgumentException("Invalid json to convert to mongo ")
    }
  }
}
