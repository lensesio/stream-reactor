package com.datamountaineer.streamreactor.connect.cassandra.utils

import com.datamountaineer.streamreactor.connect.json.SimpleJsonConverter
import com.jayway.jsonpath.{Configuration, JsonPath}
import org.apache.kafka.connect.data.{Schema, Struct}

object KeyUtils {

  /**
    * Traverse a JSON structure, returning a list of the values of the given fields.
    *
    * @param json
    * @param fieldNames
    * @return
    */
  def keysFromJson(json: String, fieldNames: Seq[String]): Seq[Object] = {
    val document = Configuration.defaultConfiguration.jsonProvider.parse(json)
    fieldNames map { f => JsonPath.read(document, f).asInstanceOf[Object] }
  }


  /**
    * Traverse a Struct, returning a list of the values of the given fields.
    *
    * @param struct
    * @param schema
    * @param fieldNames
    * @return
    */
  def keysFromStruct(struct: Struct, schema: Schema, fieldNames: Seq[String]): Seq[Object] =
    keysFromJson(new SimpleJsonConverter().fromConnectData(schema, struct).toString, fieldNames)

}
