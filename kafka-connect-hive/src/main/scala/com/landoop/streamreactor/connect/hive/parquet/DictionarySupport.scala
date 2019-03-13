package com.landoop.streamreactor.connect.hive.parquet

import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.PrimitiveConverter

trait DictionarySupport {
  self: PrimitiveConverter =>
  var dictionary: Dictionary = _
  override def hasDictionarySupport: Boolean = true
  override def setDictionary(dictionary: Dictionary): Unit = this.dictionary = dictionary
  override def addValueFromDictionary(dictionaryId: Int): Unit = addBinary(dictionary.decodeToBinary(dictionaryId))
}
