package com.landoop.streamreactor.connect.hive

case class UnsupportedSchemaType(msg: String) extends RuntimeException(msg)

case class UnsupportedHiveTypeConversionException(msg: String) extends Exception(msg)
