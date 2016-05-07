package com.datastax.driver.core

import com.datastax.driver.core.ColumnDefinitions.Definition
/**
  * Created by andrew@datamountaineer.com on 21/04/16.
  * stream-reactor
  */
object TestUtils {
  def getColumnDefs :ColumnDefinitions = {
    val cols = List(
      "uuidCol" -> DataType.uuid(),
      "inetCol" -> DataType.inet(),
      "asciiCol" -> DataType.ascii(),
      "textCol" -> DataType.text(),
      "varcharCol" -> DataType.varchar(),
      "booleanCol" -> DataType.cboolean(),
      "smallintCol" -> DataType.smallint(),
      "intCol" -> DataType.cint(),
      "decimalCol" -> DataType.decimal(),
      "floatCol" -> DataType.cfloat(),
      "counterCol" -> DataType.counter(),
      "bigintCol" -> DataType.bigint(),
      "varintCol" -> DataType.varint(),
      "doubleCol" -> DataType.cdouble(),
      "timeuuidCol" -> DataType.timeuuid(),
      "blobCol" -> DataType.blob(),
      "dateCol" -> DataType.date(),
      "timeCol" -> DataType.time(),
      "timestampCol"->DataType.timestamp(),
      "mapCol"->DataType.map(DataType.varchar(), DataType.varchar()),
      "listCol"->DataType.list(DataType.varchar()),
      "setCol"->DataType.set(DataType.varchar())
    )

    val definitions = cols.map {
      case (name, colType) => new Definition("sink_test", "sink_test", name, colType)
    }.toArray
    new ColumnDefinitions(definitions, CodecRegistry.DEFAULT_INSTANCE)
  }
}
