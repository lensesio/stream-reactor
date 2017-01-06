package com.datamountaineer.streamreactor.connect.ftp

object KeyStyle extends Enumeration {
  type KeyStyle = Value
  val String = Value(FtpSourceConfig.StringKeyStyle)
  val Struct = Value(FtpSourceConfig.StructKeyStyle)
}
