package com.datamountaineer.streamreactor.connect.hbase

import java.nio.file.Paths

object PersonAvroSchema {
  lazy val schema = scala.io.Source.fromFile(Paths.get(getClass.getResource("/person.avsc").toURI).toFile).mkString
}
