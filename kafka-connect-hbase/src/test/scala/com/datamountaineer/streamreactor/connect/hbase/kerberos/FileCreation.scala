package com.datamountaineer.streamreactor.connect.hbase.kerberos

import java.io.File

trait FileCreation {
  def createFile(path: String): File = {
    val file = new File(path)
    if (!file.exists()) {
      file.createNewFile()
    }
    file
  }
}
