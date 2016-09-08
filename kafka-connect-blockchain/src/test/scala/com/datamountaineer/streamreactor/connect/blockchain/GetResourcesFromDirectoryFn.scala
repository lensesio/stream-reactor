package com.datamountaineer.streamreactor.connect.blockchain

import java.io.File

object GetResourcesFromDirectoryFn {
  def apply(path: String) = {
    val folder = new File(this.getClass.getResource(path).toURI.getPath)
    require(folder.isDirectory, "$path is not a valid directory")
    folder.listFiles().toSeq
  }
}
