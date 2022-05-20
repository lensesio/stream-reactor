package io.lenses.streamreactor.connect.aws.s3.model.location

import com.typesafe.scalalogging.LazyLogging

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream

object FileUtils extends LazyLogging {

  def toBufferedOutputStream(file: File): BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file))

  def createFileAndParents(file: File): Boolean = {
    Option(file.getParentFile)
      .foreach {
        parent =>
          logger.debug("Creating dir {}", parent)
          parent.mkdirs()
      }

    logger.debug("Creating file {}", file)
    file.createNewFile()
  }
}
