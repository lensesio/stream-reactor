package io.lenses.streamreactor.connect.aws.s3.model.location

import com.typesafe.scalalogging.LazyLogging

import java.io.{BufferedOutputStream, File, FileOutputStream}

object FileUtils extends LazyLogging {

  def toBufferedOutputStream(file: File) : BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file))

  def createFileAndParents(file: File): Boolean = {

      logger.info("Creating dir {}", file.getParentFile)
      file.getParentFile.mkdirs()

      logger.info("Creating file {}", file)
      file.createNewFile()

  }
}