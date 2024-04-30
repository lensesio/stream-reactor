package io.lenses.streamreactor.connect.aws.s3.source

import java.io.File
import java.util.UUID

trait TempFileHelper {

  def withFile(fileName: String)(f: File => Either[Throwable, Unit]): Either[Throwable, Unit] = {
    val folderName = UUID.randomUUID().toString
    val folder     = new File(folderName)
    try {
      folder.mkdir()
      folder.deleteOnExit()
      val file = new File(folder, fileName)
      file.deleteOnExit()
      f(file)
    } catch {
      case e: Throwable => Left(e)
    } finally {
      folder.listFiles().foreach(_.delete())
      folder.delete()
      ()
    }
  }
}
