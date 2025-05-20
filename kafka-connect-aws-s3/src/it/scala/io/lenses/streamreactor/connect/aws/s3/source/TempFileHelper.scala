package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits._
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError

import java.io.File
import java.util.UUID

trait TempFileHelper {

  def withFile(fileName: String)(f: File => Either[UploadError, String]): Either[Throwable, Unit] = {
    val folderName = UUID.randomUUID().toString
    val folder     = new File(folderName)
    try {
      folder.mkdir()
      folder.deleteOnExit()
      val file = new File(folder, fileName)
      file.deleteOnExit()
      f(file)
    } catch {
      case e: UploadError => Left(e)
    } finally {
      folder.listFiles().foreach(_.delete())
      folder.delete()
      ()
    }
  }
    .leftMap(uploadError => new IllegalStateException(uploadError.message()))
    .map(_ => ())
}
