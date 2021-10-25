package io.lenses.streamreactor.connect.aws.s3.source.reader

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.model.SourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocationWithLine
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.apache.kafka.common.errors.OffsetOutOfRangeException

import scala.util.Try

class ReaderCreator(
                     sourceName: String,
                     format: FormatSelection,
                     targetTopic: String
                   )(
  implicit storageInterface: StorageInterface) extends LazyLogging {

  def create(pathWithLine: RemoteS3PathLocationWithLine) : Either[Throwable, ResultReader] = {
    for {
      reader <- Try(createInner(pathWithLine)).toEither
    } yield new ResultReader(reader, targetTopic)
  }

  private def createInner(pathWithLine: RemoteS3PathLocationWithLine): S3FormatStreamReader[_ <: SourceData] = {
    val file = pathWithLine.file
    val inputStreamFn = () => storageInterface.getBlob(file)
    val fileSizeFn = () => storageInterface.getBlobSize(file)
    logger.info(s"[$sourceName] Reading next file: ${pathWithLine.file} from line ${pathWithLine.line}")

    val reader = S3FormatStreamReader(inputStreamFn, fileSizeFn, format, file)

    if (!pathWithLine.isFromStart) {
      skipLinesToStartLine(reader, pathWithLine.line)
    }

    reader
  }

  private def skipLinesToStartLine(reader: S3FormatStreamReader[_ <: SourceData], lineToStartOn: Int) = {
    for (_ <- 0 to lineToStartOn) {
      if (reader.hasNext) {
        reader.next()
      } else {
        throw new OffsetOutOfRangeException("Unknown file offset")
      }
    }
  }


}
