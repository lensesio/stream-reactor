package io.lenses.streamreactor.connect.aws.s3.model

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.Format

/**
  * For a source, can extract the original partition so that the message can be returned to the original partition.
  */
sealed trait PartitionExtractor {

  def extract(remoteS3Path: String): Option[Int]
}

object PartitionExtractor extends LazyLogging {
  def apply(extractorType: String, extractorRegex: Option[String]): Option[PartitionExtractor] =
    extractorType.toLowerCase match {
      case "regex" =>
        extractorRegex.fold {
          logger.info("No regex provided for regex mode, defaulting to NoOp mode")
          Option.empty[PartitionExtractor]
        }(rex => Some(new RegexPartitionExtractor(rex)))
      case "hierarchical" => Some(new HierarchicalPartitionExtractor())
      case _              => Option.empty[PartitionExtractor]
    }
}
class RegexPartitionExtractor(
  pathRegex: String,
) extends PartitionExtractor
    with LazyLogging {
  private val rc = pathRegex.r
  logger.debug("Initialised RegexPartitionExtractor with regex {}", pathRegex)
  override def extract(remoteS3Path: String): Option[Int] =
    Option(rc.findAllIn(remoteS3Path).group(1)).flatMap(_.toIntOption)
}

class HierarchicalPartitionExtractor()
    extends RegexPartitionExtractor(
      s"(?i)^(?:.*)\\/([0-9]*)\\/(?:[0-9]*)[.](?:${Format.values.map(_.entryName).mkString("|")})$$",
    ) {}
