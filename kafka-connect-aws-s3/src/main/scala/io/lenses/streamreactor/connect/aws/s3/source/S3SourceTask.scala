/*
 * Copyright 2020 Lenses.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lenses.streamreactor.connect.aws.s3.source

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import io.lenses.streamreactor.connect.aws.s3.auth.AwsContextCreator
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.source.config.S3SourceConfig
import io.lenses.streamreactor.connect.aws.s3.storage.{MultipartBlobStoreStorageInterface, StorageInterface}
import org.apache.kafka.connect.data.{Schema, SchemaAndValue}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import java.util
import scala.collection.JavaConverters._
import scala.util.Try

class S3SourceTask extends SourceTask {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  private implicit var storageInterface: StorageInterface = _

  private implicit var sourceLister: S3SourceLister = _

  private var config: S3SourceConfig = _

  private var readerManagers: Seq[S3BucketReaderManager] = _

  override def version(): String = manifest.version()

  /**
    * Start sets up readers for every configured connection in the properties
    **/
  override def start(props: util.Map[String, String]): Unit = {

    logger.debug(s"Received call to S3SinkTask.start with ${props.size()} properties")

    val awsConfig = S3SourceConfig(props.asScala.toMap)

    val awsContextCreator = new AwsContextCreator(AwsContextCreator.DefaultCredentialsFn)
    storageInterface = new MultipartBlobStoreStorageInterface(awsContextCreator.fromConfig(awsConfig.s3Config))
    sourceLister = new S3SourceLister()

    val configs = Option(context).flatMap(c => Option(c.configs())).filter(_.isEmpty == false).getOrElse(props)

    config = S3SourceConfig(configs.asScala.toMap)


    val offsetFn: (String, String) => Option[OffsetReaderResult] =
      (bucket, prefix) => {
        val key = fromSourcePartition(bucket, prefix).asJava
        Try {
          val matchingOffset = context.offsetStorageReader().offset(key).asScala.toMap

          OffsetReaderResult(
            matchingOffset.getOrElse("path", throw new IllegalArgumentException("Could not find path value in matching offset")).asInstanceOf[String],
            matchingOffset.getOrElse("line", throw new IllegalArgumentException("Could not find line value in matching offset")).asInstanceOf[String],
          )
        }.toOption
      }

    readerManagers = config.bucketOptions
      .map(new S3BucketReaderManager(_, offsetFn))

  }

  override def stop(): Unit = {
    logger.debug(s"Received call to S3SinkTask.stop")

    readerManagers.foreach(_.close())
  }

  override def poll(): util.List[SourceRecord] = readerManagers
    .flatMap(_.poll())
    .flatMap(convertToSourceRecordList)
    .asJava

  private def fromSourcePartition(bucket: String, prefix: String) =
    Map(
      "container" -> bucket,
      "prefix" -> prefix
    )

  private def fromSourceOffset(bucketAndPath: BucketAndPath, offset: Long): Map[String, AnyRef] =
    Map(
      "path" -> bucketAndPath.path,
      "line" -> offset.toString
    )

  private def convertToSourceRecordList(pollResults: PollResults): Vector[SourceRecord] =
    pollResults.resultList.map(convertToSourceRecord(_, pollResults))

  private def convertToSourceRecord(sourceData: SourceData, pollResults: PollResults): SourceRecord = {

    val (schema: Option[Schema], value: AnyRef, key: Option[AnyRef]) = sourceData match {
      case SchemaAndValueSourceData(result: SchemaAndValue, _) =>
        (Some(result.schema()), result.value(), None)
      case StringSourceData(result: String, _) =>
        (None, result, None)
      case ByteArraySourceData(resultBytes, _) =>
        (None, resultBytes.value, Some(resultBytes.key))
      case _ =>
        throw new IllegalArgumentException(s"Unexpected type in convertToSourceRecord, ${sourceData.getClass}")
    }
    if (key.isDefined) {
      new SourceRecord(
        fromSourcePartition(pollResults.bucketAndPath.bucket, pollResults.prefix).asJava,
        fromSourceOffset(pollResults.bucketAndPath, sourceData.getLineNumber).asJava,
        pollResults.targetTopic,
        null,
        key,
        schema.orNull,
        value
      )
    } else {
      new SourceRecord(
        fromSourcePartition(pollResults.bucketAndPath.bucket, pollResults.prefix).asJava,
        fromSourceOffset(pollResults.bucketAndPath, sourceData.getLineNumber).asJava,
        pollResults.targetTopic,
        schema.orNull,
        value
      )
    }

  }

}
