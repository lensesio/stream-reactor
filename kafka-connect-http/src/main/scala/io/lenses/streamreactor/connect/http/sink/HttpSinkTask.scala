/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.http.sink

import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.unsafe.IORuntime
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig.fromJson
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.configProp
import io.lenses.streamreactor.connect.http.sink.tpl.RawTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import cats.syntax.all._
import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class HttpSinkTask extends SinkTask with LazyLogging {
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  implicit val runtime = IORuntime.global
  private var maybeTemplate:      Option[TemplateType]      = Option.empty
  private var maybeWriterManager: Option[HttpWriterManager] = Option.empty
  private var maybeSinkName:      Option[String]            = Option.empty
  private def sinkName = maybeSinkName.getOrElse("Lenses.io HTTP Sink")
  private val deferred: Deferred[IO, Either[Throwable, Unit]] = Deferred.unsafe[IO, Either[Throwable, Unit]]

  private val errorRef: Ref[IO, List[Throwable]] = Ref.of[IO, List[Throwable]](List.empty).unsafeRunSync()

  override def start(props: util.Map[String, String]): Unit = {

    printAsciiHeader(manifest, "/http-sink-ascii.txt")

    val propsAsScala = props.asScala
    maybeSinkName = propsAsScala.get("name")

    IO
      .fromEither(parseConfig(propsAsScala.get(configProp)))
      .flatMap { config =>
        val template      = RawTemplate(config.endpoint, config.content, config.headers.getOrElse(Seq.empty))
        val writerManager = HttpWriterManager(sinkName, config, template, deferred)
        val refUpdateCallback: Throwable => Unit =
          (err: Throwable) => {
            {
              for {
                updated <- this.errorRef.getAndUpdate {
                  lts => lts :+ err
                }
              } yield updated
            }.unsafeRunSync()
            ()

          }
        writerManager.start(refUpdateCallback)
          .map { _ =>
            this.maybeTemplate      = Some(template)
            this.maybeWriterManager = Some(writerManager)
          }
      }
      .recoverWith {
        case e =>
          // errors at this point simply need to be thrown
          IO.raiseError[Unit](new RuntimeException("Unexpected error occurred during sink start", e))
      }.unsafeRunSync()
  }

  private def parseConfig(propVal: Option[String]): Either[Throwable, HttpSinkConfig] =
    propVal.toRight(new IllegalArgumentException("No prop found"))
      .flatMap(fromJson)

  override def put(records: util.Collection[SinkRecord]): Unit = {

    logger.debug(s"[$sinkName] put call with ${records.size()} records")
    val storedErrors = errorRef.get.unsafeRunSync()

    if (storedErrors.nonEmpty) {
      throw new ConnectException(s"Previous operation failed with error: " +
        storedErrors.map(_.getMessage).mkString(";"))

    } else {

      logger.trace(s"[$sinkName] building template")
      val template = maybeTemplate.getOrElse(throw new IllegalStateException("No template available in put"))
      val writerManager =
        maybeWriterManager.getOrElse(throw new IllegalStateException("No writer manager available in put"))

      records
        .asScala
        .toSeq
        .map {
          rec =>
            Topic(rec.topic()).withPartition(rec.kafkaPartition()).withOffset(Offset(rec.kafkaOffset())) -> rec
        }
        .groupBy {
          case (tpo, _) => tpo.topic
        }
        .foreach {
          case (tp, records) =>
            val recs           = records.map(_._2)
            val eitherRendered = template.renderRecords(recs)
            eitherRendered match {
              case Left(ex) =>
                logger.error(s"[$sinkName] Template Rendering Failure", ex)
                IO.raiseError(ex)
              // rendering errors can not be recovered from as configuration should be amended

              case Right(renderedRecs) =>
                logger.trace(s"[$sinkName] Rendered successful: $renderedRecs")
                writerManager
                  .getWriter(tp)
                  .flatMap {
                    writer =>
                      writer.add(renderedRecs)
                  }
                  .unsafeRunSync()
            }

        }
    }
  }

  override def preCommit(
    currentOffsets: util.Map[KafkaTopicPartition, OffsetAndMetadata],
  ): util.Map[KafkaTopicPartition, OffsetAndMetadata] = {

    val writerManager =
      maybeWriterManager.getOrElse(throw new IllegalStateException("No writer manager available in put"))

    def getDebugInfo(in: util.Map[KafkaTopicPartition, OffsetAndMetadata]): String =
      in.asScala.map {
        case (k, v) =>
          k.topic() + "-" + k.partition() + "=" + v.offset()
      }.mkString(";")

    logger.debug(s"[{}] preCommit with offsets={}",
                 sinkName,
                 getDebugInfo(Option(currentOffsets).getOrElse(new util.HashMap())): Any,
    )

    val topicPartitionOffsetTransformed: Map[TopicPartition, OffsetAndMetadata] =
      Option(currentOffsets)
        .getOrElse(new util.HashMap())
        .asScala
        .map {
          case (tp, offsetAndMetadata) =>
            Topic(tp.topic()).withPartition(tp.partition()) -> offsetAndMetadata
        }
        .toMap

    (for {
      offsets <- writerManager
        .preCommit(topicPartitionOffsetTransformed)
      tpoTransformed = offsets.map {
        case (topicPartition, offsetAndMetadata) =>
          (topicPartition.toKafka, offsetAndMetadata)
      }.asJava
      _ <- IO(logger.debug(s"[{}] Returning latest written offsets={}", sinkName, getDebugInfo(tpoTransformed)))
    } yield tpoTransformed).unsafeRunSync()

  }

  override def stop(): Unit =
    (for {
      _ <- maybeWriterManager.traverse(_.close)
      _ <- deferred.complete(().asRight)
    } yield ()).unsafeRunSync()

  override def version(): String = manifest.version()
}
