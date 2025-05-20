/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import cats.Order
import cats.data.NonEmptySeq
import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef
import io.lenses.streamreactor.connect.http.sink.metrics.HttpSinkMetrics
import io.lenses.streamreactor.connect.http.sink.metrics.MetricsRegistrar
import io.lenses.streamreactor.connect.http.sink.tpl.RawTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class HttpSinkTask extends SinkTask with LazyLogging with JarManifestProvided {
  implicit val runtime:       IORuntime    = IORuntime.global
  implicit val topicOrdering: Order[Topic] = Order.fromOrdering(Topic.orderingByTopicValue)

  private var maybeTemplate:      Option[TemplateType]      = Option.empty
  private var maybeWriterManager: Option[HttpWriterManager] = Option.empty
  private var maybeSinkName:      Option[String]            = Option.empty
  private def sinkName = maybeSinkName.getOrElse("Lenses.io HTTP Sink")
  private val deferred: Deferred[IO, Either[Throwable, Unit]] = Deferred.unsafe[IO, Either[Throwable, Unit]]

  private val taskNumberRef: Ref[IO, Int]             = Ref.unsafe[IO, Int](0)
  private val errorRef:      Ref[IO, List[Throwable]] = Ref.unsafe[IO, List[Throwable]](List.empty)

  override def start(props: util.Map[String, String]): Unit = {

    printAsciiHeader(manifest, "/http-sink-ascii.txt")

    val propsAsScala = props.asScala.toMap
    maybeSinkName = propsAsScala.get("name")

    val refUpdateCallback: Throwable => IO[Unit] = (err: Throwable) =>
      this.errorRef.update {
        lts => lts :+ err
      }

    (for {
      taskNumber <- propsAsScala.getOrElse(HttpSinkConfigDef.TaskNumberProp, "1").toIntOption match {
        case Some(value) => IO(value)
        case None        => IO.raiseError(new IllegalArgumentException("Task number must be an integer"))
      }
      _             <- taskNumberRef.set(taskNumber)
      config        <- IO.fromEither(HttpSinkConfig.from(propsAsScala - HttpSinkConfigDef.TaskNumberProp))
      metrics       <- IO(new HttpSinkMetrics())
      _             <- IO(MetricsRegistrar.registerMetricsMBean(metrics, sinkName, taskNumber))
      template       = RawTemplate(config.endpoint, config.content, config.headers, config.nullPayloadHandler)
      writerManager <- HttpWriterManager.apply(sinkName, config, template, deferred, metrics)
      _             <- writerManager.start(refUpdateCallback)
    } yield {
      this.maybeTemplate      = Some(template)
      this.maybeWriterManager = Some(writerManager)
    }).recoverWith {
      case e =>
        // errors at this point simply need to be thrown
        IO.raiseError[Unit](new RuntimeException("Unexpected error occurred during sink start", e))
    }.unsafeRunSync()
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {

    logger.debug(s"[$sinkName] put call with ${records.size()} records")

    val storedErrors = errorRef.get.unsafeRunSync()
    //Filter out null records since there are users who are sending null records
    val nonEmptyRecords = NonEmptySeq.fromSeq(records.asScala.toSeq.filter(_ != null))
    (storedErrors, nonEmptyRecords) match {
      case (errors, _) if errors.nonEmpty =>
        handleStoredErrors(errors)
      case (_, None) =>
        logger.debug(s"[$sinkName] no records seen, continuing")
      case (_, Some(records)) =>
        logger.trace(s"[$sinkName] processing records")
        processRecords(records)
    }
  }

  private def handleStoredErrors(storedErrors: List[Throwable]): Unit =
    throw new ConnectException(s"Previous operation failed with error: " +
      storedErrors.map(_.getMessage).mkString(";"))

  private def processRecords(
    nonEmptyRecords: NonEmptySeq[SinkRecord],
  ): Unit = {

    val template = maybeTemplate.getOrElse(throw new IllegalStateException("No template available in put"))
    val writerManager =
      maybeWriterManager.getOrElse(throw new IllegalStateException("No writer manager available in put"))

    nonEmptyRecords
      .map { rec =>
        Topic(rec.topic()).withPartition(rec.kafkaPartition()).withOffset(Offset(rec.kafkaOffset())) -> rec
      }
      .groupBy { case (tpo, _) => tpo.topic }
      .foreach {
        case (tp, records) =>
          val recs           = records.map(_._2)
          val eitherRendered = template.renderRecords(recs)
          eitherRendered match {
            case Left(ex) =>
              logger.error(s"[$sinkName] Template Rendering Failure", ex)
              IO.raiseError(ex)
            case Right(renderedRecs) =>
              logger.trace(s"[$sinkName] Rendered successful: $renderedRecs")
              writerManager
                .getWriter(tp)
                .flatMap(writer => writer.add(renderedRecs))
                .unsafeRunSync()
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
      taskNumber <- taskNumberRef.get
      _          <- IO(MetricsRegistrar.unregisterMetricsMBean(sinkName, taskNumber))
      _ <- maybeWriterManager.traverse { x =>
        x.closeReportingControllers()
        x.close
      }
      _ <- deferred.complete(().asRight)
    } yield ()).unsafeRunSync()

}
