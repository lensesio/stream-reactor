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

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig.fromJson
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef.configProp
import io.lenses.streamreactor.connect.http.sink.tpl.templates.ProcessedTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.templates.RawTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.templates.Template
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.http4s.client.Client
import org.http4s.jdkhttpclient.JdkHttpClient

import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

class HttpSinkTask extends SinkTask {
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  private var maybeConfig: Option[HttpSinkConfig] = Option.empty

  private var maybeTemplate: Option[Template] = Option.empty

  private var client:                Client[IO] = _
  private var clientResourceRelease: IO[Unit]   = _

  override def start(props: util.Map[String, String]): Unit = {
    {
      JdkHttpClient.simple[IO].allocated.unsafeRunSync() match {
        case (cRes, cResRel) =>
          client                = cRes
          clientResourceRelease = cResRel
      }

      for {
        propVal <- props.asScala.get(configProp).toRight(new IllegalArgumentException("No prop found"))
        config  <- fromJson(propVal)
        template = RawTemplate(config.endpoint, config.content, config.headers)
          .parse()
      } yield {
        this.maybeConfig   = config.some
        this.maybeTemplate = template.some
      }
    }.leftMap(throw _)
    ()

  }

  override def put(records: util.Collection[SinkRecord]): Unit = {

    // TODO: handle error if client hasn't been allocated

    val sinkRecords = records.asScala
    (maybeConfig, maybeTemplate) match {
      case (Some(config), Some(template)) =>
        val sender = new HttpRequestSender(config)

        sinkRecords.map {
          record =>
            val processed: ProcessedTemplate = template.process(record)
            sender.sendHttpRequest(
              client,
              processed,
            )
        }.toSeq.sequence *> IO.unit
      case _ => throw new IllegalArgumentException("Config or template not set")
    }

  }

  override def stop(): Unit =
    clientResourceRelease.unsafeRunSync()

  override def version(): String = manifest.version()
}
