/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.config.processors

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings
import io.lenses.streamreactor.connect.aws.s3.config.processors.kcql.KcqlProcessor
import org.yaml.snakeyaml.Yaml

import java.io.{FileNotFoundException, InputStream, SequenceInputStream}
import java.util
import java.util.Collections.enumeration
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
  * If Yaml profiles are configured on the connector, this tries to load them via a classpath resource, and uses the
  * KcqlProcessor to merge the various different profiles into a single Kcql string.
  */
class YamlProfileProcessor extends ConfigDefProcessor with LazyLogging {

  private val yaml = new Yaml()

  override def process(connectorConfig: Map[String, AnyRef]): Either[Throwable, Map[String, AnyRef]] = {
    val profileResources = for {
      profileResources <- getConfigProfileProperty(connectorConfig)
    } yield profileResources
    profileResources match {
      case Left(error) => error.asLeft[Map[String, AnyRef]]
      case Right(None) => connectorConfig.asRight
      case Right(Some(value)) => parseProps(connectorConfig, value)
    }
  }

  def closeStreams(streams: Seq[InputStream]) : Either[Throwable, Unit] = {
    streams.foreach(s => Try(s.close()))
    ().asRight
  }

  private def parseProps(connectorConfig: Map[String, AnyRef], profileResources: String): Either[Throwable, Map[String, AnyRef]] = {

    for {
      streams <- openProfileFiles(profileResources)
      seqInputStream <- openSequenceInputStream(streams)
      yamlConfigs <- parseMapProperties(seqInputStream)
      _ <- closeStreams(seqInputStream +: streams)
      flattenedYamlProps <- flattenRootYamlProps(yamlConfigs)
      kcqlString <- KcqlProcessor.process(yamlConfigs :+ connectorConfig)
      merged <- mergeYamlProperties(connectorConfig, flattenedYamlProps, kcqlString)
    } yield merged
  }

  private def getConfigProfileProperty(input: Map[String, AnyRef]): Either[Throwable, Option[String]] = {
    input.get(S3ConfigSettings.PROFILES) match {
      case Some(value: String) => Some(value).asRight
      case None => None.asRight
      case other => new IllegalArgumentException(s"Invalid configuration: $other").asLeft
    }
  }

  private def openProfileFiles(profiles: String): Either[Throwable, List[InputStream]] = {
    profiles.split(",").map(
      profile =>
        Try {
          classOf[YamlProfileProcessor].getResourceAsStream(profile)
        } match {
          case Success(value: InputStream) => value
          case Failure(exception) => return exception.asLeft[List[InputStream]]
          case Success(null) => return new FileNotFoundException(s"yaml profile not found: $profile").asLeft[List[InputStream]]
        }
    ).toList.asRight
  }

  private def openSequenceInputStream(fileInputStreams: List[InputStream]): Either[Throwable, SequenceInputStream] = {
    fileInputStreams.filterNot(_ == null) match {
      case Nil => new IllegalStateException("No valid input streams").asLeft[SequenceInputStream]
      case isList: List[InputStream] => Try {
        new SequenceInputStream(enumeration(isList.asJava))
      }.toEither
    }
  }

  private def parseMapProperties(fullYamlStream: SequenceInputStream): Either[Throwable, List[Map[String, AnyRef]]] = {
    Try {
      yaml.loadAll(fullYamlStream).asScala.map {
        case map: util.Map[_, _] => map.asInstanceOf[util.Map[String, AnyRef]].asScala.toMap
        case _ => throw new IllegalStateException("Unexpected type") //TODO don't throw
      }.toList
    }.toEither
  }

  private def flattenRootYamlProps(configSets: List[Map[String, AnyRef]]): Either[Throwable, Map[String, AnyRef]] = {

    configSets.flatMap {
      _.flatMap {
        case (k: String, v: AnyRef) => Some(k, v)
        case _ => None
      }
    }.toMap.asRight
  }

  private def mergeYamlProperties(input: Map[String, AnyRef], yamlProps: Map[String, AnyRef], kcqlString: String): Either[Throwable, Map[String, AnyRef]] = {
    var merged: Map[String, AnyRef] = yamlProps ++ input
    merged = merged -- Seq(S3ConfigSettings.KCQL_BUILDER, S3ConfigSettings.PROFILES)
    merged = merged + (S3ConfigSettings.KCQL_CONFIG -> kcqlString)
    merged.asRight
  }

}
