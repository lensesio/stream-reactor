/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.datamountaineer.streamreactor.connect.jms.sink.converters

import com.datamountaineer.streamreactor.connect.jms.config.JMSSetting
import com.github.os72.protocjar.Protoc
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.json.JsonConverterConfig
import org.apache.kafka.connect.sink.SinkRecord
import cats.implicits._
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class ProtoStoredAsConverter() extends ProtoConverter with StrictLogging {

  private val SCHEMA_PROTO_PATH             = "proto_path"
  private val SCHEMA_PROTO_FILE             = "proto_file"
  private val CONNECT_SINK_CONVERTER_PREFIX = "connect.sink.converter"

  private val CONNECT_SINK_CONVERTER_SCHEMA_CONFIG = CONNECT_SINK_CONVERTER_PREFIX + "." + SCHEMA_PROTO_PATH
  private val jsonConverter                        = new JsonConverter

  private val descriptors = new ConcurrentHashMap[String, Descriptors.Descriptor]

  private var defaultProtoPath: String = _

  private val BACK_QUOTE = "`"
  private val EMPTY      = ""

  override def initialize(map: Map[String, String]): Unit = {
    defaultProtoPath = map.get(CONNECT_SINK_CONVERTER_SCHEMA_CONFIG).getOrElse(EMPTY)
    jsonConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false), false)
  }

  override def convert(record: SinkRecord, setting: JMSSetting): Either[IOException, Array[Byte]] = {
    val storedAs = setting.storageOptions.storedAs
      .replace(BACK_QUOTE, EMPTY)
    logger.debug(s"storedAs:  $storedAs")

    //Cache the descriptor lookup so not doing reflection on every record.
    val descriptor = descriptors.computeIfAbsent(
      storedAs,
      (name: String) => {
        val properties: util.Map[String, String] = setting.storageOptions.storedAsProperties.asJava
        val protoPath = properties.getOrDefault(SCHEMA_PROTO_PATH, defaultProtoPath)
          .replace(BACK_QUOTE, EMPTY)
        logger.debug(s"protoPath:  $protoPath")
        val protoFile = properties.getOrDefault(SCHEMA_PROTO_FILE, EMPTY)
          .replace(BACK_QUOTE, EMPTY)
        logger.debug(s"protoFile:  $protoFile")

        try if (protoPath.trim.nonEmpty) {
          if (protoFile.trim.nonEmpty) {
            getDescriptor(name, protoPath, protoFile)
          } else {
            val basePath = Paths.get(protoPath)
            val protoFiles: util.Collection[String] =
              Files.find(basePath,
                         Integer.MAX_VALUE,
                         (_: Path, fileAttr: BasicFileAttributes) => fileAttr.isRegularFile,
              )
                .map[String](file => basePath.relativize(file).toString)
                .collect(Collectors.toList[String])
            getDescriptor(name, protoPath, protoFiles)
          }
        } else {
          val specificProtobufClass = Class.forName(name)
          logger.debug(s"Class loaded is: $specificProtobufClass")
          val parseMethod = specificProtobufClass.getDeclaredMethod("getDescriptor")
          parseMethod.invoke(null)
            .asInstanceOf[Descriptors.Descriptor]
        } catch {
          case x: Exception => logger.error("Invalid storedAs settings: " + x.getMessage)
            null
        }
      },
    )

    if (descriptor == null) {
      throw new DataException("Invalid storedAs settings")
    }

    // As Protobuf is Positional based, yet Record is FieldName based,
    // we can convert to JSON and then back into Proto using JsonFormat which will match the FieldName to the Protobuf FieldName
    // This is safest for compatibility to be explicit though unfortunate the extra convert to JSON is not ideal.
    val json_converted_data = jsonConverter.fromConnectData(record.topic, record.valueSchema, record.value)
    val json                = new String(json_converted_data, StandardCharsets.UTF_8)
    val b                   = DynamicMessage.newBuilder(descriptor)

    JsonFormat.parser().usingTypeRegistry(TypeRegistry.getEmptyTypeRegistry).ignoringUnknownFields()
      .merge(json, b)

    b.build
      .toByteArray.asRight
  }

  private def getDescriptor(
    message:    String,
    protoPath:  String,
    protoFiles: util.Collection[String],
  ): Descriptors.Descriptor =
    protoFiles.asScala
      .flatMap(file => Option(getDescriptor(message, protoPath, file)))
      .headOption
      .orNull

  private def getDescriptor(message: String, protoPath: String, protoFile: String): Descriptors.Descriptor = try {
    val descFile: File = generateDescFile(protoPath, protoFile)
    val fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(new FileInputStream(descFile.getAbsolutePath))
    val fileDescriptorProto = fileDescriptorSet.getFileList.stream.filter((fdp: DescriptorProtos.FileDescriptorProto) =>
      fdp.getName == protoFile,
    )
      .findFirst
      .orElse(null)

    if (fileDescriptorProto != null) {
      val fileDescriptor = buildFileDescriptor(fileDescriptorProto, fileDescriptorSet)
      val descriptor = fileDescriptor.getMessageTypes.stream
        .filter((pointerDescriptor: Descriptors.Descriptor) => pointerDescriptor.getFullName == message)
        .findFirst
        .orElse(null)

      logger.debug(s"Descriptor value is $descriptor")
      descriptor
    } else {
      logger.error(s"File descriptor name=$message doesn't match with proto file name=$protoFile")
      null
    }
  } catch {
    case x @ (_: IOException | _: InterruptedException) =>
      logger.error("Unexpected error", x.getMessage)
      null
  }

  private def generateDescFile(protoPath: String, protoFile: String) = {
    val descFile = File.createTempFile(protoFile, ".desc")
    val args2 = Array(
      "--include_std_types",
      "--proto_path=" + protoPath,
      "--descriptor_set_out=" + descFile.getAbsolutePath,
      "--include_imports",
      protoPath + File.separator + protoFile,
    )

    Protoc.runProtoc(args2)
    descFile
  }

  private def buildFileDescriptor(
    proto: DescriptorProtos.FileDescriptorProto,
    set:   DescriptorProtos.FileDescriptorSet,
  ): Descriptors.FileDescriptor = {
    val fileProtoCache = new util.HashMap[String, DescriptorProtos.FileDescriptorProto]
    set.getFileList.asScala.foreach((file: DescriptorProtos.FileDescriptorProto) =>
      fileProtoCache.put(file.getName, file),
    )
    buildFileDescriptor(proto, fileProtoCache)
  }

  private def buildFileDescriptor(
    currentFileProto: DescriptorProtos.FileDescriptorProto,
    fileProtoCache:   util.Map[String, DescriptorProtos.FileDescriptorProto],
  ): Descriptors.FileDescriptor = {
    val dependencies = currentFileProto.getDependencyList.asScala.foldLeft(Array.empty[Descriptors.FileDescriptor]) {
      case (array, dependency: String) =>
        val dependencyFileProto = fileProtoCache.get(dependency)
        val dependencyFileDescriptor: Descriptors.FileDescriptor =
          buildFileDescriptor(dependencyFileProto, fileProtoCache)
        array :+ dependencyFileDescriptor
    }

    Try(Descriptors.FileDescriptor.buildFrom(currentFileProto, dependencies)) match {
      case Success(value) => value
      case Failure(ex)    => throw new IllegalStateException("FileDescriptor build fail!", ex)
    }
  }

}
