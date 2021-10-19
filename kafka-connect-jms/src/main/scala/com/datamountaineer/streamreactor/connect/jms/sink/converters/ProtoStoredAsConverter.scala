package com.datamountaineer.streamreactor.connect.jms.sink.converters

import com.datamountaineer.streamreactor.common.converters.ParserImpl
import com.datamountaineer.streamreactor.connect.jms.config.JMSSetting
import com.github.os72.protocjar.Protoc
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.{DescriptorProtos, Descriptors, DynamicMessage, TypeRegistry}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.{JsonConverter, JsonConverterConfig}
import org.apache.kafka.connect.sink.SinkRecord

import java.io.{File, FileInputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import scala.collection.JavaConverters._

case class ProtoStoredAsConverter() extends ProtoConverter with StrictLogging {

  private val SCHEMA_PROTO_PATH = "proto_path"
  private val SCHEMA_PROTO_FILE = "proto_file"
  private val CONNECT_SINK_CONVERTER_PREFIX = "connect.sink.converter"

  private val CONNECT_SINK_CONVERTER_SCHEMA_CONFIG = CONNECT_SINK_CONVERTER_PREFIX + "." + SCHEMA_PROTO_PATH
  private val jsonConverter = new JsonConverter

  private val descriptors = new ConcurrentHashMap[String, Descriptors.Descriptor]

  private var defaultProtoPath: String = _

  private val BACK_QUOTE = "`"
  private val EMPTY = ""

  override def initialize(map: util.Map[String, String]): Unit = {
    defaultProtoPath = Option(map.get(CONNECT_SINK_CONVERTER_SCHEMA_CONFIG)).getOrElse(EMPTY)
    jsonConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false), false)
  }


  override def convert(record: SinkRecord, setting: JMSSetting): Array[Byte] = {
    val storedAs = setting.storedAs
      .replace(BACK_QUOTE, EMPTY)
    logger.debug(s"storedAs:  $storedAs")

    //Cache the descriptor lookup so not doing reflection on every record.
    val descriptor = descriptors.computeIfAbsent(storedAs, (name: String) => {
      val properties: util.Map[String, String] = mapAsJavaMap(setting.storedAsProperties)
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
          val protoFiles: util.Collection[String] = Files.find(basePath, Integer.MAX_VALUE, (filePath: Path, fileAttr: BasicFileAttributes) => fileAttr.isRegularFile)
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
      }
      catch {
        case x: Exception => logger.error("Invalid storedAs settings: " + x.getMessage)
        null
      }
    })

    if (descriptor == null) {
      throw new DataException("Invalid storedAs settings")
    }

    // As Protobuf is Positional based, yet Record is FieldName based,
    // we can convert to JSON and then back into Proto using JsonFormat which will match the FieldName to the Protobuf FieldName
    // This is safest for compatibility to be explicit though unfortunate the extra convert to JSON is not ideal.
    val json_converted_data = jsonConverter.fromConnectData(record.topic, record.valueSchema, record.value)
    val json = new String(json_converted_data, StandardCharsets.UTF_8)
    val b = DynamicMessage.newBuilder(descriptor)

    new ParserImpl(TypeRegistry.getEmptyTypeRegistry, JsonFormat.TypeRegistry.getEmptyTypeRegistry, true, 100)
      .merge(json, b)

    JsonFormat.printer
      .print(b)
      .getBytes()
  }

  private def getDescriptor(message: String, protoPath: String, protoFiles: util.Collection[String]): Descriptors.Descriptor = {
    protoFiles.asScala
      .flatMap(file => Option(getDescriptor(message, protoPath, file)))
      .headOption
      .orNull
  }

  private def getDescriptor(message: String, protoPath: String, protoFile: String): Descriptors.Descriptor = try {
    val descFile: File = generateDescFile(protoPath, protoFile)
    val fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(new FileInputStream(descFile.getAbsolutePath))
    val fileDescriptorProto = fileDescriptorSet.getFileList.stream.
      filter((fdp: DescriptorProtos.FileDescriptorProto) => fdp.getName == protoFile)
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
    }
    else {
      logger.error(s"File descriptor name=$message doesn't match with proto file name=$protoFile")
      null
    }
  } catch {
    case x@(_: IOException | _: InterruptedException) =>
      logger.error("Unexpected error", x.getMessage)
      null
  }

  private def generateDescFile(protoPath: String, protoFile: String) = {
    val descFile = File.createTempFile(protoFile, ".desc")
    val args2 = Array("--include_std_types",
      "--proto_path=" + protoPath,
      "--descriptor_set_out=" + descFile.getAbsolutePath,
      "--include_imports",
      protoPath + File.separator + protoFile)

    Protoc.runProtoc(args2)
    descFile
  }

  private def buildFileDescriptor(proto: DescriptorProtos.FileDescriptorProto, set: DescriptorProtos.FileDescriptorSet): Descriptors.FileDescriptor = {
    val fileProtoCache = new util.HashMap[String, DescriptorProtos.FileDescriptorProto]
    set.getFileList.forEach((file: DescriptorProtos.FileDescriptorProto) => fileProtoCache.put(file.getName, file))
    buildFileDescriptor(proto, fileProtoCache)
  }

  private def buildFileDescriptor(currentFileProto: DescriptorProtos.FileDescriptorProto, fileProtoCache: util.Map[String, DescriptorProtos.FileDescriptorProto]): Descriptors.FileDescriptor = {
    val dependencyFileDescriptorList = new util.ArrayList[Descriptors.FileDescriptor]
    currentFileProto.getDependencyList.forEach((dependencyStr: String) => {
      {
        val dependencyFileProto = fileProtoCache.get(dependencyStr)
        val dependencyFileDescriptor: Descriptors.FileDescriptor = buildFileDescriptor(dependencyFileProto, fileProtoCache)
        dependencyFileDescriptorList.add(dependencyFileDescriptor)
      }
    })
    try Descriptors.FileDescriptor.buildFrom(currentFileProto, dependencyFileDescriptorList.toArray(new Array[Descriptors.FileDescriptor](0)))
    catch {
      case e: Descriptors.DescriptorValidationException =>
        throw new IllegalStateException("FileDescriptor build fail!", e)
    }
  }

}
