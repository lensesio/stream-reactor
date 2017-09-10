/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.helm

import java.io.{File, PrintWriter}

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.commons.lang.StringUtils
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.ConfigKey

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 31/08/2017.
  * stream-reactor
  */
object HelmHelper {


  def generateHelmChart(name: String, config: ConfigDef, sink: Boolean, version: String) = {

    // Get the chart name from the first to parts of the config keys
    val keys = config.configKeys().asScala.toMap
    val chartName = if (name.contains("elastic5")){
      "kafka-connect-elastic5s-sink"
    } else if (name.contains("FtpSourceConnector")) {
      "kafka-connect-ftp-source"
    } else if (name.contains("kudu")) {
      "kafka-connect-kudu-sink"
    } else {
      s"kafka-${keys.head._2.name.split("\\.").take(2).mkString("-")}${if (sink) "-sink" else "-source"}"
    }

    println(s"Generating chart $chartName")
    generateValues(keys, chartName, version)
    generateTemplate(keys, chartName, version)
    copyHelpers(chartName)
    generateChartDesc(chartName, version)
  }

  def generateConfigMap(name: String) = {
    val templateDir = new File(s"charts/$name/templates")
    templateDir.mkdirs()
    val writer = new PrintWriter(s"$templateDir/configmap.yaml")
    val base = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/configmap.yaml")).mkString
    writer.println(base.toString)
    writer.close()
  }

  def generateChartDesc(name: String, version: String) = {
    val templateDir = new File(s"charts/$name")
    templateDir.mkdirs()
    val writer = new PrintWriter(s"$templateDir/Chart.yaml")
    val base = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/Chart.yaml")).mkString
    val out = base.replace("<NAME>", name).replace("<VERSION>", version)
    writer.println(out)
    writer.close()
  }

  def copyHelpers(name: String) = {
    val templateDir = new File(s"charts/$name/templates")
    templateDir.mkdirs()
    val writer = new PrintWriter(s"$templateDir/_helpers.tpl")
    val base = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/_helpers.tpl")).mkString
    writer.println(base)
    writer.close()
  }

  def generateTemplate(keys: Map[String, ConfigKey], chartsName: String, version: String) = {
    val templateDir = new File(s"charts/$chartsName/templates")
    templateDir.mkdirs()
    val writer = new PrintWriter(s"$templateDir/deployment.yaml")
    val base = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/deployment.yaml")).mkString
    writer.println(base)
    writer.println()

    keys
        .map({
          case (name, key) => {
            val envName = toENV(name)
            val helmValName = toCamelCase(name)

            //  Create config map for avro schemas
            if (helmValName.equals("avroSchemas")) generateConfigMap(chartsName)

            writer.println(s"        - name: $envName")
            if (key.`type`.equals(ConfigDef.Type.PASSWORD)) {
              val secret = s"          valueFrom:\n            secretKeyRef:\n              name: {{ .Values.secretsRef | quote }}\n              key:  {{ .Values.${helmValName}Key }}"
              writer.println(secret)
            } else {
              writer.println(s"          value: {{ .Values.${helmValName} | quote }}")
            }
          }
        })
    writer.close()
  }

  def generateValues(keys: Map[String, ConfigKey], name: String, version: String) = {
    val chartDir = new File(s"charts/$name")
    chartDir.mkdirs()
    val writer = new PrintWriter(s"$chartDir/values.yaml")
    val base = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/values.yaml")).mkString
    writer.println(base.replace("<NAME>", name).replace("<VERSION>", version))
    keys.map({
        case (name, key) =>
          {
            val helmName = toCamelCase(name)
            writer.println(getDescriptionComments(helmName, key))
            writer.println(getHelmValue(helmName, key))
            writer.println
          }
    })
    writer.close()
  }

  def getDescriptionComments(name: String, key: ConfigKey) = {
    s"# ${name} ${key.documentation.replaceAll("\\n", "\n# ")} type: ${key.`type`.toString} importance: ${key.importance}"
  }

  def getHelmValue(name: String, key: ConfigKey) = {
    val default = if (!key.hasDefault){
      "\"__REQUIRED__\""
    } else if (key.`type`.equals(ConfigDef.Type.PASSWORD) && (name.contains("keyStorePassword") || name.contains("trustStorePassword"))) {
      ""
    } else if (key.`type`.equals(ConfigDef.Type.PASSWORD)) {
      "\"__REQUIRED__\""
    } else {
      val str = ConfigDef.convertToString(key.defaultValue, key.`type`)
      if (str == null) "" else str
    }

    s"$name: $default"
  }

  def toCamelCase(name: String): String = {
    val drop = if (name.contains("ftp")) name.split("\\.") else name.split("\\.").drop(2)
    val ret = new StringBuilder(drop.mkString.length)

    drop
      .foreach(p => {
        ret.append(p.substring(0, 1).toUpperCase)
        ret.append(p.substring(1).toLowerCase)
      })

    if (ret.length > 0) {
      ret.toString.substring(0, 1).toLowerCase() + ret.toString().substring(1)
    } else {
      ret.toString()
    }
  }

  def toENV(prop: String): String = StringUtils.replace(prop.toUpperCase(), ".", "_")
}
