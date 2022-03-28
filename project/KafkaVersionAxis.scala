import Dependencies.{confluentAvroConverter, jacksonDatabind, jacksonModuleScala, json4sJackson, json4sNative}
import sbt.Keys._
import sbt.VirtualAxis._
import sbt.internal.ProjectMatrix
import sbt._
import Settings.E2ETest

//2.8.1, 3.1.0

case class KafkaVersionAxis(kafkaVersion: String) extends WeakAxis {

  private val json4sVersion : String = kafkaVersion match {
    //case "2.8.1" => "3.6.7"
    //case "3.1.0" => "4.0.4"
    case _ => "4.0.4"
  }

  private val confluentPlatformVersion: String = kafkaVersion match {
    case "2.8.1" => "6.2.2"
    case "3.1.0" => "7.0.1"
    case _ => throw new IllegalStateException("unexpected kafka version")
  }

  private val jacksonVersion: String = kafkaVersion match {
    case "2.8.1" => "2.10.5"
    case "3.1.0" => "2.12.6"
    case _ => throw new IllegalStateException("unexpected kafka version")
  }

  private val kafkaVersionCompat: String = kafkaVersion.split("\\.", 3).take(2).mkString("-")

  override val directorySuffix = s"-kafka-${kafkaVersionCompat}"

  override val idSuffix: String = directorySuffix.replaceAll("\\W+", "-")

  def deps(): Seq[ModuleID] = Seq(
    json4sNative(json4sVersion),
    json4sJackson(json4sVersion),
    jacksonDatabind(jacksonVersion),
    jacksonModuleScala(jacksonVersion),
    confluentAvroConverter(confluentPlatformVersion)
  )

  def ideEnable() : Boolean = kafkaVersion == "3.1.0"
}

object KafkaVersionAxis {

  implicit class ProjectExtension(val p: ProjectMatrix) extends AnyVal {

    def isScala2_13(axes: Seq[VirtualAxis]): Boolean = {
      axes.collectFirst { case ScalaVersionAxis(_, scalaVersionCompat) => scalaVersionCompat }.forall(_ == "2.13")
    }

    def kafka2Row( settings: Def.SettingsDefinition*): ProjectMatrix = {
      kafkaRow(KafkaVersionAxis("2.8.1"), scalaVersions = Seq("2.13.8"), settings:_*)
    }

    def kafka3Row( settings: Def.SettingsDefinition*): ProjectMatrix = {
      kafkaRow(KafkaVersionAxis("3.1.0"), scalaVersions = Seq("2.13.8"), settings:_*)
    }

    def kafkaRow(kafkaVersionAxis: KafkaVersionAxis, scalaVersions: Seq[String], settings: Def.SettingsDefinition*): ProjectMatrix =
      p.customRow(
        scalaVersions = scalaVersions,
        axisValues = Seq(kafkaVersionAxis, VirtualAxis.jvm),
        _
          .settings(
            E2ETest / envVars := Map("KAFKA_VERSION_DIRECTORY_SUFFIX" -> kafkaVersionAxis.directorySuffix),
            name := name.value + kafkaVersionAxis.directorySuffix,
            moduleName := moduleName.value + kafkaVersionAxis.directorySuffix,
            libraryDependencies ++= kafkaVersionAxis.deps(),
            dependencyOverrides ++= kafkaVersionAxis.deps()
          )
          .settings(settings: _*)
      )
  }
}