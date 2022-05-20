import Dependencies._
import Settings.FunctionalTest
import sbt.Keys._
import sbt.VirtualAxis._
import sbt._
import sbt.internal.ProjectMatrix

//2.8.1, 3.1.0

case class KafkaVersionAxis(kafkaVersion: String) extends WeakAxis {

  val confluentPlatformVersion: String = kafkaVersion match {
    case "2.8.1" => "6.2.2"
    case "3.1.0" => "7.0.1"
    case _       => throw new IllegalStateException("unexpected kafka version")
  }

  private val jacksonVersion: String = kafkaVersion match {
    case "2.8.1" => "2.10.5"
    case "3.1.0" => "2.12.6"
    case _       => throw new IllegalStateException("unexpected kafka version")
  }

  private val jacksonDatabindVersion: String = if (jacksonVersion == "2.12.6") "2.12.6.1" else "2.10.5.1"

  private val kafkaVersionCompat: String = kafkaVersion.split("\\.", 3).take(2).mkString("-")

  override val directorySuffix = s"-kafka-$kafkaVersionCompat"

  override val idSuffix: String = directorySuffix.replaceAll("\\W+", "-")

  def testDeps(): Seq[ModuleID] = Seq(
    kafkaClients(kafkaVersion) % Test,
  )

  def deps(): Seq[ModuleID] = Seq(
    kafkaConnectJson(kafkaVersion),
    confluentAvroConverter(confluentPlatformVersion),
    confluentJsonSchemaSerializer(confluentPlatformVersion),
    jacksonDatabind(jacksonDatabindVersion),
    jacksonModuleScala(jacksonVersion),
  )

  def fixedDeps(): Seq[ModuleID] = Seq(
    jacksonCore(jacksonVersion),
    jacksonDatabind(jacksonDatabindVersion),
    jacksonDataformatCbor(jacksonVersion),
    jacksonModuleScala(jacksonVersion),
    confluentProtobufConverter(confluentPlatformVersion),
  )

  def ideEnable(): Boolean = kafkaVersion == "3.1.0"
}

object KafkaVersionAxis {

  implicit class ProjectExtension(val p: ProjectMatrix) extends AnyVal {

    def isScala2_13(axes: Seq[VirtualAxis]): Boolean =
      axes.collectFirst { case ScalaVersionAxis(_, scalaVersionCompat) => scalaVersionCompat }.forall(_ == "2.13")

    def kafka2Row(settings: Def.SettingsDefinition*): ProjectMatrix =
      kafkaRow(KafkaVersionAxis("2.8.1"), scalaVersions = Seq("2.13.8"), settings: _*)

    def kafka3Row(settings: Def.SettingsDefinition*): ProjectMatrix =
      kafkaRow(KafkaVersionAxis("3.1.0"), scalaVersions = Seq("2.13.8"), settings: _*)

    def kafkaRow(
      kafkaVersionAxis: KafkaVersionAxis,
      scalaVersions:    Seq[String],
      settings:         Def.SettingsDefinition*,
    ): ProjectMatrix =
      p.customRow(
        scalaVersions = scalaVersions,
        axisValues    = Seq(kafkaVersionAxis, VirtualAxis.jvm),
        _
          .settings(
            FunctionalTest / envVars := Map(
              "KAFKA_VERSION_DIRECTORY_SUFFIX" -> kafkaVersionAxis.directorySuffix,
              "CONFLUENT_VERSION"              -> kafkaVersionAxis.confluentPlatformVersion,
            ),
            name := name.value + kafkaVersionAxis.directorySuffix,
            moduleName := moduleName.value + kafkaVersionAxis.directorySuffix,
            libraryDependencies ++= kafkaVersionAxis.deps(),
            libraryDependencies ++= kafkaVersionAxis.testDeps(),
            dependencyOverrides ++= kafkaVersionAxis.fixedDeps(),
          )
          .settings(settings: _*),
      )
  }
}
