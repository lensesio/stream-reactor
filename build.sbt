import Dependencies.Versions
import Dependencies.`cyclopsPure`
import Dependencies.`cyclops`
import Dependencies.`lombok`
import Dependencies.globalExcludeDeps
import Dependencies.gson
import Settings.*
import sbt.Keys.libraryDependencies
import sbt.*
import sbt.Project.projectToLocalProject
import sbt.Project.projectToRef

import java.io.File
import scala.sys.process.*

ThisBuild / scalaVersion := Dependencies.scalaVersion

lazy val subProjects: Seq[Project] = Seq(
  `test-utils`,
  `query-language`,
  `java-common`,
  `sink-reporting`,
  `gcp-common`,
  common,
  `sql-common`,
  `cloud-common`,
  `aws-s3`,
  `azure-documentdb`,
  `azure-datalake`,
  cassandra,
  elastic6,
  elastic7,
  ftp,
  `gcp-storage`,
  http,
  influxdb,
  jms,
  mongodb,
  mqtt,
  redis,
)

lazy val subProjectsRefs: Seq[ProjectReference] = subProjects.map(projectToLocalProject)

lazy val root = (project in file("."))
  .settings(
    publish := {},
    publishArtifact := false,
    name := "stream-reactor",
  )
  .aggregate(
    subProjectsRefs: _*,
  )
  .disablePlugins(AssemblyPlugin, HeaderPlugin)

lazy val `test-utils` = (project in file("java-connectors/test-utils"))
  .settings(
    settings ++
      Seq(
        name := "test-utils",
        description := "Java utils for connector testing",
        libraryDependencies ++= javaCommonDeps,
        publish / skip := true,
      ),
  )
  .configureAssembly(false)
  .configureTests(javaCommonTestDeps)

lazy val `query-language` = (project in file("java-connectors/kafka-connect-query-language"))
  .dependsOn(`test-utils` % "test->test")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-query-language",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= Seq(cyclops, cyclopsPure, lombok),
        publish / skip := true,
      ),
  )
  .configureAssembly(true)
  .configureTests(javaCommonTestDeps)
  .configureAntlr()

lazy val `java-common` = (project in file("java-connectors/kafka-connect-common"))
  .dependsOn(`test-utils` % "test->test")
  .dependsOn(`query-language`)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-java-common",
        description := "Common components from java",
        libraryDependencies ++= javaCommonDeps,
        publish / skip := true,
      ),
  )
  .configureAssembly(false)
  .configureTests(javaCommonTestDeps)

lazy val `sink-reporting` = (project in file("java-connectors/kafka-connect-sink-reporting"))
  .dependsOn(`java-common`)
  .dependsOn(`test-utils` % "test->test")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-sink-reporting",
        description := "Common reporting components from java",
        libraryDependencies ++= javaCommonDeps,
        publish / skip := true,
      ),
  )
  .configureAssembly(false)
  .configureTests(javaCommonTestDeps)

lazy val `gcp-common` = (project in file("java-connectors/kafka-connect-gcp-common"))
  .dependsOn(`java-common`)
  .dependsOn(`test-utils` % "test->test")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-gcp-common",
        description := "GCP Commons Module",
        libraryDependencies ++= kafkaConnectGcpCommonDeps,
        publish / skip := true,
      ),
  )
  .configureAssembly(true)
  .configureTests(javaCommonTestDeps)
  .configureAntlr()

lazy val `sql-common` = (project in file("kafka-connect-sql-common"))
  .dependsOn(`query-language`)
  .dependsOn(`common`)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-sql-common",
        description := "Common SQL Components required by some connectors",
        libraryDependencies ++= sqlCommonDeps,
        publish / skip := true,
      ),
  )
  .configureAssembly(true)
  .configureTests(baseTestDeps)

lazy val common = (project in file("kafka-connect-common"))
  .dependsOn(`query-language`)
  .dependsOn(`java-common`)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-common",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps,
        publish / skip := true,
      ),
  )
  .configureAssembly(true)
  .configureTests(baseTestDeps)

lazy val `cloud-common` = (project in file("kafka-connect-cloud-common"))
  .dependsOn(common)
  //.dependsOn(`test-common` % "fun->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-cloud-common",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCloudCommonDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureTests(baseTestDeps)
  .enablePlugins(PackPlugin)

lazy val `aws-s3` = (project in file("kafka-connect-aws-s3"))
  .dependsOn(common)
  .dependsOn(`cloud-common` % "compile->compile;test->test;it->it")
  .dependsOn(`test-common` % "fun->compile;it->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-aws-s3",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCloudCommonDeps ++ kafkaConnectS3Deps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectS3TestDeps)
  .configureFunctionalTests(kafkaConnectS3FuncTestDeps)
  .enablePlugins(PackPlugin)

lazy val `azure-datalake` = (project in file("kafka-connect-azure-datalake"))
  .dependsOn(common)
  .dependsOn(`cloud-common` % "compile->compile;test->test")
  .dependsOn(`test-common` % "test->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-azure-datalake",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCloudCommonDeps ++ kafkaConnectAzureDatalakeDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  //.configureIntegrationTests(kafkaConnectAzureDatalakeTestDeps)
  //.configureFunctionalTests(kafkaConnectAzureDatalakeFuncTestDeps)
  .enablePlugins(PackPlugin)

lazy val `gcp-storage` = (project in file("kafka-connect-gcp-storage"))
  .dependsOn(common)
  .dependsOn(`gcp-common`)
  .dependsOn(`cloud-common` % "compile->compile;test->test;it->it")
  .dependsOn(`test-common` % "test->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-gcp-storage",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCloudCommonDeps ++ kafkaConnectGcpStorageDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(false)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectGcpStorageTestDeps)
  //.configureFunctionalTests(kafkaConnectAzureDatalakeFuncTestDeps)
  .enablePlugins(PackPlugin)

lazy val `azure-documentdb` = (project in file("kafka-connect-azure-documentdb"))
  .dependsOn(common)
  .dependsOn(`sql-common`)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-azure-documentdb",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectAzureDocumentDbDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  .enablePlugins(PackPlugin)

lazy val cassandra = (project in file("kafka-connect-cassandra"))
  .dependsOn(common)
  .dependsOn(`sql-common`)
  .dependsOn(`test-common` % "fun->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-cassandra",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCassandraDeps,
        publish / skip := true,
        FunctionalTest / baseDirectory := (LocalRootProject / baseDirectory).value,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectCassandraTestDeps)
  .configureFunctionalTests()
  .enablePlugins(PackPlugin)

lazy val elastic6 = (project in file("kafka-connect-elastic6"))
  .dependsOn(common)
  .dependsOn(`sql-common`)
  .dependsOn(`test-common` % "fun->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-elastic6",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectElastic6Deps,
        publish / skip := true,
        FunctionalTest / baseDirectory := (LocalRootProject / baseDirectory).value,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectElastic6TestDeps)
  .configureFunctionalTests()
  .enablePlugins(PackPlugin)

lazy val elastic7 = (project in file("kafka-connect-elastic7"))
  .dependsOn(common)
  .dependsOn(`sql-common`)
  .dependsOn(`test-common` % "fun->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-elastic7",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectElastic7Deps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectElastic7TestDeps)
  .configureFunctionalTests()
  .enablePlugins(PackPlugin)

lazy val http = (project in file("kafka-connect-http"))
  .dependsOn(common)
  .dependsOn(`sink-reporting`)
  .dependsOn(`test-common` % "fun->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-http",
        description := "Kafka Connect compatible connectors to move data between Kafka and http",
        libraryDependencies ++= baseDeps ++ kafkaConnectHttpDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(false)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps ++ kafkaConnectHttpTestDeps)
  .configureIntegrationTests(baseTestDeps ++ kafkaConnectHttpTestDeps)
  .configureFunctionalTests()
  .enablePlugins(PackPlugin, ProtocPlugin)

lazy val influxdb = (project in file("kafka-connect-influxdb"))
  .dependsOn(common)
  .dependsOn(`sql-common`)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-influxdb",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectInfluxDbDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  .enablePlugins(PackPlugin)

lazy val jms = (project in file("kafka-connect-jms"))
  .dependsOn(common)
  .dependsOn(`sql-common`)
  //.dependsOn(`test-common` % "fun->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-jms",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectJmsDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
        Compile / PB.protoSources := Seq(sourceDirectory.value / "test" / "resources" / "example"),
        Compile / PB.targets := Seq(
          PB.gens.java(Versions.googleProtobufVersion) -> (Test / sourceManaged).value,
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(kafkaConnectJmsTestDeps)
  .configureIntegrationTests(kafkaConnectJmsTestDeps)
  .disableParallel()
  .enablePlugins(PackPlugin, ProtocPlugin)

lazy val mqtt = (project in file("kafka-connect-mqtt"))
  .dependsOn(common)
  .dependsOn(`sql-common`)
  .dependsOn(`test-common` % "fun->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-mqtt",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectMqttDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  .configureFunctionalTests()
  .configureIntegrationTests(kafkaConnectMqttTestDeps)
  .disableParallel()
  .enablePlugins(PackPlugin)

lazy val ftp = (project in file("kafka-connect-ftp"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-ftp",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectFtpDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectFtpTestDeps)
  .enablePlugins(PackPlugin)

lazy val mongodb = (project in file("kafka-connect-mongodb"))
  .dependsOn(common)
  .dependsOn(`sql-common`)
  .dependsOn(`test-common` % "test->compile;it->compile;fun->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-mongodb",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectMongoDbDeps,
        publish / skip := true,
        FunctionalTest / baseDirectory := (LocalRootProject / baseDirectory).value,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectMongoDbTestDeps)
  .configureFunctionalTests()
  .enablePlugins(PackPlugin)

lazy val redis = (project in file("kafka-connect-redis"))
  .dependsOn(common)
  .dependsOn(`sql-common`)
  .dependsOn(`test-common` % "fun->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-redis",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectRedisDeps,
        publish / skip := true,
        FunctionalTest / baseDirectory := (LocalRootProject / baseDirectory).value,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly(true)
  .configureMavenDescriptor()
  .configureTests(baseTestDeps ++ Seq(gson))
  .configureIntegrationTests(kafkaConnectRedisTestDeps)
  .configureFunctionalTests()
  .enablePlugins(PackPlugin)

lazy val `test-common` = (project in file("test-common"))
  .settings(
    settings ++
      Seq(
        name := "test-common",
        libraryDependencies ++= testCommonDeps,
      ),
  )
  .disablePlugins(AssemblyPlugin)

addCommandAlias(
  "validateAll",
  "headerCheck;test:headerCheck;it:headerCheck;fun:headerCheck;scalafmtCheckAll;test-common/scalafmtCheck;test-common/headerCheck",
)

lazy val gradleSpotlessApply = taskKey[Unit]("Run 'gradlew spotlessApply' via external process")
gradleSpotlessApply := {
  // Specify the desired working directory for the external process
  val targetDirectory = baseDirectory.value / "java-connectors"

  // Execute './gradle spotlessApply' in the specified directory
  val exitCode = Process("./gradlew spotlessApply", targetDirectory).!

  if (exitCode != 0) {
    throw new RuntimeException("gradlew spotlessApply command failed")
  }
}

addCommandAlias(
  "formatAll",
  ";headerCreateAll;scalafmtAll;scalafmtSbt;test-common/scalafmt;test-common/headerCreateAll;gradleSpotlessApply",
)

addCommandAlias("fullTest", ";test;it:test;fun:test")
addCommandAlias("fullCoverageTest", ";coverage;test;it:test;coverageReport;coverageAggregate")

excludeDependencies ++= globalExcludeDeps

val generateModulesList         = taskKey[Seq[File]]("generateModulesList")
val generateItModulesList       = taskKey[Seq[File]]("generateItModulesList")
val generateFunModulesList      = taskKey[Seq[File]]("generateFunModulesList")
val generateDepCheckModulesList = taskKey[Seq[File]]("generateDepCheckModulesList")
val generatePublishModulesList  = taskKey[Seq[File]]("generatePublishModulesList")

Compile / generateModulesList :=
  new FileWriter(subProjects).generate((Compile / resourceManaged).value / "modules.txt")
Compile / generateDepCheckModulesList :=
  new FileWriter(subProjects.filter(sp => !sp.base.asPath.startsWith("java-connectors/")))
    .generate((Compile / resourceManaged).value / "depcheck-modules.txt")
Compile / generateItModulesList :=
  new FileWriter(
    subProjects.filter(p => p.containsDir("src/it")),
  ).generate((Compile / resourceManaged).value / "it-modules.txt")
Compile / generateFunModulesList :=
  new FileWriter(
    subProjects.filter(p => p.containsDir("src/fun")),
  ).generate((Compile / resourceManaged).value / "fun-modules.txt")

Compile / generatePublishModulesList := {

  val nonPublishableModulesFragments = Set("common", "utils", "query-language", "test", "reporting")
  val publishableModules = subProjects
    .filterNot(project => nonPublishableModulesFragments.exists(ignore => project.id.contains(ignore)))

  val outputFile = (Compile / resourceManaged).value / "publish-modules.txt"
  new FileWriter(publishableModules).generate(outputFile)
}
Compile / resourceGenerators += (Compile / generateModulesList)
Compile / resourceGenerators += (Compile / generateItModulesList)
Compile / resourceGenerators += (Compile / generateFunModulesList)
Compile / resourceGenerators += (Compile / generatePublishModulesList)

conflictManager := ConflictManager.strict
