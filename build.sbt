import Dependencies.globalExcludeDeps
import Dependencies.gson
import Dependencies.googleProtobuf

import Settings._
import sbt.Keys.libraryDependencies
import sbt._
import sbt.Project.projectToLocalProject

import java.io.File

ThisBuild / scalaVersion := Dependencies.scalaVersion

lazy val subProjects: Seq[Project] = Seq(
  common,
  `aws-s3`,
  `azure-documentdb`,
  cassandra,
  elastic6,
  elastic7,
  ftp,
  hazelcast,
  hbase,
  hive,
  influxdb,
  jms,
  kudu,
  mongodb,
  mqtt,
  pulsar,
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

lazy val common = (project in file("kafka-connect-common"))
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-common",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps,
        publish / skip := true,
      ),
  )
  .configureAssembly()
  .configureTests(baseTestDeps)

lazy val `aws-s3` = (project in file("kafka-connect-aws-s3"))
  .dependsOn(common)
  .dependsOn(`test-common` % "fun->compile")
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-aws-s3",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectS3Deps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectS3TestDeps)
  .configureFunctionalTests(kafkaConnectS3FuncTestDeps)
  .enablePlugins(PackPlugin)

lazy val `azure-documentdb` = (project in file("kafka-connect-azure-documentdb"))
  .dependsOn(common)
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
  .configureAssembly()
  .configureTests(baseTestDeps)
  .enablePlugins(PackPlugin)

lazy val cassandra = (project in file("kafka-connect-cassandra"))
  .dependsOn(common)
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
  .configureAssembly()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectCassandraTestDeps)
  .configureFunctionalTests()
  .enablePlugins(PackPlugin)

lazy val elastic6 = (project in file("kafka-connect-elastic6"))
  .dependsOn(common)
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
  .configureAssembly()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectElastic6TestDeps)
  .configureFunctionalTests()
  .enablePlugins(PackPlugin)

lazy val elastic7 = (project in file("kafka-connect-elastic7"))
  .dependsOn(common)
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
  .configureAssembly()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectElastic7TestDeps)
  .configureFunctionalTests()
  .enablePlugins(PackPlugin)

lazy val hazelcast = (project in file("kafka-connect-hazelcast"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-hazelcast",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectHazelCastDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly()
  .configureTests(baseTestDeps)
  .disableParallel()
  .enablePlugins(PackPlugin)

lazy val influxdb = (project in file("kafka-connect-influxdb"))
  .dependsOn(common)
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
  .configureAssembly()
  .configureTests(baseTestDeps)
  .enablePlugins(PackPlugin)

lazy val jms = (project in file("kafka-connect-jms"))
  .dependsOn(common)
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
          PB.gens.java -> (Test / sourceManaged).value,
        ),
      ),
  )
  .configureAssembly()
  .configureTests(kafkaConnectJmsTestDeps)
  .configureIntegrationTests(kafkaConnectJmsTestDeps)
  //.configureFunctionalTests(kafkaConnectS3FuncTestDeps)
  .disableParallel()
  .enablePlugins(PackPlugin, ProtocPlugin)

lazy val kudu = (project in file("kafka-connect-kudu"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-kudu",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectKuduDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly()
  .configureTests(baseTestDeps)
  .enablePlugins(PackPlugin)

lazy val mqtt = (project in file("kafka-connect-mqtt"))
  .dependsOn(common)
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
  .configureAssembly()
  .configureTests(baseTestDeps)
  .configureFunctionalTests()
  .configureIntegrationTests(kafkaConnectMqttTestDeps)
  .disableParallel()
  .enablePlugins(PackPlugin)

lazy val pulsar = (project in file("kafka-connect-pulsar"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-pulsar",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectPulsarDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly()
  .configureTests(baseTestDeps)
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
  .configureAssembly()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectFtpTestDeps)
  .enablePlugins(PackPlugin)

lazy val hbase = (project in file("kafka-connect-hbase"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-hbase",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectHbaseDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly()
  .configureTests(baseTestDeps)
  .enablePlugins(PackPlugin)

lazy val hive = (project in file("kafka-connect-hive"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-hive",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectHiveDeps,
        publish / skip := true,
        packExcludeJars := Seq(
          "scala-.*\\.jar",
          "zookeeper-.*\\.jar",
        ),
      ),
  )
  .configureAssembly()
  .configureTests(kafkaConnectHiveTestDeps)
  .enablePlugins(PackPlugin)

lazy val mongodb = (project in file("kafka-connect-mongodb"))
  .dependsOn(common)
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
  .configureAssembly()
  .configureTests(baseTestDeps)
  .configureIntegrationTests(kafkaConnectMongoDbTestDeps)
  .configureFunctionalTests()
  .enablePlugins(PackPlugin)

lazy val redis = (project in file("kafka-connect-redis"))
  .dependsOn(common)
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
  .configureAssembly()
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
  ";headerCheck;test:headerCheck;it:headerCheck;fun:headerCheck;scalafmtCheckAll;",
)
addCommandAlias(
  "formatAll",
  ";headerCreateAll;scalafmtAll;scalafmtSbt;",
)
addCommandAlias("fullTest", ";test;it:test;fun:test")
addCommandAlias("fullCoverageTest", ";coverage;test;it:test;coverageReport;coverageAggregate")

dependencyCheckFormats := Seq("XML", "HTML")
dependencyCheckNodeAnalyzerEnabled := Some(false)
dependencyCheckNodeAuditAnalyzerEnabled := Some(false)
dependencyCheckNPMCPEAnalyzerEnabled := Some(false)
dependencyCheckRetireJSAnalyzerEnabled := Some(false)

excludeDependencies ++= globalExcludeDeps

val generateModulesList    = taskKey[Seq[File]]("generateModulesList")
val generateItModulesList  = taskKey[Seq[File]]("generateItModulesList")
val generateFunModulesList = taskKey[Seq[File]]("generateFunModulesList")

Compile / generateModulesList :=
  new FileWriter(subProjects).generate((Compile / resourceManaged).value / "modules.txt")
Compile / generateItModulesList :=
  new FileWriter(
    subProjects.filter(p => p.containsDir("src/it")),
  ).generate((Compile / resourceManaged).value / "it-modules.txt")
Compile / generateFunModulesList :=
  new FileWriter(
    subProjects.filter(p => p.containsDir("src/fun")),
  ).generate((Compile / resourceManaged).value / "fun-modules.txt")

Compile / resourceGenerators += (Compile / generateModulesList)
Compile / resourceGenerators += (Compile / generateItModulesList)
Compile / resourceGenerators += (Compile / generateFunModulesList)

conflictManager := ConflictManager.strict
