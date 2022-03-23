import Dependencies.globalExcludeDeps
import KafkaVersionAxis.ProjectExtension
import sbt._
import Settings._
import sbt.internal.ProjectMatrix.projectMatrixToLocalProjectMatrix
import sbt.internal.{ProjectMatrix, ProjectMatrixReference}

ThisBuild / scalaVersion := "2.13.8"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")


lazy val subProjects: Seq[ProjectMatrix] = Seq(
  common,
  awsS3,
  azureDocumentDb,
  cassandra,
  coap,
  elastic6,
  elastic7,
  ftp,
  hazelCast,
  hbase,
  hive,
  influx,
  jms,
  kudu,
  mongoDb,
  mqtt,
  pulsar,
  redis
)
lazy val subProjectsRefs: Seq[ProjectMatrixReference] = subProjects.map(projectMatrixToLocalProjectMatrix)



lazy val root = (projectMatrix in file("."))
  .settings(
    publish := {},
    publishArtifact := false,
    name := "stream-reactor"
  )
  .aggregate(
    subProjectsRefs:_*
  )

lazy val common = (projectMatrix in file("kafka-connect-common"))
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-common",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCommonDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(baseTestDeps)

lazy val awsS3 = (projectMatrix in file("kafka-connect-aws-s3"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-aws-s3",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectS3Deps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectS3TestDeps)

lazy val azureDocumentDb = (projectMatrix in file("kafka-connect-azure-documentdb"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-azure-documentdb",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectAzureDocumentDbDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(baseTestDeps)

lazy val coap = (projectMatrix in file("kafka-connect-coap"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-coap",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCoapDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(baseTestDeps)

lazy val cassandra = (projectMatrix in file("kafka-connect-cassandra"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-cassandra",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCassandraDeps,
        publish / skip := true,
     )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectCassandraTestDeps)

lazy val elastic6 = (projectMatrix in file("kafka-connect-elastic6"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-elastic6",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectElastic6Deps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectElastic6TestDeps)

lazy val elastic7 = (projectMatrix in file("kafka-connect-elastic7"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-elastic7",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectElastic7Deps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectElastic7TestDeps)

lazy val hazelCast = (projectMatrix in file("kafka-connect-hazelcast"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-hazelcast",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectHazelCastDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(baseTestDeps)

lazy val influx = (projectMatrix in file("kafka-connect-influxdb"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-influxdb",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectInfluxDbDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(baseTestDeps)

lazy val jms = (projectMatrix in file("kafka-connect-jms"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-jms",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectJmsDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectJmsTestDeps)
  .disableParallel()

lazy val kudu = (projectMatrix in file("kafka-connect-kudu"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-kudu",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectKuduDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(baseTestDeps)

lazy val mqtt = (projectMatrix in file("kafka-connect-mqtt"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-mqtt",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectMqttDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectMqttTestDeps)
  .disableParallel()

lazy val pulsar = (projectMatrix in file("kafka-connect-pulsar"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-pulsar",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectPulsarDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(baseTestDeps)

lazy val ftp = (projectMatrix in file("kafka-connect-ftp"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-ftp",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectFtpDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectFtpTestDeps)
  .disableParallel()

lazy val hbase = (projectMatrix in file("kafka-connect-hbase"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-hbase",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectHbaseDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectHbaseTestDeps)

lazy val hive = (projectMatrix in file("kafka-connect-hive"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-hive",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectHiveDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectHiveTestDeps)

lazy val mongoDb = (projectMatrix in file("kafka-connect-mongodb"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-mongodb",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectMongoDbDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectMongoDbTestDeps)

lazy val redis =  (projectMatrix in file("kafka-connect-redis"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-redis",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectRedisDeps,
        publish / skip := true,
      )
  )
  .kafka2Row()
  .kafka3Row()
  .configureAssembly()
  .configureTests(kafkaConnectRedisTestDeps)

/*lazy val testContainers = (projectMatrix in file("kafka-connect-testcontainers"))
  .dependsOn(cassandra, elastic6, mongoDb, redis)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-testcontainers",
        description := "Integration testing project",
      )
  )
  .kafka2Row()
  .kafka3Row()
  .settings(libraryDependencies ++= kafkaConnectTestContainersDeps)*/

addCommandAlias(
  "validateAll",
  ";headerCheck;test:headerCheck;fun:headerCheck;it:headerCheck;scalafmtCheck;test:scalafmtCheck;it:scalafmtCheck;fun:scalafmtCheck;e2e:scalafmtCheck"
)
addCommandAlias(
  "formatAll",
  ";headerCreate;test:headerCreate;fun:headerCreate;it:headerCreate;scalafmt;test:scalafmt;it:scalafmt;fun:scalafmt;e2e:scalafmt"
)
addCommandAlias("fullTest", ";test;fun:test;it:test;e2e:test")
addCommandAlias("fullCoverageTest", ";coverage;test;fun:test;it:test;e2e:test;coverageReport;coverageAggregate")

dependencyCheckFormats := Seq("XML", "HTML")
dependencyCheckNodeAnalyzerEnabled := Some(false)
dependencyCheckNodeAuditAnalyzerEnabled := Some(false)
dependencyCheckNPMCPEAnalyzerEnabled := Some(false)
dependencyCheckRetireJSAnalyzerEnabled := Some(false)

excludeDependencies ++= globalExcludeDeps




val generateModulesList = taskKey[Seq[File]]("generateModulesList")

Compile / generateModulesList :=
  new FileWriter(subProjects).generate( (Compile / resourceManaged).value / "modules.txt")

Compile / sourceGenerators += (Compile / generateModulesList)

