import sbt._
import Settings._

// This line ensures that sources are downloaded for dependencies, when using Bloop
bloopExportJarClassifiers in Global := Some(Set("sources"))

ThisBuild / scalaVersion := "2.13.7"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

lazy val root = Project("stream-reactor", file("."))
  .settings(
    publish := {},
    publishArtifact := false,
    name := "stream-reactor"
  )
  .aggregate(
    common,
    awsS3,
    azureDocumentDb,
    cassandra,
    coap,
    //elastic6,
    //elastic7,
    ftp,
    hazelCast,
    //hbase,
    //hive
    influx,
    jms,
    kudu,
    //mongodb
    mqtt,
    pulsar
    //redis
    //testcontainers
  )

lazy val common = (project in file("kafka-connect-common"))
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-common",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCommonDeps,
        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject()
  .enablePlugins(PackPlugin)

lazy val awsS3 = (project in file("kafka-connect-aws-s3"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-aws-s3",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectS3Deps,
        //Test / parallelExecution := false,

        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject(testDeps = kafkaConnectS3TestDeps)
  .enablePlugins(PackPlugin)

lazy val azureDocumentDb = (project in file("kafka-connect-azure-documentdb"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-azure-documentdb",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectAzureDocumentDbDeps,

        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject()
  .enablePlugins(PackPlugin)

lazy val coap = (project in file("kafka-connect-coap"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-coap",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCoapDeps,

        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject()
  .enablePlugins(PackPlugin)

lazy val cassandra = (project in file("kafka-connect-cassandra"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-cassandra",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectCassandraDeps,

        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject(testDeps = kafkaConnectCassandraTestDeps)
  .enablePlugins(PackPlugin)

lazy val hazelCast = (project in file("kafka-connect-hazelcast"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-hazelcast",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectHazelCastDeps,

        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject()
  .enablePlugins(PackPlugin)

lazy val influx = (project in file("kafka-connect-influxdb"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-influxdb",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectInfluxDbDeps,

        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject()
  .enablePlugins(PackPlugin)

lazy val jms = (project in file("kafka-connect-jms"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-jms",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectJmsDeps,

        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject(testDeps = kafkaConnectJmsTestDeps)
  .enablePlugins(PackPlugin)

lazy val kudu = (project in file("kafka-connect-kudu"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-kudu",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectKuduDeps,

        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject()
  .enablePlugins(PackPlugin)

lazy val mqtt = (project in file("kafka-connect-mqtt"))
  .dependsOn(common)
  .settings(
    settings ++
      Seq(
        name := "kafka-connect-mqtt",
        description := "Kafka Connect compatible connectors to move data between Kafka and popular data stores",
        libraryDependencies ++= baseDeps ++ kafkaConnectMqttDeps,

        publish / skip := true,
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject(testDeps = kafkaConnectMqttTestDeps)
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
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject()
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
        packDir := s"pack_${CrossVersion.binaryScalaVersion(scalaVersion.value)}",
        packGenerateMakefile := false,
        packExcludeJars := Seq("kafka-clients.*\\.jar", "kafka-clients.*\\.jar", "hadoop-yarn.*\\.jar")
      )
  )
  .configureTestsForProject(testDeps = kafkaConnectFtpTestDeps)
  .enablePlugins(PackPlugin)

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
