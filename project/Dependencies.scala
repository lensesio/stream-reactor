import Dependencies._
import sbt._

import scala.collection.immutable

object Dependencies {

  // scala versions
  val scalaOrganization      = "org.scala-lang"
  val scala213Version        = "2.13.5"
  val supportedScalaVersions = List(scala213Version)

  val FunctionalTest: Configuration = config("fun") extend Test describedAs "Runs functional tests"
  val ItTest:         Configuration = config("it").extend(Test).describedAs("Runs integration tests")
  val E2ETest:        Configuration = config("e2e").extend(Test).describedAs("Runs E2E tests")

  val testConfigurationsMap =
    Map(Test.name -> Test, FunctionalTest.name -> FunctionalTest, ItTest.name -> ItTest, E2ETest.name -> E2ETest)

  val commonResolvers = Seq(
    Resolver sonatypeRepo "public",
    Resolver typesafeRepo "releases",
    Resolver.mavenLocal,
    "confluent" at "https://packages.confluent.io/maven/",
    "typesafe" at "https://repo.typesafe.com/typesafe/releases/",
    "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "conjars" at "https://conjars.org/repo",
    "jitpack" at "https://jitpack.io",
  )

  object Versions {
    // libraries versions
    val scalatestVersion               = "3.1.0"
    val scalaCheckPlusVersion          = "3.1.0.0"
    val scalatestPlusScalaCheckVersion = "3.1.0.0-RC2"
    val scalaCheckVersion              = "1.14.3"
    val randomDataGeneratorVersion     = "2.8"

    val enumeratumVersion = "1.7.0"

    val kafkaVersion = "2.8.0"

    val confluentVersion = "6.2.0"

    val http4sVersion = "1.0.0-M27"
    val avroVersion   = "1.9.2"
    //val avro4sVersion = "4.0.11"
    val avro4sVersion = "3.1.1"

    val catsVersion           = "2.6.1"
    val catsEffectVersion     = "3.2.2"
    val `cats-effect-testing` = "1.2.0"

    val urlValidatorVersion       = "1.6"
    val circeVersion              = "0.14.1"
    val circeGenericExtrasVersion = "0.14.1"
    val circeJsonSchemaVersion    = "0.2.0"

    // build plugins versions
    val silencerVersion         = "1.7.1"
    val kindProjectorVersion    = "0.10.3"
    val betterMonadicForVersion = "0.3.1"

    val slf4jVersion = "1.7.25"

    val logbackVersion        = "1.2.3"
    val scalaLoggingVersion   = "3.9.2"
    val classGraphVersions    = "4.4.12"

    val wiremockJre8Version = "2.25.1"
    val parquetVersion      = "1.12.0"
    val hadoopVersion       = "2.10.1"

    val jerseyCommonVersion = "2.34"

    val calciteVersion = "1.12.0"
    val awsSdkVersion = "2.17.22"
    val jCloudsSdkVersion = "2.3.0"
    val guavaVersion = "31.0.1-jre"
    val guiceVersion = "5.0.1"

    val kcqlVersion = "2.8.7"
    val json4sVersion = "4.0.3"
    val jacksonVersion = "2.12.3"
    val mockitoScalaVersion = "1.16.46"
    val snakeYamlVersion = "1.29"
    val openCsvVersion = "5.5.2"

    val californiumVersion = "2.0.0-M4"
    val bouncyCastleVersion = "1.54"
    val nettyVersion = "4.0.47.Final"
    val dropWizardMetricsVersion = "4.0.2"
    val cassandraDriverVersion = "4.13.0"
    val jsonPathVersion = "2.4.0"

    val cassandraUnitVersion = "4.3.1.0"

    val azureDocumentDbVersion = "2.6.4"
    val scalaParallelCollectionsVersion = "0.2.0"
    val testcontainersScalaVersion = "0.39.12"

    val hazelCastVersion = "3.12.6"
    val javaxCacheVersion = "1.0.0"

    val influxVersion = "2.21"

    val jmsApiVersion = "2.0.1"
    val activeMqVersion = "5.14.5"

    val kuduVersion = "1.11.1"

    val mqttVersion = "1.2.5"

    //val pulsarVersion = "2.9.0"
    val pulsarVersion = "1.22.0-incubating"
  }

  import Versions._

  // functional libraries
  val cats           = "org.typelevel" %% "cats-core"        % catsVersion
  val catsLaws       = "org.typelevel" %% "cats-laws"        % catsVersion
  val catsEffect     = "org.typelevel" %% "cats-effect"      % catsEffectVersion
  val catsEffectLaws = "org.typelevel" %% "cats-effect-laws" % catsEffectVersion
  lazy val catsFree  = "org.typelevel" %% "cats-free"        % catsVersion

  val urlValidator = "commons-validator" % "commons-validator" % urlValidatorVersion

  val circeGeneric = "io.circe" %% "circe-generic" % circeVersion
  val circeParser  = "io.circe" %% "circe-parser"  % circeVersion
  val circeRefined = "io.circe" %% "circe-refined" % circeVersion
  val circe        = Seq(circeGeneric, circeParser, circeRefined)

  // logging
  val logback          = "ch.qos.logback"              % "logback-classic" % logbackVersion
  lazy val logbackCore = "ch.qos.logback"              % "logback-core"    % logbackVersion
  val scalaLogging     = "com.typesafe.scala-logging" %% "scala-logging"   % scalaLoggingVersion

  // testing
  val scalatest = "org.scalatest" %% "scalatest" % scalatestVersion
  val scalatestPlusScalaCheck =
    "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestPlusScalaCheckVersion
  val scalaCheck      = "org.scalacheck"    %% "scalacheck"  % scalaCheckVersion
  val `mockito-scala` = "org.mockito" %% "mockito-scala" % mockitoScalaVersion


  lazy val pegDown = "org.pegdown" % "pegdown" % "1.6.0"

  val catsEffectScalatest = "org.typelevel" %% "cats-effect-testing-scalatest" % `cats-effect-testing`

  val enumeratumCore  = "com.beachape" %% "enumeratum"       % enumeratumVersion
  val enumeratumCirce = "com.beachape" %% "enumeratum-circe" % enumeratumVersion
  val enumeratum      = Seq(enumeratumCore, enumeratumCirce)

  val classGraph = "io.github.classgraph" % "classgraph" % classGraphVersions

  lazy val slf4j = "org.slf4j" % "slf4j-api" % slf4jVersion

  lazy val kafkaConnectJson = "org.apache.kafka" % "connect-json" % kafkaVersion % "provided"

  lazy val confluentAvroConverter = ("io.confluent" % "kafka-connect-avro-converter" % confluentVersion)
    .exclude("org.slf4j", "slf4j-log4j12")
    .exclude("org.apache.kafka", "kafka-clients")
    .exclude("javax.ws.rs", "javax.ws.rs-api")
    //.exclude("io.confluent", "kafka-schema-registry-client")
    //.exclude("io.confluent", "kafka-schema-serializer")
    .excludeAll(ExclusionRule(organization = "io.swagger"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))

  val http4sDsl         = "org.http4s" %% "http4s-dsl"               % http4sVersion
  val http4sAsyncClient = "org.http4s" %% "http4s-async-http-client" % http4sVersion
  val http4sBlazeServer = "org.http4s" %% "http4s-blaze-server"      % http4sVersion
  val http4sBlazeClient = "org.http4s" %% "http4s-blaze-client"      % http4sVersion
  val http4sCirce       = "org.http4s" %% "http4s-circe"             % http4sVersion
  val http4s            = Seq(http4sDsl, http4sAsyncClient, http4sBlazeServer, http4sCirce)

  val californiumCore =   "org.eclipse.californium" % "californium-core" % californiumVersion
  val scandium = "org.eclipse.californium" % "scandium" % californiumVersion
  val californium = Seq(californiumCore, scandium)

  val bouncyProv = "org.bouncycastle" % "bcprov-jdk15on" % bouncyCastleVersion
  val bouncyPkix = "org.bouncycastle" % "bcpkix-jdk15on" % bouncyCastleVersion
  val bouncyBcpg = "org.bouncycastle" % "bcpg-jdk15on" % bouncyCastleVersion
  val bouncyCastle = Seq(bouncyProv, bouncyPkix, bouncyBcpg)

  //lazy val avro   = "org.apache.avro"      % "avro"        % avroVersion
  lazy val avro4s = "com.sksamuel.avro4s" %% "avro4s-core" % avro4sVersion
  lazy val avro4sJson = "com.sksamuel.avro4s" %% "avro4s-json" % avro4sVersion

  val `wiremock-jre8` = "com.github.tomakehurst" % "wiremock-jre8" % wiremockJre8Version

  val jerseyCommon = "org.glassfish.jersey.core" % "jersey-common" % jerseyCommonVersion

  lazy val parquetAvro   = "org.apache.parquet" % "parquet-avro"   % parquetVersion
  lazy val parquetHadoop = "org.apache.parquet" % "parquet-hadoop" % parquetVersion
  lazy val hadoopCommon = ("org.apache.hadoop" % "hadoop-common" % hadoopVersion)
    .excludeAll(ExclusionRule(organization = "javax.servlet"))
    .excludeAll(ExclusionRule(organization = "javax.servlet.jsp"))
    .excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    .excludeAll(ExclusionRule(organization = "org.codehaus.jackson"))
    .exclude("org.apache.hadoop", "hadoop-annotations")
    .exclude("org.apache.hadoop", "hadoop-auth")

  lazy val hadoopMapReduce = ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion)
    .excludeAll(ExclusionRule(organization = "javax.servlet"))
    .excludeAll(ExclusionRule(organization = "javax.servlet.jsp"))
    .excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    .exclude("org.apache.hadoop", "hadoop-yarn-common")
    .exclude("org.apache.hadoop", "hadoop-yarn-client")

  lazy val kcql = ("com.datamountaineer" % "kcql" % kcqlVersion)
    .exclude("com.google.guava", "guava")

  lazy val calciteCore = ("org.apache.calcite" % "calcite-core" % calciteVersion)
  lazy val calciteLinq4J = ("org.apache.calcite" % "calcite-linq4j" % calciteVersion)

  lazy val s3Sdk = ("software.amazon.awssdk" % "s3" % awsSdkVersion)

  lazy val jcloudsBlobstore = ("org.apache.jclouds" % "jclouds-blobstore" % jCloudsSdkVersion)
  lazy val jcloudsProviderS3 = ("org.apache.jclouds.provider" % "aws-s3" % jCloudsSdkVersion)
  lazy val guava = ("com.google.guava" % "guava" % guavaVersion)
  lazy val guice = ("com.google.inject" % "guice" % guiceVersion)
  lazy val guiceAssistedInject = ("com.google.inject.extensions" % "guice-assistedinject" % guiceVersion)



  lazy val json4sNative = ("org.json4s" %% "json4s-native" % json4sVersion)
  lazy val json4sJackson = ("org.json4s" %% "json4s-jackson" % json4sVersion)
  lazy val jacksonDatabind = ("com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion)
  lazy val jacksonModuleScala = ("com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion)

  lazy val snakeYaml = ("org.yaml" % "snakeyaml" % snakeYamlVersion)
  lazy val openCsv = ("com.opencsv" % "opencsv" % openCsvVersion)

  lazy val cassandraDriver = ("com.datastax.oss" % "java-driver-core" % cassandraDriverVersion)
  lazy val jsonPath = ("com.jayway.jsonpath" % "json-path" % jsonPathVersion)
  lazy val nettyTransport = ("io.netty" % "netty-transport-native-epoll" % nettyVersion classifier "linux-x86_64")

  lazy val cassandraUnit = ("org.cassandraunit" % "cassandra-unit" % cassandraUnitVersion)

  lazy val azureDocumentDb = ("com.microsoft.azure" % "azure-documentdb" % azureDocumentDbVersion)
  lazy val scalaParallelCollections = ("org.scala-lang.modules" %% "scala-parallel-collections" % scalaParallelCollectionsVersion)

  lazy val testContainers = ("com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion)
  lazy val testContainersCassandra = ("com.dimafeng" %% "testcontainers-scala-cassandra" % testcontainersScalaVersion)
  lazy val testContainersToxiProxy = ("com.dimafeng" %% "testcontainers-scala-toxiproxy" % testcontainersScalaVersion)
  lazy val dropWizardMetrics = ("io.dropwizard.metrics" % "metrics-jmx" % dropWizardMetricsVersion)

  lazy val hazelCast = ("com.hazelcast" % "hazelcast-all" % hazelCastVersion)
  lazy val javaxCache = ("javax.cache" % "cache-api" % javaxCacheVersion)
  lazy val influx = ("org.influxdb" % "influxdb-java" % influxVersion)

  lazy val jmsApi = ("javax.jms" % "javax.jms-api" % jmsApiVersion)
  lazy val activeMq = ("org.apache.activemq" % "activemq-all" % activeMqVersion)

  lazy val kuduClient = ("org.apache.kudu" % "kudu-client" % kuduVersion)

  lazy val mqttClient = ("org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % mqttVersion)

  lazy val pulsar = ("org.apache.pulsar" % "pulsar-client" % pulsarVersion)
}

trait Dependencies {

  import Versions._

  val scalaOrganizationUsed:      String                = scalaOrganization
  val scalaVersionUsed:           String                = scala213Version
  val supportedScalaVersionsUsed: immutable.Seq[String] = supportedScalaVersions

  // resolvers
  val projectResolvers: Seq[MavenRepository] = commonResolvers

  val baseTestDeps: Seq[ModuleID] = (Seq(
    cats,
    catsLaws,
    catsEffect,
    catsEffectLaws,
    scalatest,
    catsEffectScalatest,
    scalatestPlusScalaCheck,
    scalaCheck,
    `mockito-scala`,
    `wiremock-jre8`,
    jerseyCommon,
    avro4s,
  ) ++ enumeratum ++ circe ++ http4s).map(_ exclude ("org.slf4j", "slf4j-log4j12")).map(
    _ % testConfigurationsMap.keys.mkString(","),
  )

  //Specific modules dependencies
  val baseDeps: Seq[ModuleID] = (Seq(
    kafkaConnectJson,
    confluentAvroConverter,
    cats,
    catsLaws,
    catsEffect,
    catsEffectLaws,
    logback,
    logbackCore,
    scalaLogging,
    urlValidator,
    catsFree,
    parquetAvro,
    parquetHadoop,
    hadoopCommon,
    hadoopMapReduce,
    guava
  ) ++ enumeratum ++ circe ++ http4s).map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  //Specific modules dependencies
  val kafkaConnectCommonDeps: Seq[ModuleID] = Seq(
    avro4s,
    kcql,
    calciteCore,
    calciteLinq4J,
    json4sNative,
    json4sJackson,
    jacksonDatabind,
    jacksonModuleScala,
  ).map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  //Specific modules dependencies

  val kafkaConnectS3Deps: Seq[ModuleID] = Seq(
    s3Sdk,
    jcloudsBlobstore,
    jcloudsProviderS3,
    snakeYaml,
    openCsv,
    guice,
    guiceAssistedInject,
  ).map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  val kafkaConnectS3TestDeps: Seq[ModuleID] = Seq(testContainers)

  val kafkaConnectCoapDeps: Seq[ModuleID] = (californium ++ bouncyCastle).map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  val kafkaConnectCassandraDeps: Seq[ModuleID] = Seq(
    cassandraDriver,
    jsonPath,
    nettyTransport,
    json4sNative,
    //dropWizardMetrics
  ).map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  val kafkaConnectCassandraTestDeps: Seq[ModuleID] = Seq(testContainers, testContainersCassandra)

  val kafkaConnectHazelCastDeps: Seq[ModuleID] = Seq(
    hazelCast,
    javaxCache
  )

  val kafkaConnectAzureDocumentDbDeps: Seq[ModuleID] = Seq(azureDocumentDb, json4sNative//, scalaParallelCollections
    )
    .map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  val kafkaConnectInfluxDbDeps : Seq[ModuleID] = Seq(influx, avro4s, avro4sJson)
    .map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  val kafkaConnectJmsDeps : Seq[ModuleID] = Seq(jmsApi)
    .map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  val kafkaConnectJmsTestDeps : Seq[ModuleID] = Seq(activeMq)

  val kafkaConnectKuduDeps : Seq[ModuleID] = Seq(kuduClient, json4sNative)
    .map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  val kafkaConnectMqttDeps : Seq[ModuleID] = Seq(mqttClient, avro4s, avro4sJson) ++ bouncyCastle
    .map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  val kafkaConnectMqttTestDeps : Seq[ModuleID] = Seq(json4sJackson, testContainers, testContainersToxiProxy)

  val kafkaConnectPulsarDeps : Seq[ModuleID] = Seq(pulsar, avro4s, avro4sJson)
    .map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  // build plugins
  val kindProjectorPlugin = addCompilerPlugin("org.typelevel" %% "kind-projector" % kindProjectorVersion)
  val betterMonadicFor    = addCompilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion)

  implicit final class ProjectRoot(project: Project) {

    def root: Project = project in file(".")
  }

  implicit final class ProjectFrom(project: Project) {

    private val commonDir = "modules"

    def from(dir: String): Project = project in file(s"$commonDir/$dir")
  }

  implicit final class DependsOnProject(project: Project) {
    private def findCompileAndTestConfigs(p: Project) =
      findTestConfigs(p) + "compile"

    private def findTestConfigs(p: Project) =
      p.configurations.map(_.name).toSet.intersect(testConfigurationsMap.keys.toSet)

    private val thisProjectsConfigs = findCompileAndTestConfigs(project)
    private def generateDepsForProject(p: Project, withCompile: Boolean) =
      p % thisProjectsConfigs
        .intersect(if (withCompile) findCompileAndTestConfigs(p) else findTestConfigs(p))
        .map(c => s"$c->$c")
        .mkString(";")

    def compileAndTestDependsOn(projects: Project*): Project =
      project.dependsOn(projects.map(generateDepsForProject(_, true)): _*)

    def testsDependOn(projects: Project*): Project =
      project.dependsOn(projects.map(generateDepsForProject(_, false)): _*)
  }
}
