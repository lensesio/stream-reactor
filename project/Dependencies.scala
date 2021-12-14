import Dependencies._
import sbt.Keys.excludeDependencies
import sbt._
import sbt.librarymanagement.InclExclRule

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
    val s3ParquetVersion      = "1.11.0"
    val hiveParquetVersion      = "1.8.3"

    val jerseyCommonVersion = "2.34"

    val calciteVersion = "1.12.0"
    val awsSdkVersion = "2.17.22"
    val jCloudsSdkVersion = "2.3.0"
    val guavaVersion = "31.0.1-jre"
    val guiceVersion = "5.0.1"
    val javaxBindVersion = "2.3.1"

    val kcqlVersion = "2.8.7"
    val json4sVersion = "4.0.3"
    val jacksonVersion = "2.12.3"
    val mockitoScalaVersion = "1.16.46"
    val snakeYamlVersion = "1.29"
    val openCsvVersion = "5.5.2"

    val californiumVersion = "2.0.0-M4"
    val bouncyCastleVersion = "1.54"
    //val nettyVersion = "4.0.47.Final"
    val nettyVersion = "4.1.52.Final"

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

    val commonsNetVersion = "3.8.0"
    val commonsCodecVersion = "1.15"
    val commonsIOVersion = "2.11.0"
    val jschVersion = "0.1.55"

    val minaVersion = "2.1.5"
    val betterFilesVersion = "3.8.0"
    val ftpServerVersion = "1.1.1"
    val fakeSftpServerVersion = "2.0.0"

    val hbaseClientVersion = "2.4.8"
    //val hadoopVersion = "2.10.1"
    val hadoopVersion = "2.7.4"

    val zookeeperServerVersion = "3.7.0"

    val mongoDbVersion = "3.12.10"
    val mongoDbEmbeddedVersion = "3.2.0"

    val jedisVersion = "3.6.3"
    val gsonVersion = "2.8.9"

    val hiveVersion = "2.1.1"
    val joddVersion = "4.1.4"

    trait ElasticVersions {
      val elastic4sVersion, elasticSearchVersion, jnaVersion: String
    }

    object Elastic6Versions extends ElasticVersions() {
      override val elastic4sVersion: String = "6.7.8"
      override val elasticSearchVersion: String = "6.7.2"
      override val jnaVersion: String = "3.0.9"
    }

    object Elastic7Versions extends ElasticVersions {
      override val elastic4sVersion: String = "7.7.0"
      override val elasticSearchVersion: String = "7.7.0"
      override val jnaVersion: String = "4.5.1"
    }

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

  def parquetAvro(version: String)   = "org.apache.parquet" % "parquet-avro"   % version
  def parquetHadoop(version: String) = "org.apache.parquet" % "parquet-hadoop" % version
  def parquetColumn(version: String) = "org.apache.parquet" % "parquet-column" % version
  def parquetEncoding(version: String) = "org.apache.parquet" % "parquet-encoding" % version
  def parquetHadoopBundle(version: String) = "org.apache.parquet" % "parquet-hadoop-bundle" % version

  lazy val hadoopCommon = ("org.apache.hadoop" % "hadoop-common" % hadoopVersion)
    .excludeAll(ExclusionRule(organization = "javax.servlet"))
    .excludeAll(ExclusionRule(organization = "javax.servlet.jsp"))
    .excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    .excludeAll(ExclusionRule(organization = "org.codehaus.jackson"))
    .exclude("org.apache.hadoop", "hadoop-annotations")
    .exclude("org.apache.hadoop", "hadoop-auth")

  lazy val hadoopMapReduce = ("org.apache.hadoop" % "hadoop-mapreduce" % hadoopVersion)

  lazy val hadoopMapReduceClient = ("org.apache.hadoop" % "hadoop-mapreduce-client" % hadoopVersion)

  lazy val hadoopMapReduceClientCore = ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion)


  lazy val kcql = ("com.datamountaineer" % "kcql" % kcqlVersion)
    .exclude("com.google.guava", "guava")

  lazy val calciteCore = ("org.apache.calcite" % "calcite-core" % calciteVersion)
  lazy val calciteLinq4J = ("org.apache.calcite" % "calcite-linq4j" % calciteVersion)

  lazy val s3Sdk = ("software.amazon.awssdk" % "s3" % awsSdkVersion)
  lazy val javaxBind = ("javax.xml.bind" % "jaxb-api" % javaxBindVersion)

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
  lazy val testContainersElasticSearch = ("com.dimafeng" %% "testcontainers-scala-elasticsearch" % testcontainersScalaVersion)
  lazy val dropWizardMetrics = ("io.dropwizard.metrics" % "metrics-jmx" % dropWizardMetricsVersion)

  lazy val hazelCast = ("com.hazelcast" % "hazelcast-all" % hazelCastVersion)
  lazy val javaxCache = ("javax.cache" % "cache-api" % javaxCacheVersion)
  lazy val influx = ("org.influxdb" % "influxdb-java" % influxVersion)

  lazy val jmsApi = ("javax.jms" % "javax.jms-api" % jmsApiVersion)
  lazy val activeMq = ("org.apache.activemq" % "activemq-all" % activeMqVersion)

  lazy val kuduClient = ("org.apache.kudu" % "kudu-client" % kuduVersion)

  lazy val mqttClient = ("org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % mqttVersion)

  lazy val pulsar = ("org.apache.pulsar" % "pulsar-client" % pulsarVersion)

  lazy val commonsNet = ("commons-net" % "commons-net" % commonsNetVersion)
  lazy val commonsCodec = ("commons-codec" % "commons-codec" % commonsCodecVersion)
  lazy val commonsIO = ("commons-io" % "commons-io" % commonsIOVersion)
  lazy val jsch = ("com.jcraft" % "jsch" % jschVersion)
  lazy val mina = ("org.apache.mina" % "mina-core" % minaVersion)
  lazy val betterFiles = ("com.github.pathikrit" %% "better-files" % betterFilesVersion)
  lazy val ftpServer = ("org.apache.ftpserver" % "ftpserver-core" % ftpServerVersion)
  lazy val fakeSftpServer = ("com.github.stefanbirkner" % "fake-sftp-server-lambda" % fakeSftpServerVersion)

  lazy val hbaseClient = "org.apache.hbase" % "hbase-client" % hbaseClientVersion
  lazy val hadoopHdfs = "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
  lazy val zookeeperServer = "org.apache.zookeeper" % "zookeeper" % zookeeperServerVersion

  lazy val mongoDb = "org.mongodb" % "mongo-java-driver" % mongoDbVersion
  lazy val mongoDbEmbedded = "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % mongoDbEmbeddedVersion

  lazy val jedis = "redis.clients" % "jedis" % jedisVersion
  lazy val gson = "com.google.code.gson" % "gson" % gsonVersion

  lazy val nettyAll = ("io.netty" % "netty-all" % nettyVersion)
  lazy val joddCore = ("org.jodd" % "jodd-core" % joddVersion)
  lazy val hiveJdbc = "org.apache.hive" % "hive-jdbc" % hiveVersion
  lazy val hiveMetastore = "org.apache.hive" % "hive-metastore" % hiveVersion

  lazy val hiveExec = ("org.apache.hive" % "hive-exec" % hiveVersion)// classifier "core"
    .exclude("org.apache.calcite", "calcite-avatica")
    .exclude("com.fasterxml.jackson.core" , "jackson-annotations")

  def elastic4sCore(v: String) = ("com.sksamuel.elastic4s" %% "elastic4s-core" % v)
  def elastic4sClient(v: String) = ("com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % v)
  def elastic4sTestKit(v: String) = ("com.sksamuel.elastic4s" %% "elastic4s-testkit" % v % Test)
  def elastic4sHttp(v: String) = ("com.sksamuel.elastic4s" %% "elastic4s-http" % v)
  def elastic4sEmbedded(v: String) = ("com.sksamuel.elastic4s" %% "elastic4s-embedded" % v % Test)

  def elasticSearch(v: String) = ("org.elasticsearch" % "elasticsearch" % v)
  def elasticSearchAnalysis(v: String) = ("org.codelibs.elasticsearch.module" % "analysis-common" % v)

  def jna(v: String) = ("net.java.dev.jna" % "jna" % v)

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
    //parquetAvro,
    //parquetHadoop,
    //hadoopCommon,
    //hadoopMapReduce,
    guava
  ) ++ enumeratum ++ circe ++ http4s)
    .map(_.exclude("org.slf4j", "slf4j-log4j12"))
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
    zookeeperServer,
  ).map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  //Specific modules dependencies

  val kafkaConnectS3Deps: Seq[ModuleID] = Seq(
    s3Sdk,
    parquetAvro(s3ParquetVersion),
    parquetHadoop(s3ParquetVersion),
    hadoopCommon,
    hadoopMapReduce,
    javaxBind,
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

  def elasticCommonDeps(v: ElasticVersions) : Seq[ModuleID] = Seq(
    elastic4sCore(v.elastic4sVersion),
    jna(v.jnaVersion),
    elasticSearch(v.elasticSearchVersion),
    elasticSearchAnalysis(v.elasticSearchVersion)
  )

  def elasticTestCommonDeps(v: ElasticVersions) : Seq[ModuleID] = Seq(
    elastic4sTestKit(v.elastic4sVersion),
    testContainers,
    testContainersElasticSearch
  )

  val kafkaConnectElastic6Deps : Seq[ModuleID] = elasticCommonDeps(Elastic6Versions) ++ Seq(elastic4sHttp(Elastic6Versions.elastic4sVersion))

  val kafkaConnectElastic6TestDeps : Seq[ModuleID] = elasticTestCommonDeps(Elastic6Versions) ++ Seq(elastic4sEmbedded(Elastic6Versions.elastic4sVersion))

  val kafkaConnectElastic7Deps : Seq[ModuleID] = elasticCommonDeps(Elastic7Versions) ++ Seq(elastic4sClient(Elastic7Versions.elastic4sVersion))

  val kafkaConnectElastic7TestDeps : Seq[ModuleID] = elasticTestCommonDeps(Elastic7Versions) ++ Seq()

  val kafkaConnectFtpDeps : Seq[ModuleID] = Seq(commonsNet, commonsCodec, commonsIO, jsch)
    .map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  val kafkaConnectFtpTestDeps : Seq[ModuleID] = Seq(mina, betterFiles, ftpServer, fakeSftpServer)
    .map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  val kafkaConnectHbaseDeps : Seq[ModuleID] = Seq(hadoopHdfs, hbaseClient)

  val kafkaConnectHbaseTestDeps : Seq[ModuleID] = Seq()


  /**
        compile("org.apache.hadoop:hadoop-mapreduce-client:$hadoopVersion")
        compile("org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion")
        compile("org.typelevel:cats-core_$scalaMajorVersion:$catsVersion")
   */
  val kafkaConnectHiveDeps : Seq[ModuleID] = Seq(nettyAll, parquetAvro(hiveParquetVersion), parquetColumn(hiveParquetVersion), parquetEncoding(hiveParquetVersion), parquetHadoop(hiveParquetVersion), parquetHadoopBundle(hiveParquetVersion), joddCore, hiveJdbc, hiveExec, hadoopCommon, hadoopHdfs, hadoopMapReduce, hadoopMapReduceClient, hadoopMapReduceClientCore)

  val kafkaConnectHiveTestDeps : Seq[ModuleID] = Seq(testContainers)

  val kafkaConnectMongoDbDeps : Seq[ModuleID] = Seq(mongoDb, json4sNative, json4sJackson)

  val kafkaConnectMongoDbTestDeps : Seq[ModuleID] = Seq(mongoDbEmbedded, avro4s)

  val kafkaConnectRedisDeps : Seq[ModuleID] = Seq(jedis)

  val kafkaConnectRedisTestDeps : Seq[ModuleID] = Seq(testContainers, gson)

  // build plugins
  val kindProjectorPlugin = addCompilerPlugin("org.typelevel" %% "kind-projector" % kindProjectorVersion)
  val betterMonadicFor    = addCompilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion)

  val globalExcludeDeps : Seq[InclExclRule] = Seq(
    "log4j" % "log4j",
    "org.slf4j" % "slf4j-log4j12",
    "org.apache.logging.log4j" % "log4j-api",
    "org.apache.logging.log4j" % "log4j-core",
  )

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
