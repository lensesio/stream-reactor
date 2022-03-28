import Dependencies._
import sbt._
import sbt.librarymanagement.InclExclRule

import scala.collection.immutable

object Dependencies {

  val globalExcludeDeps : Seq[InclExclRule] = Seq(
    "log4j" % "log4j",
    "org.slf4j" % "slf4j-log4j12",
    "org.apache.logging.log4j" % "log4j-api",
    "org.apache.logging.log4j" % "log4j-core",
    "org.apache.logging.log4j" % "log4j-slf4j-impl",
    "com.sun.jersey" % "*",
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "com.fasterxml.jackson.module"),
  )

  // scala versions
  val scalaOrganization      = "org.scala-lang"
  val scala213Version        = "2.13.5"
  val supportedScalaVersions = List(scala213Version)

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
    val mockitoScalaVersion = "1.16.46"
    val snakeYamlVersion = "1.29"
    val openCsvVersion = "5.5.2"

    val californiumVersion = "2.0.0-M4"
    val bouncyCastleVersion = "1.54"
    //val nettyVersion = "4.0.47.Final"
    val nettyVersion = "4.1.52.Final"

    val dropWizardMetricsVersion = "4.0.2"
    val cassandraDriverVersion = "3.7.1"
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
    val s3HadoopVersion = "2.10.0"
    val hiveHadoopVersion = "2.7.4"

    val zookeeperServerVersion = "3.7.0"

    val mongoDbVersion = "3.12.10"
    val mongoDbEmbeddedVersion = "3.2.0"

    val jedisVersion = "3.6.3"
    val gsonVersion = "2.8.9"

    val hiveVersion = "2.1.1"
    val joddVersion = "4.1.4"

    val junitVersion = "4.11"
    val junitInterfaceVersion = "0.11"

    trait ElasticVersions {
      val elastic4sVersion, elasticSearchVersion, jnaVersion: String
    }

    object Elastic6Versions extends ElasticVersions() {
      override val elastic4sVersion: String = "6.7.4"
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
  val junit = "junit" % "junit" % junitVersion
  val `junit-interface` = "com.novocode" % "junit-interface" % junitInterfaceVersion

  lazy val pegDown = "org.pegdown" % "pegdown" % "1.6.0"

  val catsEffectScalatest = "org.typelevel" %% "cats-effect-testing-scalatest" % `cats-effect-testing`

  val enumeratumCore  = "com.beachape" %% "enumeratum"       % enumeratumVersion
  val enumeratumCirce = "com.beachape" %% "enumeratum-circe" % enumeratumVersion
  val enumeratum      = Seq(enumeratumCore, enumeratumCirce)

  val classGraph = "io.github.classgraph" % "classgraph" % classGraphVersions

  lazy val slf4j = "org.slf4j" % "slf4j-api" % slf4jVersion

  lazy val kafkaConnectJson = "org.apache.kafka" % "connect-json" % kafkaVersion % "provided"

  //lazy val log4jConfig = "org.apache.logging.log4j" % "log4j-config" % ""

  def confluentAvroConverter(confluentVersion: String) = ("io.confluent" % "kafka-connect-avro-converter" % confluentVersion)
    .exclude("org.slf4j", "slf4j-log4j12")
    .exclude("org.apache.kafka", "kafka-clients")
    .exclude("javax.ws.rs", "javax.ws.rs-api")
    //.exclude("io.confluent", "kafka-schema-registry-client")
    //.exclude("io.confluent", "kafka-schema-serializer")
    .excludeAll(ExclusionRule(organization = "io.swagger"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.databind"))


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

  def hadoopCommon(version: String) = ("org.apache.hadoop" % "hadoop-common" % version)
    .excludeAll(ExclusionRule(organization = "javax.servlet"))
    .excludeAll(ExclusionRule(organization = "javax.servlet.jsp"))
    .excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    .excludeAll(ExclusionRule(organization = "org.codehaus.jackson"))
    .exclude("org.apache.hadoop", "hadoop-annotations")
    .exclude("org.apache.hadoop", "hadoop-auth")

  def hadoopMapReduce(version: String) = ("org.apache.hadoop" % "hadoop-mapreduce" % version)

  def hadoopMapReduceClient(version: String) = ("org.apache.hadoop" % "hadoop-mapreduce-client" % version)

  def hadoopMapReduceClientCore(version: String) = ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % version)

  def hadoopExec(version: String) = ("org.apache.hadoop" % "hadoop-exec" % version)
  def hadoopHdfs(version: String) = ("org.apache.hadoop" % "hadoop-hdfs" % version)


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

  def json4sNative(json4sVersion: String) = ("org.json4s" %% "json4s-native" % json4sVersion)
  def json4sJackson(json4sVersion: String) = ("org.json4s" %% "json4s-jackson" % json4sVersion)
  def jacksonDatabind(jacksonVersion: String) = ("com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion)
  def jacksonModuleScala(jacksonVersion: String) = ("com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion)

  lazy val snakeYaml = ("org.yaml" % "snakeyaml" % snakeYamlVersion)
  lazy val openCsv = ("com.opencsv" % "opencsv" % openCsvVersion)

  lazy val cassandraDriver = ("com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverVersion)
  lazy val jsonPath = ("com.jayway.jsonpath" % "json-path" % jsonPathVersion)
  lazy val nettyTransport = ("io.netty" % "netty-transport-native-epoll" % nettyVersion classifier "linux-x86_64")

  lazy val cassandraUnit = ("org.cassandraunit" % "cassandra-unit" % cassandraUnitVersion)

  lazy val azureDocumentDb = ("com.microsoft.azure" % "azure-documentdb" % azureDocumentDbVersion)
  lazy val scalaParallelCollections = ("org.scala-lang.modules" %% "scala-parallel-collections" % scalaParallelCollectionsVersion)

  lazy val testContainers = ("com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion)
  lazy val testContainersCassandra = ("com.dimafeng" %% "testcontainers-scala-cassandra" % testcontainersScalaVersion)
  lazy val testContainersMongoDb = ("com.dimafeng" %% "testcontainers-scala-mongodb" % testcontainersScalaVersion)
  lazy val testContainersToxiProxy = ("com.dimafeng" %% "testcontainers-scala-toxiproxy" % testcontainersScalaVersion)
  lazy val testContainersElasticSearch = ("com.dimafeng" %% "testcontainers-scala-elasticsearch" % testcontainersScalaVersion)
  lazy val testContainersDebezium = ( "io.debezium" % "debezium-testing-testcontainers" % "1.4.2.Final")

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

  // testcontainers module only
  lazy val festAssert = ("org.easytesting" % "fest-assert" % "1.4")

  def elastic4sCore(v: String) = ("com.sksamuel.elastic4s" %% "elastic4s-core" % v)
  def elastic4sClient(v: String) = ("com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % v)
  def elastic4sTestKit(v: String) = ("com.sksamuel.elastic4s" %% "elastic4s-testkit" % v)
  def elastic4sHttp(v: String) = ("com.sksamuel.elastic4s" %% "elastic4s-http" % v)

  def elasticSearch(v: String) = ("org.elasticsearch" % "elasticsearch" % v)
  def elasticSearchAnalysis(v: String) = ("org.codelibs.elasticsearch.module" % "analysis-common" % v)

  def jna(v: String) = ("net.java.dev.jna" % "jna" % v)
  lazy val log4j2Slf4j = ("org.apache.logging.log4j" % "log4j-to-slf4j" % "2.16.0")

  lazy val kafkaClients             = "org.apache.kafka"          % "kafka-clients"          % Versions.kafkaVersion
  lazy val kafkaJsonSchemaSerializer = "io.confluent" % "kafka-json-schema-serializer" % confluentVersion
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
  ) ++ enumeratum ++ circe ++ http4s)
    .map(_ exclude ("org.slf4j", "slf4j-log4j12"))

  //Specific modules dependencies
  val baseDeps: Seq[ModuleID] = (Seq(
    kafkaConnectJson,
    cats,
    catsLaws,
    catsEffect,
    catsEffectLaws,
    logback,
    logbackCore,
    scalaLogging,
    urlValidator,
    catsFree,
    guava
  ) ++ enumeratum ++ circe ++ http4s)

  //Specific modules dependencies
  val kafkaConnectCommonDeps: Seq[ModuleID] = Seq(
    avro4s,
    kcql,
    calciteCore,
    calciteLinq4J,
    zookeeperServer,
  )

  //Specific modules dependencies

  val kafkaConnectS3Deps: Seq[ModuleID] = Seq(
    s3Sdk,
    parquetAvro(s3ParquetVersion),
    parquetHadoop(s3ParquetVersion),
    hadoopCommon(s3HadoopVersion),
    hadoopMapReduce(s3HadoopVersion),
    hadoopMapReduceClient(s3HadoopVersion),
    hadoopMapReduceClientCore(s3HadoopVersion),
    javaxBind,
    jcloudsBlobstore,
    jcloudsProviderS3,
    snakeYaml,
    openCsv,
    guice,
    guiceAssistedInject,
  )

  val kafkaConnectS3TestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(testContainers)

  val kafkaConnectCoapDeps: Seq[ModuleID] = (californium ++ bouncyCastle)

  val kafkaConnectCassandraDeps: Seq[ModuleID] = Seq(
    cassandraDriver,
    jsonPath,
    nettyTransport,
    //dropWizardMetrics
  )

  val kafkaConnectCassandraTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(testContainers, testContainersCassandra)

  val kafkaConnectHazelCastDeps: Seq[ModuleID] = Seq(
    hazelCast,
    javaxCache
  )

  val kafkaConnectAzureDocumentDbDeps: Seq[ModuleID] = Seq(azureDocumentDb,
    //, scalaParallelCollections
    )

  val kafkaConnectInfluxDbDeps : Seq[ModuleID] = Seq(influx, avro4s, avro4sJson)

  val kafkaConnectJmsDeps : Seq[ModuleID] = Seq(jmsApi)

  val kafkaConnectJmsTestDeps : Seq[ModuleID] = baseTestDeps ++ Seq(activeMq)

  val kafkaConnectKuduDeps : Seq[ModuleID] = Seq(kuduClient)

  val kafkaConnectMqttDeps : Seq[ModuleID] = Seq(mqttClient, avro4s, avro4sJson) ++ bouncyCastle

  val kafkaConnectMqttTestDeps : Seq[ModuleID] = baseTestDeps ++ Seq(testContainers, testContainersToxiProxy)

  val kafkaConnectPulsarDeps : Seq[ModuleID] = Seq(pulsar, avro4s, avro4sJson)

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

  val kafkaConnectElastic6TestDeps : Seq[ModuleID] = baseTestDeps ++ elasticTestCommonDeps(Elastic6Versions) ++ Seq()

  val kafkaConnectElastic7Deps : Seq[ModuleID] = elasticCommonDeps(Elastic7Versions) ++ Seq(elastic4sClient(Elastic7Versions.elastic4sVersion))

  val kafkaConnectElastic7TestDeps : Seq[ModuleID] = baseTestDeps ++ elasticTestCommonDeps(Elastic7Versions) ++ Seq()

  val kafkaConnectFtpDeps : Seq[ModuleID] = Seq(commonsNet, commonsCodec, commonsIO, jsch)

  val kafkaConnectFtpTestDeps : Seq[ModuleID] = baseTestDeps ++ Seq(mina, betterFiles, ftpServer, fakeSftpServer)

  val kafkaConnectHbaseDeps : Seq[ModuleID] = Seq(hadoopHdfs(s3HadoopVersion), hbaseClient)

  val kafkaConnectHbaseTestDeps : Seq[ModuleID] = baseTestDeps ++ Seq()

  val kafkaConnectHiveDeps : Seq[ModuleID] = Seq(nettyAll, parquetAvro(hiveParquetVersion), parquetColumn(hiveParquetVersion), parquetEncoding(hiveParquetVersion), parquetHadoop(hiveParquetVersion), parquetHadoopBundle(hiveParquetVersion), joddCore, hiveJdbc, hiveExec, hadoopCommon(hiveHadoopVersion), hadoopHdfs(hiveHadoopVersion), hadoopMapReduce(hiveHadoopVersion), hadoopMapReduceClient(hiveHadoopVersion), hadoopMapReduceClientCore(hiveHadoopVersion))

  val kafkaConnectHiveTestDeps : Seq[ModuleID] = baseTestDeps ++ Seq(testContainers)

  val kafkaConnectMongoDbDeps : Seq[ModuleID] = Seq(mongoDb)

  val kafkaConnectMongoDbTestDeps : Seq[ModuleID] = baseTestDeps ++ Seq(mongoDbEmbedded, avro4s)

  val kafkaConnectRedisDeps : Seq[ModuleID] = Seq(jedis)

  val kafkaConnectRedisTestDeps : Seq[ModuleID] = (baseTestDeps ++ Seq(testContainers, gson))
    .map {
      moduleId: ModuleID => moduleId.extra("scope" -> "test")
    }

  val kafkaConnectTestContainersDeps : Seq[ModuleID] = baseTestDeps ++ Seq(
    testContainers,
    testContainersCassandra,
    testContainersMongoDb,
    testContainersElasticSearch,
    testContainersDebezium,
    jsonPath,
    festAssert,
    jedis,
    mongoDb,
    avro4s,
    kafkaClients,
    kafkaJsonSchemaSerializer,
    junit,
    `junit-interface`
  )

  // build plugins
  val kindProjectorPlugin = addCompilerPlugin("org.typelevel" %% "kind-projector" % kindProjectorVersion)
  val betterMonadicFor    = addCompilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion)

}
