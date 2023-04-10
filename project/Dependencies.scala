import Dependencies._
import KafkaVersionAxis.kafkaVersionAxis
import sbt._
import sbt.librarymanagement.InclExclRule

object Dependencies {

  val kafkaVersion = kafkaVersionAxis.kafkaVersion

  val confluentVersion = kafkaVersionAxis.confluentPlatformVersion

  val globalExcludeDeps: Seq[InclExclRule] = Seq(
    "org.jboss.logging"        % "commons-logging-jboss-logging",
    "org.jboss.logging"        % "jboss-logging",
    "org.jboss.logging"        % "jboss-logging-annotations",
    "org.jboss.logmanager"     % "jboss-logmanager-embedded",
    "org.jboss.sif4j"          % "sIf4j-jboss-logmanager",
    "commons-logging"          % "commons-logging",
    "log4j"                    % "log4j",
    "org.slf4j"                % "slf4j-log4j12",
    "org.apache.logging.log4j" % "log4j",
    //"org.apache.logging.log4j" % "log4j-api",
    "org.apache.logging.log4j" % "log4j-core",
    "org.apache.logging.log4j" % "log4j-slf4j-impl",
    "com.sun.jersey"           % "*",
  )

  // scala versions
  val scalaOrganization      = "org.scala-lang"
  val scalaVersion           = "2.13.10"
  val supportedScalaVersions = List(Dependencies.scalaVersion)

  val commonResolvers = Seq(
    Resolver sonatypeRepo "public",
    Resolver typesafeRepo "releases",
    Resolver.mavenLocal,
    "confluent" at "https://packages.confluent.io/maven/",
    "typesafe" at "https://repo.typesafe.com/typesafe/releases/",
    "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "jitpack" at "https://jitpack.io",
    "twitter" at "https://maven.twttr.com/",
  )

  object Versions {
    // libraries versions
    val scalatestVersion               = "3.2.15" // Higher versions result in org.scala-lang.modules:scala-xml conflicts
    val scalaCheckPlusVersion          = "3.1.0.0"
    val scalatestPlusScalaCheckVersion = "3.1.0.0-RC2"
    val scalaCheckVersion              = "1.17.0"
    val randomDataGeneratorVersion     = "2.8"

    val enumeratumVersion = "1.7.2"

    val http4sVersion = "1.0.0-M32"
    val avroVersion   = "1.11.0"
    val avro4sVersion = "4.1.0"

    val catsVersion           = "2.9.0"
    val catsEffectVersion     = "3.4.6"
    val `cats-effect-testing` = "1.4.0"

    val urlValidatorVersion       = "1.7"
    val circeVersion              = "0.15.0-M1"
    val circeGenericExtrasVersion = "0.14.1"
    val circeJsonSchemaVersion    = "0.2.0"
    val shapelessVersion          = "2.3.10"

    // build plugins versions
    val silencerVersion         = "1.7.1"
    val kindProjectorVersion    = "0.13.2"
    val betterMonadicForVersion = "0.3.1"

    val logbackVersion      = "1.4.5"
    val scalaLoggingVersion = "3.9.5"
    val classGraphVersions  = "4.8.154"

    val wiremockJre8Version = "2.35.0"
    val parquetVersion      = "1.12.3"

    val jerseyCommonVersion = "3.1.1"

    val calciteVersion    = "1.33.0"
    val awsSdkVersion     = "2.19.33"
    val jCloudsSdkVersion = "2.5.0"
    val guavaVersion      = "31.0.1-jre"
    val guiceVersion      = "5.1.0"
    val javaxBindVersion  = "2.3.1"

    val kcqlVersion         = "2.9.1"
    val jacksonVersion      = "2.14.2"
    val json4sVersion       = "4.0.6"
    val mockitoScalaVersion = "1.17.12"
    val snakeYamlVersion    = "1.33"
    val openCsvVersion      = "5.7.1"

    val xzVersion  = "1.9"
    val lz4Version = "1.8.0"
    val lzoVersion = "0.4.19"

    val californiumVersion  = "3.5.0"
    val bouncyCastleVersion = "1.70"
    //val nettyVersion = "4.0.47.Final"
    val nettyVersion = "4.1.71.Final"

    val dropWizardMetricsVersion = "4.2.15"
    val cassandraDriverVersion   = "3.11.3"
    val jsonPathVersion          = "2.7.0"

    val azureDocumentDbVersion          = "2.6.5"
    val scalaParallelCollectionsVersion = "1.0.4"
    val testcontainersScalaVersion      = "0.40.14"
    val testcontainersVersion           = "1.17.6"

    val hazelCastVersion          = "4.2.7"
    val hazelCastAzureVersion     = "2.1.2"
    val hazelCastGcpVersion       = "2.1"
    val hazelCastHibernateVersion = "2.2.1"
    val hazelCastWmVersion        = "4.0"

    val javaxCacheVersion = "1.1.1"

    val influxVersion = "2.23"

    val jmsApiVersion                 = "2.0.1"
    val activeMqVersion               = "5.17.0"
    val protocVersion                 = "3.11.4"
    val googleProtobufVersion         = "3.21.12"
    val protobufCompilerPluginVersion = "0.11.12"

    val kuduVersion = "1.16.0"

    val mqttVersion = "1.2.5"

    val pulsarVersion = "2.10.0"

    val httpClientVersion       = "4.5.14"
    val commonsBeanUtilsVersion = "1.9.4"
    val commonsNetVersion       = "3.9.0"
    val commonsCodecVersion     = "1.15"
    val commonsIOVersion        = "2.11.0"
    val jschVersion             = "0.1.55"

    val minaVersion           = "2.2.1"
    val betterFilesVersion    = "3.9.2"
    val ftpServerVersion      = "1.2.0"
    val fakeSftpServerVersion = "2.0.0"

    val zookeeperServerVersion = "3.8.1"

    val mongoDbVersion = "3.12.12"

    val jedisVersion = "4.3.1"
    val gsonVersion  = "2.10.1"

    val hbaseClientVersion = "2.5.3"

    val nimbusJoseJwtVersion = "9.30.1"
    val hiveVersion          = "3.1.3"
    val hadoopVersion        = "3.3.2"

    val junitVersion = "4.13.2"

    trait ElasticVersions {
      val elastic4sVersion, elasticSearchVersion, jnaVersion: String
    }

    object Elastic6Versions extends ElasticVersions() {
      override val elastic4sVersion:     String = "6.7.8"
      override val elasticSearchVersion: String = "6.8.23"
      override val jnaVersion:           String = "3.0.9"
    }

    object Elastic7Versions extends ElasticVersions {
      override val elastic4sVersion:     String = "7.17.2"
      override val elasticSearchVersion: String = "7.17.2"
      override val jnaVersion:           String = "4.5.1"
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
  val circe        = Seq(circeGeneric, circeParser)

  // logging
  val logback          = "ch.qos.logback"              % "logback-classic"  % logbackVersion
  lazy val logbackCore = "ch.qos.logback"              % "logback-core"     % logbackVersion
  val scalaLogging     = "com.typesafe.scala-logging" %% "scala-logging"    % scalaLoggingVersion
  val log4jToSlf4j     = "org.slf4j"                   % "log4j-over-slf4j" % "2.0.6"
  val jclToSlf4j       = "org.slf4j"                   % "jcl-over-slf4j"   % "2.0.6"
  val slf4jApi         = "org.slf4j"                   % "slf4j-api"        % "2.0.6"

  // testing
  val scalatest = "org.scalatest" %% "scalatest" % scalatestVersion
  val scalatestPlusScalaCheck =
    "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestPlusScalaCheckVersion
  val scalaCheck      = "org.scalacheck" %% "scalacheck"      % scalaCheckVersion
  val `mockito-scala` = "org.mockito"    %% "mockito-scala"   % mockitoScalaVersion
  val junit           = "com.github.sbt"  % "junit-interface" % "0.13.3"

  lazy val pegDown = "org.pegdown" % "pegdown" % "1.6.0"

  val catsEffectScalatest = "org.typelevel" %% "cats-effect-testing-scalatest" % `cats-effect-testing`

  val enumeratumCore  = "com.beachape" %% "enumeratum"       % enumeratumVersion
  val enumeratumCirce = "com.beachape" %% "enumeratum-circe" % enumeratumVersion
  val enumeratum      = Seq(enumeratumCore, enumeratumCirce)

  val classGraph = "io.github.classgraph" % "classgraph" % classGraphVersions

  val kafkaConnectJson: ModuleID = "org.apache.kafka" % "connect-json"  % kafkaVersion
  val kafkaClients:     ModuleID = "org.apache.kafka" % "kafka-clients" % kafkaVersion

  val confluentJsonSchemaSerializer: ModuleID =
    "io.confluent" % "kafka-json-schema-serializer" % confluentVersion

  def confluentExcludes(moduleID: ModuleID): ModuleID = moduleID
    .exclude("org.apache.kafka", "kafka-clients")
    .exclude("javax.ws.rs", "javax.ws.rs-api")
    .excludeAll(ExclusionRule(organization = "io.swagger"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.databind"))

  val confluentAvroConverter: ModuleID =
    confluentExcludes("io.confluent" % "kafka-connect-avro-converter" % confluentVersion)

  val confluentAvroData: ModuleID =
    confluentExcludes("io.confluent" % "kafka-connect-avro-data" % confluentVersion)

  val confluentProtobufConverter: ModuleID =
    confluentExcludes("io.confluent" % "kafka-connect-protobuf-converter" % confluentVersion)

  val http4sDsl         = "org.http4s" %% "http4s-dsl"               % http4sVersion
  val http4sAsyncClient = "org.http4s" %% "http4s-async-http-client" % http4sVersion
  val http4sBlazeServer = "org.http4s" %% "http4s-blaze-server"      % http4sVersion
  val http4sBlazeClient = "org.http4s" %% "http4s-blaze-client"      % http4sVersion
  val http4sCirce       = "org.http4s" %% "http4s-circe"             % http4sVersion
  val http4s            = Seq(http4sDsl, http4sAsyncClient, http4sBlazeServer, http4sCirce)

  val bouncyProv   = "org.bouncycastle" % "bcprov-jdk15on" % bouncyCastleVersion
  val bouncyUtil   = "org.bouncycastle" % "bcutil-jdk15on" % bouncyCastleVersion
  val bouncyPkix   = "org.bouncycastle" % "bcpkix-jdk15on" % bouncyCastleVersion
  val bouncyBcpg   = "org.bouncycastle" % "bcpg-jdk15on"   % bouncyCastleVersion
  val bouncyTls    = "org.bouncycastle" % "bctls-jdk15on"  % bouncyCastleVersion
  val bouncyCastle = Seq(bouncyProv, bouncyUtil, bouncyPkix, bouncyBcpg, bouncyTls)

  lazy val avro           = "org.apache.avro"      % "avro"            % avroVersion
  lazy val avroProtobuf   = "org.apache.avro"      % "avro-protobuf"   % avroVersion
  lazy val avro4s         = "com.sksamuel.avro4s" %% "avro4s-core"     % avro4sVersion
  lazy val avro4sJson     = "com.sksamuel.avro4s" %% "avro4s-json"     % avro4sVersion
  lazy val avro4sProtobuf = "com.sksamuel.avro4s" %% "avro4s-protobuf" % avro4sVersion

  val `wiremock-jre8` = "com.github.tomakehurst" % "wiremock-jre8" % wiremockJre8Version

  val jerseyCommon = "org.glassfish.jersey.core" % "jersey-common" % jerseyCommonVersion

  lazy val parquetAvro:         ModuleID = "org.apache.parquet" % "parquet-avro"          % parquetVersion
  lazy val parquetHadoop:       ModuleID = "org.apache.parquet" % "parquet-hadoop"        % parquetVersion
  lazy val parquetColumn:       ModuleID = "org.apache.parquet" % "parquet-column"        % parquetVersion
  lazy val parquetEncoding:     ModuleID = "org.apache.parquet" % "parquet-encoding"      % parquetVersion
  lazy val parquetHadoopBundle: ModuleID = "org.apache.parquet" % "parquet-hadoop-bundle" % parquetVersion

  lazy val hadoopCommon: ModuleID = hiveExcludes("org.apache.hadoop" % "hadoop-common" % hadoopVersion)
    .excludeAll(ExclusionRule(organization = "javax.servlet"))
    .excludeAll(ExclusionRule(organization = "javax.servlet.jsp"))
    .excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    .excludeAll(ExclusionRule(organization = "org.codehaus.jackson"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.databind"))
    .exclude("org.apache.hadoop", "hadoop-annotations")
    .exclude("org.apache.hadoop", "hadoop-auth")
    .excludeAll(ExclusionRule(organization = "org.eclipse.jetty"))

  lazy val hadoopAuth: ModuleID = hiveExcludes("org.apache.hadoop" % "hadoop-auth" % hadoopVersion)

  lazy val hadoopClient: ModuleID = hiveExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion)
  lazy val hadoopDistCp: ModuleID = hiveExcludes("org.apache.hadoop" % "hadoop-distcp" % hadoopVersion)
  lazy val hadoopMapreduceClientApp: ModuleID =
    hiveExcludes("org.apache.hadoop" % "hadoop-mapreduce-client-app" % hadoopVersion)
  lazy val hadoopMapreduceClientCommon: ModuleID =
    hiveExcludes("org.apache.hadoop" % "hadoop-mapreduce-client-common" % hadoopVersion)
  lazy val hadoopMapreduceClientJobClient: ModuleID =
    hiveExcludes("org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion)
  lazy val hadoopMapreduceClientShuffle: ModuleID =
    hiveExcludes("org.apache.hadoop" % "hadoop-mapreduce-client-shuffle" % hadoopVersion)
  lazy val hadoopMapReduce: ModuleID = hiveExcludes("org.apache.hadoop" % "hadoop-mapreduce" % hadoopVersion)
  lazy val hadoopMapReduceClient: ModuleID =
    hiveExcludes("org.apache.hadoop" % "hadoop-mapreduce-client" % hadoopVersion)
  lazy val hadoopMapReduceClientCore: ModuleID =
    hiveExcludes("org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion)
  lazy val hadoopExec:       ModuleID = hiveExcludes("org.apache.hadoop" % "hadoop-exec" % hadoopVersion)
  lazy val hadoopHdfs:       ModuleID = hiveExcludes("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion)
  lazy val hadoopHdfsClient: ModuleID = hiveExcludes("org.apache.hadoop" % "hadoop-hdfs-client" % hadoopVersion)

  lazy val kcql = ("com.datamountaineer" % "kcql" % kcqlVersion)
    .exclude("com.google.guava", "guava")

  lazy val calciteCore = hiveExcludes("org.apache.calcite" % "calcite-core" % calciteVersion)
    .excludeAll(ExclusionRule(organization = "io.swagger"))
    .excludeAll(ExclusionRule(organization = "io.netty"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.databind"))
    .excludeAll(ExclusionRule(organization = "org.apache.zookeeper"))
    .excludeAll(ExclusionRule(organization = "org.eclipse.jetty"))

  lazy val calciteLinq4J = "org.apache.calcite" % "calcite-linq4j" % calciteVersion

  lazy val s3Sdk     = "software.amazon.awssdk" % "s3"       % awsSdkVersion
  lazy val stsSdk    = "software.amazon.awssdk" % "sts"      % awsSdkVersion
  lazy val javaxBind = "javax.xml.bind"         % "jaxb-api" % javaxBindVersion

  lazy val jcloudsBlobstore    = "org.apache.jclouds"           % "jclouds-blobstore"    % jCloudsSdkVersion
  lazy val jcloudsProviderS3   = "org.apache.jclouds.provider"  % "aws-s3"               % jCloudsSdkVersion
  lazy val guava               = "com.google.guava"             % "guava"                % guavaVersion
  lazy val guice               = "com.google.inject"            % "guice"                % guiceVersion
  lazy val guiceAssistedInject = "com.google.inject.extensions" % "guice-assistedinject" % guiceVersion

  lazy val json4sNative  = "org.json4s" %% "json4s-native"  % json4sVersion
  lazy val json4sJackson = "org.json4s" %% "json4s-jackson" % json4sVersion
  val jacksonCore: ModuleID = "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion
  val jacksonDatabind: ModuleID =
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
  val jacksonDataformatCbor: ModuleID =
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % jacksonVersion
  val jacksonModuleScala: ModuleID =
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
  val woodstoxCore: ModuleID =
    "com.fasterxml.woodstox" % "woodstox-core" % "6.5.0"

  lazy val snakeYaml = "org.yaml"    % "snakeyaml" % snakeYamlVersion
  lazy val openCsv   = "com.opencsv" % "opencsv"   % openCsvVersion

  lazy val cassandraDriver = "com.datastax.cassandra" % "cassandra-driver-core" % cassandraDriverVersion
  lazy val jsonPath        = "com.jayway.jsonpath"    % "json-path"             % jsonPathVersion

  lazy val nettyAll          = "io.netty" % "netty-all"                    % nettyVersion
  lazy val nettyCommon       = "io.netty" % "netty-common"                 % nettyVersion
  lazy val nettyHandler      = "io.netty" % "netty-handler"                % nettyVersion
  lazy val nettyHandlerProxy = "io.netty" % "netty-handler-proxy"          % nettyVersion
  lazy val nettyCodec        = "io.netty" % "netty-codec"                  % nettyVersion
  lazy val nettyCodecHttp    = "io.netty" % "netty-codec-http"             % nettyVersion
  lazy val nettyCodecSocks   = "io.netty" % "netty-codec-socks"            % nettyVersion
  lazy val nettyResolver     = "io.netty" % "netty-resolver"               % nettyVersion
  lazy val nettyTransport    = "io.netty" % "netty-transport-native-epoll" % nettyVersion classifier "linux-x86_64"

  lazy val azureDocumentDb = "com.microsoft.azure" % "azure-documentdb" % azureDocumentDbVersion
  lazy val scalaParallelCollections =
    "org.scala-lang.modules" %% "scala-parallel-collections" % scalaParallelCollectionsVersion

  // testcontainers
  lazy val testContainersScala = "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion
  lazy val testContainersScalaCassandra =
    "com.dimafeng" %% "testcontainers-scala-cassandra" % testcontainersScalaVersion
  lazy val testContainersScalaMongodb = "com.dimafeng" %% "testcontainers-scala-mongodb" % testcontainersScalaVersion
  lazy val testContainersScalaToxiProxy =
    "com.dimafeng" %% "testcontainers-scala-toxiproxy" % testcontainersScalaVersion
  lazy val testContainersScalaElasticsearch =
    "com.dimafeng" %% "testcontainers-scala-elasticsearch" % testcontainersScalaVersion
  lazy val testContainersDebezium = ("io.debezium" % "debezium-testing-testcontainers" % "1.4.2.Final").excludeAll(
    ExclusionRule(organization = "com.google.inject"),
  )
  lazy val testcontainersCore          = "org.testcontainers" % "testcontainers" % testcontainersVersion
  lazy val testcontainersKafka         = "org.testcontainers" % "kafka"          % testcontainersVersion
  lazy val testcontainersCassandra     = "org.testcontainers" % "cassandra"      % testcontainersVersion
  lazy val testcontainersElasticsearch = "org.testcontainers" % "elasticsearch"  % testcontainersVersion
  lazy val testcontainersMongodb       = "org.testcontainers" % "mongodb"        % testcontainersVersion

  lazy val dropWizardMetrics = "io.dropwizard.metrics" % "metrics-jmx" % dropWizardMetricsVersion

  lazy val hazelCastAll = "com.hazelcast" % "hazelcast-all" % hazelCastVersion

  lazy val javaxCache = "javax.cache"  % "cache-api"     % javaxCacheVersion
  lazy val influx     = "org.influxdb" % "influxdb-java" % influxVersion

  lazy val jmsApi         = "javax.jms"           % "javax.jms-api"   % jmsApiVersion
  lazy val activeMq       = "org.apache.activemq" % "activemq-client" % activeMqVersion
  lazy val activeMqBroker = "org.apache.activemq" % "activemq-broker" % activeMqVersion

  lazy val protoc             = "com.github.os72"     % "protoc-jar"    % protocVersion
  lazy val googleProtobufJava = "com.google.protobuf" % "protobuf-java" % googleProtobufVersion
  lazy val googleProtobuf     = "com.google.protobuf" % "protobuf-java" % googleProtobufVersion % "protobuf"

  lazy val kuduClient = "org.apache.kudu" % "kudu-client" % kuduVersion

  lazy val mqttClient = "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % mqttVersion

  lazy val pulsar = ("org.apache.pulsar" % "pulsar-client-original" % pulsarVersion).excludeAll("org.apache.avro")

  lazy val httpClient       = "org.apache.httpcomponents" % "httpclient"              % httpClientVersion
  lazy val commonsBeanUtils = "commons-beanutils"         % "commons-beanutils"       % commonsBeanUtilsVersion
  lazy val commonsNet       = "commons-net"               % "commons-net"             % commonsNetVersion
  lazy val commonsCodec     = "commons-codec"             % "commons-codec"           % commonsCodecVersion
  lazy val commonsIO        = "commons-io"                % "commons-io"              % commonsIOVersion
  lazy val jsch             = "com.jcraft"                % "jsch"                    % jschVersion
  lazy val mina             = "org.apache.mina"           % "mina-core"               % minaVersion
  lazy val betterFiles      = "com.github.pathikrit"     %% "better-files"            % betterFilesVersion
  lazy val ftpServer        = "org.apache.ftpserver"      % "ftpserver-core"          % ftpServerVersion
  lazy val fakeSftpServer   = "com.github.stefanbirkner"  % "fake-sftp-server-lambda" % fakeSftpServerVersion

  lazy val hbaseClient = ("org.apache.hbase" % "hbase-client" % hbaseClientVersion)
    .excludeAll(ExclusionRule(organization = "org.apache.hadoop"))
    .excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
    .excludeAll(ExclusionRule(organization = "org.eclipse.jetty"))
    .excludeAll(ExclusionRule(organization = "io.netty"))

  lazy val zookeeperServer = "org.apache.zookeeper" % "zookeeper" % zookeeperServerVersion

  lazy val mongoDb = "org.mongodb" % "mongo-java-driver" % mongoDbVersion

  lazy val jedis = "redis.clients"        % "jedis" % jedisVersion
  lazy val gson  = "com.google.code.gson" % "gson"  % gsonVersion

  lazy val xz  = "org.tukaani"               % "xz"         % xzVersion
  lazy val lz4 = "org.lz4"                   % "lz4-java"   % lz4Version
  lazy val lzo = "com.hadoop.gplcompression" % "hadoop-lzo" % lzoVersion

  def hiveExcludes(moduleID: ModuleID): ModuleID =
    moduleID
      .excludeAll(ExclusionRule(organization = "org.mortbay.jetty"))
      .excludeAll(ExclusionRule(organization = "org.eclipse.jetty"))
      .excludeAll(ExclusionRule(organization = "org.apache.calcite"))
      .excludeAll(ExclusionRule(organization = "org.apache.parquet"))
      .excludeAll(ExclusionRule(organization = "org.apache.calcite.avatica"))
      .excludeAll(ExclusionRule(organization = "org.apache.hbase"))
      .excludeAll(ExclusionRule(organization = "org.apache.hadoop"))
      .excludeAll(ExclusionRule(organization = "io.netty"))
      .excludeAll(ExclusionRule(organization = "com.nibusds"))
      .exclude("com.fasterxml.jackson.core", "jackson-annotations")

  lazy val nimbusJoseJwt = hiveExcludes("com.nimbusds" % "nimbus-jose-jwt" % nimbusJoseJwtVersion)
  lazy val hiveJdbc      = hiveExcludes("org.apache.hive" % "hive-jdbc" % hiveVersion)
  lazy val hiveMetastore = hiveExcludes("org.apache.hive" % "hive-metastore" % hiveVersion)
  lazy val hiveExec      = hiveExcludes("org.apache.hive" % "hive-exec" % hiveVersion classifier "core")
  lazy val airCompressor = "io.airlift" % "aircompressor" % "0.21"

  // testcontainers module only
  lazy val festAssert = "org.easytesting" % "fest-assert" % "1.4"

  def elastic4sCore(v:    String): ModuleID = "com.sksamuel.elastic4s" %% "elastic4s-core"          % v
  def elastic4sClient(v:  String): ModuleID = "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % v
  def elastic4sTestKit(v: String): ModuleID = "com.sksamuel.elastic4s" %% "elastic4s-testkit"       % v
  def elastic4sHttp(v:    String): ModuleID = "com.sksamuel.elastic4s" %% "elastic4s-http"          % v

  def elasticSearch(v:         String): ModuleID = "org.elasticsearch"                 % "elasticsearch"   % v
  def elasticSearchAnalysis(v: String): ModuleID = "org.codelibs.elasticsearch.module" % "analysis-common" % v

  def jna(v: String): ModuleID = "net.java.dev.jna" % "jna" % v

}

trait Dependencies {

  import Versions._

  val loggingDeps: Seq[ModuleID] = Seq(
    "org.apache.logging.log4j" % "log4j-api"      % "2.19.0",
    "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.19.0",
    log4jToSlf4j,
    jclToSlf4j,
    logback,
    logbackCore,
    scalaLogging,
  )

  // resolvers
  val projectResolvers: Seq[MavenRepository] = commonResolvers

  val baseTestDeps: Seq[ModuleID] = loggingDeps ++ Seq(
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
    kafkaClients,
    junit,
  ) ++ enumeratum ++ circe ++ http4s

  //Specific modules dependencies
  val baseDeps: Seq[ModuleID] = loggingDeps ++ Seq(
    cats,
    catsLaws,
    catsEffect,
    catsEffectLaws,
    urlValidator,
    catsFree,
    guava,
    snakeYaml,
    commonsBeanUtils,
    httpClient,
    json4sNative,
    json4sJackson,
    jacksonCore,
    jacksonDatabind,
    jacksonModuleScala,
    jacksonDataformatCbor,
    avro4s,
    kcql,
    calciteCore,
    calciteLinq4J,
    kafkaConnectJson,
    confluentAvroConverter,
    confluentAvroData,
    confluentJsonSchemaSerializer,
  ) ++ enumeratum ++ circe ++ http4s

  //Specific modules dependencies

  val kafkaConnectS3Deps: Seq[ModuleID] = Seq(
    s3Sdk,
    stsSdk,
    parquetAvro,
    parquetHadoop,
    hadoopCommon,
    hadoopMapReduce,
    hadoopMapReduceClient,
    hadoopMapReduceClientCore,
    javaxBind,
    jcloudsBlobstore,
    jcloudsProviderS3,
    openCsv,
    guice,
    guiceAssistedInject,
  )

  val compressionCodecDeps: Seq[ModuleID] = Seq(xz, lzo, lz4)

  val kafkaConnectS3TestDeps: Seq[ModuleID] = baseTestDeps ++ compressionCodecDeps :+ testContainersScala

  val kafkaConnectS3FuncTestDeps: Seq[ModuleID] = baseTestDeps ++ compressionCodecDeps :+ s3Sdk

  val kafkaConnectCassandraDeps: Seq[ModuleID] = Seq(
    cassandraDriver,
    jsonPath,
    nettyTransport,
    //dropWizardMetrics
  )

  val kafkaConnectCassandraTestDeps: Seq[ModuleID] =
    baseTestDeps ++ Seq(testContainersScala, testContainersScalaCassandra)

  val kafkaConnectHazelCastDeps: Seq[ModuleID] = Seq(
    hazelCastAll,
    javaxCache,
  )

  val kafkaConnectAzureDocumentDbDeps: Seq[ModuleID] = Seq(azureDocumentDb,
  //, scalaParallelCollections
  )

  val kafkaConnectInfluxDbDeps: Seq[ModuleID] = Seq(influx, avro4s, avro4sJson)

  val kafkaConnectJmsDeps: Seq[ModuleID] = Seq(
    jmsApi,
    confluentProtobufConverter,
    protoc,
    googleProtobuf,
  )

  val kafkaConnectJmsTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(
    activeMq,
    activeMqBroker,
  )

  val kafkaConnectKuduDeps: Seq[ModuleID] = Seq(kuduClient)

  val kafkaConnectMqttDeps: Seq[ModuleID] = Seq(mqttClient, avro4s, avro4sJson) ++ bouncyCastle

  val kafkaConnectMqttTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(testContainersScala, testContainersScalaToxiProxy)

  val kafkaConnectPulsarDeps: Seq[ModuleID] = Seq(pulsar, avro4s, avro4sJson, avro, avroProtobuf)

  def elasticCommonDeps(v: ElasticVersions): Seq[ModuleID] = Seq(
    elastic4sCore(v.elastic4sVersion),
    jna(v.jnaVersion),
    elasticSearch(v.elasticSearchVersion),
    //elasticSearchAnalysis(v.elasticSearchVersion)
  )

  def elasticTestCommonDeps(v: ElasticVersions): Seq[ModuleID] = Seq(
    elastic4sTestKit(v.elastic4sVersion),
    testContainersScala,
    testContainersScalaElasticsearch,
  )

  val kafkaConnectElastic6Deps: Seq[ModuleID] =
    elasticCommonDeps(Elastic6Versions) ++ Seq(elastic4sHttp(Elastic6Versions.elastic4sVersion))

  val kafkaConnectElastic6TestDeps: Seq[ModuleID] = baseTestDeps ++ elasticTestCommonDeps(Elastic6Versions)

  val kafkaConnectElastic7Deps: Seq[ModuleID] =
    elasticCommonDeps(Elastic7Versions) ++ Seq(elastic4sClient(Elastic7Versions.elastic4sVersion))

  val kafkaConnectElastic7TestDeps: Seq[ModuleID] = baseTestDeps ++ elasticTestCommonDeps(Elastic7Versions)

  val kafkaConnectFtpDeps: Seq[ModuleID] = Seq(commonsNet, commonsCodec, commonsIO, jsch)

  val kafkaConnectFtpTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(mina, betterFiles, ftpServer, fakeSftpServer)

  val kafkaConnectHbaseDeps: Seq[ModuleID] = Seq(
    nettyAll,
    hadoopCommon,
    hadoopHdfs,
    hadoopHdfsClient,
    hbaseClient,
    nimbusJoseJwt,
  )

  val kafkaConnectHiveDeps: Seq[ModuleID] = Seq(
    airCompressor,
    nettyAll,
    parquetAvro,
    parquetColumn,
    parquetEncoding,
    parquetHadoop,
    parquetHadoopBundle,
    hiveJdbc,
    hiveExec,
    hadoopCommon,
    hadoopAuth,
    hadoopHdfs,
    hadoopHdfsClient,
    hadoopMapReduce,
    hadoopMapReduceClient,
    hadoopMapReduceClientCore,
    hadoopClient,
    hadoopDistCp,
    hadoopMapreduceClientApp,
    hadoopMapreduceClientCommon,
    hadoopMapreduceClientJobClient,
    hadoopMapreduceClientShuffle,
    nimbusJoseJwt,
  )

  val kafkaConnectHiveTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(testContainersScala)

  val kafkaConnectMongoDbDeps: Seq[ModuleID] = Seq(json4sJackson, json4sNative, mongoDb)

  val kafkaConnectMongoDbTestDeps: Seq[ModuleID] = baseTestDeps ++ Seq(testContainersScalaMongodb)

  val kafkaConnectRedisDeps: Seq[ModuleID] = Seq(jedis)

  val kafkaConnectRedisTestDeps: Seq[ModuleID] = (baseTestDeps ++ Seq(testContainersScala, gson))
    .map {
      moduleId: ModuleID => moduleId.extra("scope" -> "test")
    }

  val testCommonDeps: Seq[ModuleID] = baseDeps ++ Seq(
    scalatest,
    json4sJackson,
    json4sNative,
    testcontainersCore,
    testContainersDebezium,
    testcontainersKafka,
    testcontainersCassandra,
    testcontainersElasticsearch,
    testcontainersMongodb,
    jedis,
    mongoDb,
  )

  val nettyOverrides: Seq[ModuleID] = Seq(
    nettyCommon,
    nettyHandler,
    nettyHandlerProxy,
    nettyCodec,
    nettyCodecHttp,
    nettyCodecSocks,
    nettyResolver,
    nettyTransport,
  )

  val avroOverrides: Seq[ModuleID] = Seq(
    avro,
    avroProtobuf,
  )

}
