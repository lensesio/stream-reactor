import Dependencies.Versions.kafkaVersion
import Dependencies.betterMonadicFor
import Dependencies.classGraph
import Dependencies.globalExcludeDeps
import Dependencies.googleProtobuf
import Dependencies.googleProtobufJava
import Dependencies.hadoopCommon
import Dependencies.hadoopMapReduceClientCore
import Dependencies.jsonSmart
import Dependencies.msal4j
import Dependencies.msal4jPersistenceExtension
import Dependencies.nimbusJoseJwt
import Dependencies.woodstoxCore
import Dependencies.zstd
import com.eed3si9n.jarjarabrams.ShadeRule
import com.simplytyped.Antlr4Plugin
import com.simplytyped.Antlr4Plugin.autoImport.Antlr4
import com.simplytyped.Antlr4Plugin.autoImport.antlr4PackageName
import com.simplytyped.Antlr4Plugin.autoImport.antlr4Version
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.*
import sbt.Keys.*
import sbt.Package.ManifestAttributes
import sbt.Compile
import sbt.Def
import sbt.*
import sbt.internal.util.ManagedLogger
import sbtassembly.Assembly.JarEntry
import sbtassembly.AssemblyKeys.*
import sbtassembly.CustomMergeStrategy
import sbtassembly.MergeStrategy
import sbtassembly.PathList

import java.io.File
import java.io.File
import java.io.File
import java.io.IOException
import java.net.HttpURLConnection
import java.net.HttpURLConnection
import java.net.MalformedURLException
import java.net.URL
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Year
import scala.sys.process.*
import scala.util.Try

object Settings extends Dependencies {

  // keep the SNAPSHOT version numerically higher than the latest release.
  val majorVersion        = "8.0"
  val nextSnapshotVersion = "8.1"

  val artifactVersion: String = {
    val maybeGithubRunId = sys.env.get("github_run_id")
    val maybeVersion     = sys.env.get("VERSION")
    val snapshotTag      = sys.env.get("SNAPSHOT_TAG")
    (maybeVersion, maybeGithubRunId) match {
      case (_, Some(patchVersion)) => majorVersion + "." + patchVersion
      case (Some(v), _)            => v
      case _                       => s"$nextSnapshotVersion-${snapshotTag.fold("SNAPSHOT")(t => s"$t-SNAPSHOT")}"
    }
  }

  val currentYear = Year.now().getValue()

  object ScalacFlags {
    val availableProcessors: String = java.lang.Runtime.getRuntime.availableProcessors.toString

    val commonOptions = Seq(
      // standard settings
      "-release:11",
      "-encoding",
      "UTF-8",
      "-unchecked",
      "-deprecation",
      "-explaintypes",
      "-feature",
      // language features
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-language:postfixOps",
      // private options
      "-Ybackend-parallelism",
      availableProcessors,
      "-Yrangepos", // required by SemanticDB compiler plugin
    )

    val lintings = List(
      "-Xlint:adapted-args",
      "-Xlint:constant",
      "-Xlint:delayedinit-select",
      "-Xlint:doc-detached",
      "-Xlint:inaccessible",
      "-Xlint:infer-any",
      "-Xlint:missing-interpolator",
      "-Xlint:nullary-unit",
      "-Xlint:option-implicit",
      "-Xlint:package-object-classes",
      "-Xlint:poly-implicit-overload",
      "-Xlint:private-shadow",
      "-Xlint:stars-align",
      "-Xlint:type-parameter-shadow",
    )

    object Scala213 {
      val WarnUnusedImports = "-Wunused:imports"
      val FatalWarnings     = "-Werror"
      val ValueDiscard      = "-Wvalue-discard"

      val warnings = List(
        FatalWarnings,
        ValueDiscard,
        WarnUnusedImports,
        "-Wdead-code",
        "-Wextra-implicit",
        "-Wmacros:after",
        "-Wnumeric-widen",
        "-Wunused:implicits",
        "-Wunused:locals",
        "-Wunused:patvars",
        "-Wunused:privates",
      )

      val options: Seq[String] = commonOptions ++ List(
        "-Xcheckinit",
      ) ++ warnings ++ lintings
    }
  }

  private val commonSettings: Seq[Setting[_]] = Seq(
    organization := "io.lenses",
    version := artifactVersion,
    scalaOrganization := Dependencies.scalaOrganization,
    scalaVersion := Dependencies.scalaVersion,
    headerEmptyLine := false,
    isSnapshot := artifactVersion.contains("SNAPSHOT"),
    javacOptions ++= Seq("--release", "11"),
    packageOptions := Seq(
      ManifestAttributes(
        ("Git-Commit-Hash", "git rev-parse HEAD".!!.trim),
        ("Git-Repo", "git config --get remote.origin.url".!!.trim),
        ("Git-Tag", sys.env.getOrElse("SNAPSHOT_TAG", "n/a")),
        ("Kafka-Version", kafkaVersion),
        ("StreamReactor-Version", artifactVersion),
        ("StreamReactor-Docs", "https://docs.lenses.io/5.0/integrations/connectors/stream-reactor/"),
      ),
    ),
    betterMonadicFor,
  )

  val settings: Seq[Setting[_]] = commonSettings ++ Seq(
    scalacOptions ++= ScalacFlags.Scala213.options,
    Compile / console / scalacOptions := ScalacFlags.commonOptions,
    Global / cancelable := true,
    Compile / fork := true,
    Compile / connectInput := true,
    Compile / outputStrategy := Some(StdoutOutput),
    resolvers ++= projectResolvers,
    crossScalaVersions := Dependencies.supportedScalaVersions,
    excludeDependencies ++= globalExcludeDeps,
    headerLicense := Some(HeaderLicense.ALv2(s"2017-$currentYear", "Lenses.io Ltd")),
  )

  implicit final class ParallelDestroyer(project: Project) {
    def disableParallel(): Project =
      project.settings(settings ++ Seq(
        Test / parallelExecution := false,
        IntegrationTest / parallelExecution := false,
      ))
  }

  implicit final class AssemblyConfigurator(project: Project) {

    val excludeFilePatterns = Set(".MF", ".RSA", ".DSA", ".SF")

    def excludeFileFilter(p: String): Boolean =
      excludeFilePatterns.exists(p.endsWith)

    val excludePatterns = Set(
      "kafka-client",
      "kafka-connect-json",
      "hadoop-yarn",
      "org.apache.kafka",
      "zookeeper",
      "log4j",
      "junit",
    )

    def configureAssembly(overrideNetty: Boolean): Project = {
      val nettyDepOverrides = if (overrideNetty) nettyOverrides else Seq.empty
      project.settings(
        settings ++ Seq(
          assembly / assemblyOutputPath := {
            file(target.value + "/libs/").mkdirs()
            file(target.value + "/libs/" + (assembly / assemblyJarName).value)
          },
          assembly / assemblyExcludedJars := {
            val cp: Classpath = (assembly / fullClasspath).value
            cp filter { f =>
              excludePatterns.exists(f.data.getName.contains) && (!f.data.getName.contains("slf4j"))
            }
          },
          assembly / assemblyMergeStrategy := {
            case PathList("META-INF", "maven", _ @_*) =>
              CustomMergeStrategy("keep-only-fresh-maven-descriptors", 1) {
                assemblyDependency =>
                  val keepDeps = assemblyDependency.collect {
                    case dependency @ (_: sbtassembly.Assembly.Project) =>
                      JarEntry(dependency.target, dependency.stream)
                  }
                  Right(keepDeps)
              }
            case PathList("META-INF", "maven", _ @_*) =>
              MergeStrategy.discard
            case PathList("META-INF", "MANIFEST.MF") =>
              MergeStrategy.discard
            case p if excludeFileFilter(p) =>
              MergeStrategy.discard
            case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.discard
            case _                                                   => MergeStrategy.first
          },
          assembly / assemblyShadeRules ++= Seq(
            // ShadeRule.rename("org.apache.avro.**" -> "lshaded.apache.avro.@1").inAll,
            // ShadeRule.rename("io.confluent.**" -> "lshaded.confluent.@1").inAll,
            ShadeRule.rename("com.fasterxml.**" -> "lshaded.fasterxml.@1").inAll,
            ShadeRule.rename("org.apache.hadoop" -> "lshaded.apache.hadoop").inAll,
            ShadeRule.rename("org.antlr.**" -> "lshaded.antlr.@1").inAll,
          ),
          dependencyOverrides ++= Seq(
            googleProtobuf,
            googleProtobufJava,
            hadoopCommon,
            hadoopMapReduceClientCore,
            woodstoxCore,
            jsonSmart,
            nimbusJoseJwt,
            zstd,
            classGraph,
            msal4j,
            msal4jPersistenceExtension,
          ) ++ nettyDepOverrides ++ avroOverrides,
        ),
      )
    }
  }

  implicit final class ProjectFilterOps(project: Project) {

    def containsDir(dir: String): Boolean = {
      val files = Files.find(
        Paths.get(project.base.getAbsolutePath),
        2,
        (p, _) => p.toFile.getAbsolutePath.endsWith(dir),
      ).toArray
      files.nonEmpty
    }
  }

  val IntegrationTest: Configuration = config("it").extend(Test).describedAs("Runs integration tests")
  val FunctionalTest:  Configuration = config("fun").extend(Test).describedAs("Runs system and acceptance tests")

  implicit final class MavenDescriptorConfigurator(project: Project) {

    val generateMetaInfMaven = taskKey[Unit]("Generate META-INF/maven directory")

    def configureMavenDescriptor(): Project =
      project
        .settings(
          generateMetaInfMaven := {
            val log = streams.value.log

            val targetDirBase = (Compile / crossTarget).value / "classes" / "META-INF" / "maven"

            val allModuleIds: Map[ModuleID, String] = update
              .value
              .configuration(Compile)
              .toVector
              .flatMap(_.modules)
              .map {
                e: ModuleReport => e.module -> e.artifacts.headOption
              }
              .collect {
                case (moduleId, Some((moduleJar, moduleFile))) =>
                  moduleId ->
                    moduleJar.url.get.toString
                      .reverse
                      .replaceFirst(".jar".reverse, ".pom".reverse)
                      .reverse
              }.toMap

            for ((moduleId, pomUrl) <- allModuleIds) {

              log.info(s"Processing ${moduleId.name}")

              val groupId    = moduleId.organization
              val artifactId = moduleId.name
              val version    = moduleId.revision
              val targetDir  = targetDirBase / groupId / artifactId
              targetDir.mkdirs()

              val propertiesFileChanged = createPomPropertiesIfChanged(groupId, artifactId, version, targetDir)
              if (propertiesFileChanged) {
                createPomXml(log, targetDir, pomUrl)
              }
            }

          },
          (Compile / compile) := ((Compile / compile) dependsOn generateMetaInfMaven).value,
        )

    private def createPomXml(log: ManagedLogger, targetDir: File, pomUrl: String): Option[File] = {
      val pomFile = targetDir / "pom.xml"

      try {
        val url        = new URL(pomUrl)
        val connection = url.openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("GET")

        if (connection.getResponseCode == HttpURLConnection.HTTP_OK && connection.getContentType == "text/xml") {
          val inputStream = connection.getInputStream
          try {
            val pomContent = new String(inputStream.readAllBytes())
            IO.write(pomFile, pomContent)
            log.info(s"Successfully retrieved and saved POM from $pomUrl to $pomFile")
            Some(pomFile)

          } finally {
            inputStream.close()
          }
        } else {
          log.error(
            s"Failed to retrieve POM from $pomUrl. HTTP Status: ${connection.getResponseCode}, Content Type: ${connection.getContentType}",
          )
          Option.empty
        }
      } catch {
        case e: MalformedURLException =>
          log.error(s"Invalid URL: $pomUrl")
          Option.empty
        case e: IOException =>
          log.error(s"Error while retrieving POM from $pomUrl: ${e.getMessage}")
          Option.empty
      }

    }

    private def createPomPropertiesIfChanged(
      groupId:    String,
      artifactId: String,
      version:    String,
      targetDir:  File,
    ): Boolean = {
      val propertiesFile = targetDir / "pom.properties"
      val propertiesContent =
        s"""version=$version
           |groupId=$groupId
           |artifactId=$artifactId
                  """.stripMargin

      val alreadyExists = Try(IO.read(propertiesFile))
        .toOption
        .contains(propertiesContent)

      if (!alreadyExists) {
        IO.write(propertiesFile, propertiesContent)
      }

      !alreadyExists

    }
  }

  sealed abstract class TestConfigurator(
    project:         Project,
    config:          Configuration,
    defaultSettings: Seq[Def.Setting[_]] = Defaults.testSettings,
  ) {

    protected def configure(
      requiresFork: Boolean,
      testDeps:     Seq[ModuleID],
    ): Project =
      project
        .configs(config)
        .settings(
          libraryDependencies ++= testDeps.map(d => d % config),
          inConfig(config) {
            Defaults.testSettings ++
              Seq(
                config / unmanagedSourceDirectories ++= Seq(project.base / "src" / config.name / "scala"),
                config / unmanagedSourceDirectories ++= (Test / sourceDirectories).value,
                config / unmanagedResourceDirectories ++= (Test / resourceDirectories).value,
              )

            defaultSettings ++ Seq(
              fork := requiresFork,
              testFrameworks := Seq(sbt.TestFrameworks.ScalaTest),
              classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
              javaOptions += "-Dartifact.dir=" + (project.base / "target" / "libs").getAbsolutePath,
            ) ++ org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings

          },
        )

  }

  implicit final class UnitTestConfigurator(project: Project) extends TestConfigurator(project, Test) {

    def configureTests(
      testDeps: Seq[ModuleID],
    ): Project =
      configure(requiresFork = false, testDeps)
  }

  implicit final class IntegrationTestConfigurator(project: Project)
      extends TestConfigurator(project, IntegrationTest) {

    def configureIntegrationTests(testDeps: Seq[ModuleID]): Project =
      configure(requiresFork = false, testDeps)
  }

  implicit final class FunctionalTestConfigurator(project: Project) extends TestConfigurator(project, FunctionalTest) {

    def configureFunctionalTests(extraDeps: Seq[ModuleID] = Seq.empty): Project = {
      val proj = configure(requiresFork = true, testCommonDeps ++ extraDeps)
      sys.env.get("CONNECT_IMAGE_VERSION").fold(proj)(vers =>
        proj.settings(
          FunctionalTest / envVars := Map(
            "CONFLUENT_VERSION" -> vers,
          ),
        ),
      )

    }

  }

  implicit final class AntlrConfigurator(project: Project) {

    def configureAntlr(): Project =
      project
        .enablePlugins(Antlr4Plugin)
        .settings(
          settings ++ Seq(
            Antlr4 / antlr4PackageName := Some("io.lenses.kcql.antlr4"),
            Antlr4 / antlr4Version := Dependencies.Versions.antlr4Version,
          ),
        )
  }

}
