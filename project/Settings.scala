import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import sbt.Keys._
import sbt._
import sbt.internal.ProjectMatrix
import sbtassembly.AssemblyKeys._
import sbtassembly.MergeStrategy
import sbtassembly.PathList
import scalafix.sbt.ScalafixPlugin.autoImport.scalafixSemanticdb

import java.nio.file.Files
import java.nio.file.Paths
import java.util.Calendar

object Settings extends Dependencies {
  // keep the SNAPSHOT version numerically higher than the latest release.
  val majorVersion        = "1.0"
  val nextSnapshotVersion = "1.1"

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

  val manifestSection: Package.JarManifest = {
    import java.util.jar.Attributes
    import java.util.jar.Manifest
    val manifest      = new Manifest
    val newAttributes = new Attributes()
    newAttributes.put(new Attributes.Name("version"), majorVersion)
    manifest.getEntries.put("celonis", newAttributes)
    Package.JarManifest(manifest)
  }

  val licenseHeader: String = {
    val currentYear = Calendar.getInstance().get(Calendar.YEAR)
    s"Copyright 2017-$currentYear Celonis Ltd"
  }

  object ScalacFlags {
    val availableProcessors: String = java.lang.Runtime.getRuntime.availableProcessors.toString

    val commonOptions = Seq(
      // standard settings
      "-target:jvm-1.8",
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
      "-Yrangepos",                 // required by SemanticDB compiler plugin
      "-P:semanticdb:synthetics:on",// required by scala-collection-migrations
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
    organization := "com.celonis.kafka.connect",
    version := artifactVersion,
    scalaOrganization := scalaOrganizationUsed,
    scalaVersion := scalaVersionUsed,
    headerLicense := Some(HeaderLicense.Custom(licenseHeader)),
    headerEmptyLine := false,
    isSnapshot := artifactVersion.contains("SNAPSHOT"),
    kindProjectorPlugin,
    betterMonadicFor,
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
  )

  val settings: Seq[Setting[_]] = commonSettings ++ Seq(
    scalacOptions ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        //case Some((2, n)) if n <= 12 =>
        //  ScalacFlags.Scala212.options
        case _ =>
          ScalacFlags.Scala213.options
      }
    },
    Compile / console / scalacOptions := ScalacFlags.commonOptions,
    Global / cancelable := true,
    Compile / fork := true,
    Compile / connectInput := true,
    Compile / outputStrategy := Some(StdoutOutput),
    resolvers ++= projectResolvers,
    crossScalaVersions := supportedScalaVersionsUsed,
  )

  implicit final class ParallelDestroyer(project: ProjectMatrix) {
    def disableParallel(): ProjectMatrix =
      project.settings(settings ++ Seq(
        Test / parallelExecution := false,
        IntegrationTest / parallelExecution := false,
      ))
  }

  implicit final class AssemblyConfigurator(project: ProjectMatrix) {

    val excludeFilePatterns = Set(".MF", ".RSA", ".DSA", ".SF")

    def excludeFileFilter(p: String): Boolean =
      excludeFilePatterns.exists(p.endsWith)

    val excludePatterns = Set(
      "kafka-client",
      "hadoop-yarn",
      "avro",
      "kafka",
      "confluent",
      "zookeeper",
      "log4j",
      "junit",
    )

    def configureAssembly(): ProjectMatrix =
      project.settings(
        settings ++ Seq(
          assembly / assemblyOutputPath := file(target.value + "/libs/" + (assembly / assemblyJarName).value),
          assembly / assemblyExcludedJars := {
            val cp: Classpath = (assembly / fullClasspath).value
            cp filter { f =>
              excludePatterns.exists(f.data.getName.contains)
            }
          },
          assembly / assemblyMergeStrategy := {
            case PathList("META-INF", "MANIFEST.MF")                 => MergeStrategy.discard
            case p if excludeFileFilter(p)                           => MergeStrategy.discard
            case PathList(ps @ _*) if ps.last == "module-info.class" => MergeStrategy.discard
            case _                                                   => MergeStrategy.first
          },
        ),
      )
  }

  implicit final class ProjectFilterOps(project: ProjectMatrix) {

    def containsDir(dir: String): Boolean = {
      val files = Files.find(
        Paths.get(project.base.getAbsolutePath),
        2,
        (p, _) => p.toFile.getAbsolutePath.endsWith(dir),
      ).toArray
      files.nonEmpty
    }
  }

  val FunctionalTest: Configuration = config("fun").extend(Test).describedAs("Runs system and acceptance tests")

  sealed abstract class TestConfigurator(
    project:         ProjectMatrix,
    config:          Configuration,
    defaultSettings: Seq[Def.Setting[_]] = Defaults.testSettings,
  ) {

    protected def configure(requiresFork: Boolean, testDeps: Seq[ModuleID]): ProjectMatrix =
      project
        .configs(config)
        .settings(
          libraryDependencies ++= testDeps.map(d => d % config),
          inConfig(config)(
            defaultSettings ++ Seq(
              fork := requiresFork,
              testFrameworks := Seq(sbt.TestFrameworks.ScalaTest),
              classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
            ),
          ),
        )
  }

  implicit final class UnitTestConfigurator(project: ProjectMatrix) extends TestConfigurator(project, Test) {

    def configureTests(testDeps: Seq[ModuleID]): ProjectMatrix =
      configure(requiresFork = false, testDeps)
  }

  implicit final class IntegrationTestConfigurator(project: ProjectMatrix)
      extends TestConfigurator(project, IntegrationTest) {

    def configureIntegrationTests(testDeps: Seq[ModuleID]): ProjectMatrix =
      configure(requiresFork = false, testDeps)
  }

  implicit final class FunctionalTestConfigurator(project: ProjectMatrix)
      extends TestConfigurator(project, FunctionalTest) {

    def configureFunctionalTests(): ProjectMatrix =
      configure(requiresFork = true, testCommonDeps)
  }

}
