// Activate the following only when needed to use specific tasks like `whatDependsOn` etc...
//addDependencyTreePlugin

addSbtPlugin("org.scalameta"     % "sbt-scalafmt"       % "2.5.2")
addSbtPlugin("org.scoverage"     % "sbt-scoverage"      % "2.0.11")
addSbtPlugin("de.heikoseeberger" % "sbt-header"         % "5.10.0")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.14")

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

addSbtPlugin("com.github.sbt" % "sbt-license-report" % "1.6.1")

//addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.13.2")

addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.8.3")
