// Activate the following only when needed to use specific tasks like `whatDependsOn` etc...
//addDependencyTreePlugin

addSbtPlugin("org.scalameta"     % "sbt-scalafmt"       % "2.5.0")
addSbtPlugin("org.scoverage"     % "sbt-scoverage"      % "1.9.3")
addSbtPlugin("de.heikoseeberger" % "sbt-header"         % "5.9.0")
addSbtPlugin("com.typesafe.sbt"  % "sbt-license-report" % "1.2.0")

addSbtPlugin("net.vonbuchholtz" % "sbt-dependency-check" % "4.0.0")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.14")

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.2.0")

//addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.13.2")
