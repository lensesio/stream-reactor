// Activate the following only when needed to use specific tasks like `whatDependsOn` etc...
//addDependencyTreePlugin

addSbtPlugin("org.scalameta"     % "sbt-scalafmt"       % "2.4.6")
addSbtPlugin("org.scoverage"     % "sbt-scoverage"      % "1.9.3")
addSbtPlugin("de.heikoseeberger" % "sbt-header"         % "5.7.0")
addSbtPlugin("ch.epfl.scala"     % "sbt-scalafix"       % "0.10.0")
addSbtPlugin("com.typesafe.sbt"  % "sbt-license-report" % "1.2.0")

addSbtPlugin("net.vonbuchholtz"    % "sbt-dependency-check" % "4.0.0")
libraryDependencies += "org.slf4j" % "slf4j-nop"            % "1.7.36"

addSbtPlugin("com.eed3si9n" % "sbt-projectmatrix" % "0.9.0")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.14")
