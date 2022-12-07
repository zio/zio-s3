addSbtPlugin("org.scalameta"      % "sbt-mdoc"                  % "2.3.3")
addSbtPlugin("com.eed3si9n"       % "sbt-unidoc"                % "0.4.3")
addSbtPlugin("ch.epfl.scala"      % "sbt-bloop"                 % "1.5.6")
addSbtPlugin("ch.epfl.scala"      % "sbt-scalafix"              % "0.10.0")
addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo"             % "0.11.0")
addSbtPlugin("com.geirsson"       % "sbt-ci-release"            % "1.5.5")
addSbtPlugin("com.github.cb372"   % "sbt-explicit-dependencies" % "0.2.16")
addSbtPlugin("org.portable-scala" % "sbt-crossproject"          % "1.2.0")
addSbtPlugin("de.heikoseeberger"  % "sbt-header"                % "5.6.5")
addSbtPlugin("org.scalameta"      % "sbt-scalafmt"              % "2.4.6")
addSbtPlugin("dev.zio"            % "zio-sbt-website"           % "0.0.0+84-6fd7d64e-SNAPSHOT")

resolvers += Resolver.sonatypeRepo("public")
