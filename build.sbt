import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.github.io/zio-s3/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer("regis-leray", "Regis Leray", "regis.leray@gmail.com", url("https://github.com/regis-leray"))
    ),
    Test / fork := true,
    parallelExecution in Test := false,
    publishMavenStyle := true,
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(url("https://github.com/zio/zio-s3/"), "scm:git:git@github.com:zio/zio-s3.git")
    )
  )
)

ThisBuild / publishTo := sonatypePublishToBundle.value

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

lazy val `zio-s3` = project
  .in(file("."))
  .settings(
    skip in publish := true,
    libraryDependencies ++= Seq(
      "dev.zio"                %% "zio"                         % "1.0.0-RC17",
      "dev.zio"                %% "zio-streams"                 % "1.0.0-RC17",
      "dev.zio"                %% "zio-nio"                     % "0.4.0",
      "dev.zio"                %% "zio-interop-java"            % "1.1.0.0-RC6",
      "dev.zio"                %% "zio-interop-reactivestreams" % "1.0.3.5-RC2",
      "software.amazon.awssdk" % "s3"                           % "2.10.50",
      "dev.zio"                %% "zio-test"                    % "1.0.0-RC17" % Test,
      "dev.zio"                %% "zio-test-sbt"                % "1.0.0-RC17" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val docs = project
  .in(file("zio-s3-docs"))
  .settings(
    skip.in(publish) := true,
    moduleName := "zio-s3-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "1.0.0-RC17"
    ),
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(`zio-s3`),
    target in (ScalaUnidoc, unidoc) := (baseDirectory in LocalRootProject).value / "website" / "static" / "api",
    cleanFiles += (target in (ScalaUnidoc, unidoc)).value,
    docusaurusCreateSite := docusaurusCreateSite.dependsOn(unidoc in Compile).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(unidoc in Compile).value
  )
  .dependsOn(`zio-s3`)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
