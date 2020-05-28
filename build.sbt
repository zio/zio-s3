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

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion = "1.0.0-RC20"

lazy val `zio-s3` = project
  .in(file("."))
  .settings(stdSettings("zio-s3"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio"                %% "zio"                         % zioVersion,
      "dev.zio"                %% "zio-streams"                 % zioVersion,
      "dev.zio"                %% "zio-nio"                     % "1.0.0-RC7",
      "dev.zio"                %% "zio-interop-reactivestreams" % "1.0.3.5-RC10",
      "org.scala-lang.modules" %% "scala-collection-compat"     % "2.1.6",
      "software.amazon.awssdk" % "s3"                           % "2.13.26",
      "dev.zio"                %% "zio-test"                    % zioVersion % Test,
      "dev.zio"                %% "zio-test-sbt"                % zioVersion % Test
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
      "dev.zio" %% "zio" % zioVersion
    ),
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inProjects(`zio-s3`),
    target in (ScalaUnidoc, unidoc) := (baseDirectory in LocalRootProject).value / "website" / "static" / "api",
    cleanFiles += (target in (ScalaUnidoc, unidoc)).value,
    docusaurusCreateSite := docusaurusCreateSite.dependsOn(unidoc in Compile).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(unidoc in Compile).value
  )
  .dependsOn(`zio-s3`)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
