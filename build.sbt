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
    (Test / parallelExecution) := false,
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

val zioVersion = "1.0.13"
val awsVersion = "2.16.61"

lazy val `zio-s3` = project
  .in(file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(buildInfoSettings("zio.s3"))
  .settings(stdSettings("zio-s3"))
  .settings(dottySettings)
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio"               %% "zio"                         % zioVersion,
      "dev.zio"               %% "zio-streams"                 % zioVersion,
      "dev.zio"               %% "zio-nio"                     % "1.0.0-RC12",
      "dev.zio"               %% "zio-interop-reactivestreams" % "1.3.12",
      "software.amazon.awssdk" % "s3"                          % awsVersion,
      "software.amazon.awssdk" % "sts"                         % awsVersion,
      "dev.zio"               %% "zio-test"                    % zioVersion % Test,
      "dev.zio"               %% "zio-test-sbt"                % zioVersion % Test
    ),
    libraryDependencies ++= {
      if (scalaVersion.value == ScalaDotty)
        Seq()
      else
        Seq("org.scala-lang.modules" %% "scala-collection-compat" % "2.7.0")
    },
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val docs = project
  .in(file("zio-s3-docs"))
  .settings(
    skip / publish := true,
    moduleName := "zio-s3-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion
    ),
    (ScalaUnidoc / unidoc / unidocProjectFilter) := inProjects(`zio-s3`),
    ScalaUnidoc / unidoc / target := (LocalRootProject / baseDirectory).value / "website" / "static" / "api",
    cleanFiles += (ScalaUnidoc / unidoc / target).value,
    docusaurusCreateSite := docusaurusCreateSite.dependsOn(Compile / unidoc).value,
    docusaurusPublishGhpages := docusaurusPublishGhpages.dependsOn(Compile / unidoc).value
  )
  .dependsOn(`zio-s3`)
  .enablePlugins(MdocPlugin, DocusaurusPlugin, ScalaUnidocPlugin)
