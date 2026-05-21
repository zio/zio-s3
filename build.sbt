import BuildHelper.{ buildInfoSettings => _, stdSettings => _, _ }

enablePlugins(ZioSbtEcosystemPlugin, ZioSbtCiPlugin)

inThisBuild(
  List(
    name := "ZIO S3",
    developers := List(
      Developer("regis-leray", "Regis Leray", "regis.leray@gmail.com", url("https://github.com/regis-leray"))
    ),
    ciEnabledBranches := Seq("series/2.x"),
    ciBackgroundJobs := Seq("docker compose -f docker-compose.yml up -d --build"),
    Test / fork := true,
    (Test / parallelExecution) := false
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion = "2.1.26"
val awsVersion = "2.31.45"

lazy val root =
  project.in(file(".")).settings(publish / skip := true).aggregate(`zio-s3`, docs)

lazy val `zio-s3` = project
  .in(file("zio-s3"))
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildHelper.buildInfoSettings("zio.s3"))
  .settings(BuildHelper.stdSettings("zio-s3"))
  .settings(dottySettings)
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio"               %% "zio"                         % zioVersion,
      "dev.zio"               %% "zio-streams"                 % zioVersion,
      "dev.zio"               %% "zio-nio"                     % "2.0.2",
      "dev.zio"               %% "zio-interop-reactivestreams" % "2.0.2",
      "software.amazon.awssdk" % "s3"                          % awsVersion,
      "software.amazon.awssdk" % "sts"                         % awsVersion,
      "dev.zio"               %% "zio-test"                    % zioVersion % Test,
      "dev.zio"               %% "zio-test-sbt"                % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val docs = project
  .in(file("zio-s3-docs"))
  .settings(BuildHelper.stdSettings("zio-s3-docs"))
  .settings(
    moduleName := "zio-s3-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    projectName := "ZIO S3",
    mainModuleName := (`zio-s3` / moduleName).value,
    projectStage := ProjectStage.ProductionReady,
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(`zio-s3`),
    //conflict with the dependency zio-nio & sbt-mdoc
    excludeDependencies += "org.scala-lang.modules" % "scala-collection-compat_3"
  )
  .dependsOn(`zio-s3`)
  .enablePlugins(WebsitePlugin)
