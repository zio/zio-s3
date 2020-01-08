import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.github.io/zio-s3/")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    Test / fork := true,
    parallelExecution in Test := false,
    publishMavenStyle := true,
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/zio/zio-s3/"),
        "scm:git:git@github.com:zio/zio-s3.git"
      )
    )
  )
)

val zioVersion          = "1.0.0-RC17"
val awsJavaSdkS3Version = "1.11.679"

lazy val root =
  project
    .in(file("."))
    .settings(skip in publish := true)
    .aggregate(core)

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

lazy val core = project
  .in(file("modules/core"))
  .settings(skip in publish := true)
  .settings(
    libraryDependencies := Seq(
      "com.amazonaws" % "aws-java-sdk-s3" % awsJavaSdkS3Version,
      "dev.zio"       %% "zio"            % zioVersion,
      "dev.zio"       %% "zio-test"       % zioVersion % Test,
      "dev.zio"       %% "zio-test-sbt"   % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
