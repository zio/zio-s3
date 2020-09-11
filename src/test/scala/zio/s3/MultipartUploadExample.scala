package zio.s3

import java.io.FileInputStream
import java.nio.file.Paths

import software.amazon.awssdk.services.s3.S3AsyncClient
import zio.blocking.Blocking
import zio.stream.ZStream
import zio.{ ExitCode, TaskLayer, URIO, ZEnv, ZIO, ZLayer, ZManaged }

// Preparing the data
// wget http://speedtest.tele2.net/1GB.zip
object MultipartUploadExample extends zio.App {

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val s3: TaskLayer[S3] = ZLayer.fromManaged(
      ZManaged
        .fromAutoCloseable(ZIO(S3AsyncClient.builder().build()))
        .map(new Live(_))
    )

    def test(parallelism: Int) =
      (ZIO.effectTotal(System.currentTimeMillis()) <* multipartUpload(
        "my-test",
        s"1GB-${System.currentTimeMillis()}.zip",
        ZStream.fromInputStream(new FileInputStream(Paths.get("1GB.zip").toFile)),
        parallelism = parallelism
      ))
        .tap(begin => ZIO(println(s"Parallelism $parallelism ${System.currentTimeMillis() - begin}")))
        .provideSomeLayer[Blocking](s3)

    val tests =
      for {
        _ <- ZIO(println("Test"))
        _ <- test(4)
        _ <- test(2)
        _ <- test(1)
      } yield ()

    tests.repeatN(3).exitCode
  }

}
