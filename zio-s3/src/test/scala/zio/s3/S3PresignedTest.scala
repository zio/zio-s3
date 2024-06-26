package zio.s3

import com.dimafeng.testcontainers.MinIOContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.S3Exception
import sttp.client3
import sttp.client3.httpclient.zio.HttpClientZioBackend
import sttp.client3.{ SttpBackend, basicRequest }
import sttp.model.Uri
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.{ Chunk, Scope, Task, ZIO, ZLayer, durationInt, s3 }

import java.net.{ URI, URL }

object S3PresignedTest extends ZIOSpecDefault {

  val bucketName = "initial-bucket"
  val objectKey  = "path/to/object"

  val spec = (suite("S3Presigned")(
    test("presignGetObject should return an url") {
      for {
        s3  <- ZIO.service[S3]
        url <- s3.presignGetObject(bucketName, objectKey, 1.minute)
      } yield assert(url.url.toExternalForm)(
        containsString(bucketName) &&
          containsString(objectKey) &&
          containsString("X-Amz-Expires=60") &&
          containsString("X-Amz-Signature")
      )
    },
    test("presignGetObject url should be downloadable") {

      for {
        s3          <- ZIO.service[S3]
        fileContent <- putObject(bucketName, "my-new-key")
        url         <- s3.presignGetObject(bucketName, "my-new-key", 1.minute)
        actual      <- getUrlContent(url.url)
        expected    <- fileContent.runCollect
      } yield assert(Chunk.fromArray(actual))(equalTo(expected))
    }
  ) @@ TestAspect.before(s3.createBucket(bucketName) *> putObject(bucketName, objectKey)))
    .provideSome[Scope](minio, zioS3Layer, HttpClientZioBackend.layer())

  def getUrlContent(url: URL): ZIO[SttpBackend[Task, Any], Throwable, Array[Byte]] =
    for {
      url    <- ZIO.fromEither(Uri.parse(url.toExternalForm)).orDieWith(e => new Throwable(e))
      request = basicRequest.response(client3.asByteArrayAlways).get(url)
      result <- ZIO.serviceWithZIO[SttpBackend[Task, Any]](_.send(request)).map(_.body)
    } yield result

  def putObject(bucketName: String, key: String): ZIO[S3, Throwable, ZStream[Any, Throwable, Byte]] = {
    val filePath    = ClassLoader.getSystemResource("console.log").getFile
    val fileContent = ZStream.fromFileName(filePath)
    for {
      fileLength <- fileContent.runCount
      _          <- s3.putObject(bucketName = bucketName, key = key, contentLength = fileLength, content = fileContent)
    } yield fileContent
  }

  def zioS3Layer: ZLayer[MinIOContainer, S3Exception, S3] =
    ZLayer {
      for {
        minio <- ZIO.service[MinIOContainer]
      } yield s3.live(
        region = Region.US_WEST_1,
        credentials = AwsBasicCredentials.create(minio.userName, minio.password),
        uriEndpoint = Some(URI.create(minio.s3URL)),
        forcePathStyle = Some(true)
      )
    }.flatten

  def minio: ZLayer[Scope, Nothing, MinIOContainer] =
    ZLayer {
      ZIO
        .attemptBlocking(
          new MinIOContainer(dockerImageName = DockerImageName.parse("minio/minio:RELEASE.2024-06-22T05-26-45Z"))
        )
        .tap(minio => ZIO.attemptBlockingIO(minio.start()))
        .withFinalizerAuto
    }.orDie

}
