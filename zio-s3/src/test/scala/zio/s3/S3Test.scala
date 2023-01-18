package zio.s3

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials

import java.net.URI
import java.util.UUID
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{ ObjectCannedACL, S3Exception }
import zio.nio.file.{ Path => ZPath }
import zio.stream.{ ZPipeline, ZStream }
import zio.test.Assertion._
import zio.test.TestAspect.sequential
import zio.test._
import zio.{ Chunk, Scope, ZLayer }

import scala.util.Random

object S3LiveSpec extends ZIOSpecDefault {

  private val s3 =
    zio.s3
      .live(
        Region.CA_CENTRAL_1,
        AwsBasicCredentials.create("TESTKEY", "TESTSECRET"),
        Some(URI.create("http://127.0.0.1:9000"))
      )
      .mapError(TestFailure.die)

  override def spec: Spec[TestEnvironment with Scope, Any] =
    S3Suite.spec("S3LiveSpec").provideLayerShared(s3)
}

object S3TestSpec extends ZIOSpecDefault {
  private val root = ZPath("../test-data")

  private val s3: ZLayer[Any, Nothing, S3] = zio.s3.stub(root)

  override def spec: Spec[TestEnvironment with Scope, Any] =
    S3Suite.spec("S3TestSpec").provideLayerShared(s3)
}

object InvalidS3LayerTestSpec extends ZIOSpecDefault {

  private val s3: ZLayer[Scope, S3Exception, S3] =
    zio.s3.liveZIO(Region.EU_CENTRAL_1, providers.default)

  override def spec =
    suite("InvalidS3LayerTest") {
      test("listBuckets") {
        listBuckets.provideLayer(s3).either.map(assert(_)(isLeft(isSubtype[S3Exception](anything))))
      }
    }

}

object S3Suite {
  val bucketName = "bucket-1"

  private[this] def randomNEStream(): (Int, ZStream[Any, Nothing, Byte]) = {
    val size  = PartSize.Min + Random.nextInt(100)
    val bytes = new Array[Byte](size)
    Random.nextBytes(bytes)
    (size, ZStream.fromChunks(Chunk.fromArray(bytes)))
  }

  def spec(label: String): Spec[S3, Exception] =
    suite(label)(
      test("listAllObjects") {
        for {
          list <- listAllObjects(bucketName).runCollect
        } yield assert(list.map(_.key))(hasSameElements(List("console.log", "dir1/hello.txt", "dir1/user.csv")))
      },
      test("list buckets") {
        for {
          buckets <- listBuckets
        } yield assertTrue(buckets.map(_.name) == Chunk.single(bucketName))
      },
      test("list objects") {
        for {
          succeed <- listObjects(bucketName)
        } yield assertTrue(succeed.bucketName == bucketName) &&
          assert(succeed.objectSummaries.map(s => s.bucketName -> s.key))(
            hasSameElements(
              List(
                (bucketName, "console.log"),
                (bucketName, "dir1/hello.txt"),
                (bucketName, "dir1/user.csv")
              )
            )
          )
      },
      test("list objects with prefix") {
        for {
          succeed <- listObjects(bucketName, ListObjectOptions.from("console", 10))
        } yield assert(succeed)(
          hasField("bucketName", (l: S3ObjectListing) => l.bucketName, equalTo(bucketName)) &&
            hasField(
              "objectSummaries",
              (l: S3ObjectListing) => l.objectSummaries.map(o => o.bucketName -> o.key),
              equalTo(Chunk.single((bucketName, "console.log")))
            )
        )
      },
      test("list objects with not match prefix") {
        for {
          succeed <- listObjects(bucketName, ListObjectOptions.from("blah", 10))
        } yield assertTrue(succeed.bucketName -> succeed.objectSummaries == bucketName -> Chunk.empty)
      },
      test("list objects with delimiter") {
        for {
          succeed <- listObjects(bucketName, ListObjectOptions(Some("dir1/"), 10, Some("/"), None))
        } yield assertTrue(
          succeed.bucketName          -> succeed.objectSummaries
            .map(_.key) == bucketName -> Chunk("dir1/hello.txt", "dir1/user.csv")
        )
      },
      test("list objects with startAfter dir1/hello.txt") {
        for {
          succeed <- listObjects(bucketName, ListObjectOptions.fromStartAfter("dir1/hello.txt"))
        } yield assertTrue(
          succeed.bucketName -> succeed.objectSummaries.map(_.key).sorted == bucketName -> Chunk("dir1/user.csv")
        )
      },
      test("create bucket") {
        val bucketTmp = UUID.randomUUID().toString
        for {
          succeed <- createBucket(bucketTmp)
          _       <- deleteBucket(bucketTmp)
        } yield assert(succeed)(isUnit)
      },
      test("create empty bucket name fail") {

        for {
          succeed <- createBucket("")
                       .foldCause(_ => false, _ => true)
        } yield assertTrue(!succeed)
      },
      test("create bucket already exist") {
        for {
          succeed <- createBucket(bucketName)
                       .foldCause(_ => false, _ => true)
        } yield assertTrue(!succeed)
      },
      test("delete bucket") {
        val bucketTmp = UUID.randomUUID().toString

        for {
          _       <- createBucket(bucketTmp)
          succeed <- deleteBucket(bucketTmp)
        } yield assert(succeed)(isUnit)
      },
      test("delete bucket dont exist") {
        for {
          succeed <- deleteBucket(UUID.randomUUID().toString).foldCause(_ => false, _ => true)
        } yield assertTrue(!succeed)
      },
      test("exists bucket") {
        for {
          succeed <- isBucketExists(bucketName)
        } yield assertTrue(succeed)

      },
      test("exists bucket - invalid identifier") {
        for {
          succeed <- isBucketExists(UUID.randomUUID().toString)
        } yield assertTrue(!succeed)
      },
      test("delete object - invalid identifier") {
        for {
          succeed <- deleteObject(bucketName, UUID.randomUUID().toString)
        } yield assert(succeed)(isUnit)
      },
      test("get object") {
        for {
          content      <- getObject(bucketName, "dir1/hello.txt")
                            .via(ZPipeline.utf8Decode)
                            .runCollect
          contentString = content.mkString
        } yield assertTrue(
          contentString ==
            """|Hello ZIO s3
               |this is a beautiful day""".stripMargin
        )
      },
      test("get object - invalid identifier") {
        for {
          succeed <- getObject(bucketName, UUID.randomUUID().toString)
                       .via(ZPipeline.utf8Decode)
                       .runCollect
                       .refineToOrDie[S3Exception]
                       .fold(ex => ex.statusCode() == 404, _ => false)
        } yield assertTrue(succeed)
      },
      test("get nextObjects") {
        for {
          token   <- listObjects(bucketName, ListObjectOptions.fromMaxKeys(1)).map(_.nextContinuationToken)
          listing <- getNextObjects(S3ObjectListing.from(bucketName, token))
        } yield assertTrue(listing.objectSummaries.nonEmpty)
      },
      test("get nextObjects - invalid token") {
        for {
          succeed <- getNextObjects(S3ObjectListing.from(bucketName, Some(""))).foldCause(_ => false, _ => true)
        } yield assertTrue(!succeed)

      },
      test("put object") {
        val c             = Chunk.fromArray(Random.nextString(65536).getBytes())
        val contentLength = c.length.toLong
        val data          = ZStream.fromChunks(c).rechunk(5)
        val tmpKey        = Random.alphanumeric.take(10).mkString

        for {
          _                   <- putObject(bucketName, tmpKey, contentLength, data)
          objectContentLength <- getObjectMetadata(bucketName, tmpKey).map(_.contentLength) <*
                                   deleteObject(bucketName, tmpKey)
        } yield assertTrue(objectContentLength == contentLength)

      },
      test("multipart object") {
        val text =
          """Lorem ipsum dolor sit amet, consectetur adipiscing elit.
            |Donec semper eros quis felis scelerisque, quis lobortis felis cursus.
            |Nulla vulputate arcu nec luctus lobortis.
            |Duis non est posuere, feugiat augue et, tincidunt magna.
            |Etiam tempor dolor at lorem volutpat, at efficitur purus sagittis.
            |Curabitur sed nibh nec libero viverra posuere.
            |Aenean ullamcorper tortor ac ligula rutrum, euismod pulvinar justo faucibus.
            |Mauris dictum ligula ut lacus pellentesque porta.
            |Etiam molestie dolor ac purus consectetur, eget pellentesque mauris bibendum.
            |Sed at purus volutpat, tempor elit id, maximus neque.
            |Quisque pellentesque velit sed lectus placerat cursus.
            |Vestibulum quis urna non nibh ornare elementum.
            |Aenean a massa feugiat, fringilla dui eget, ultrices velit.
            |Aliquam pellentesque felis eget mi tincidunt dapibus vel at turpis.""".stripMargin

        val data   = ZStream.fromChunks(Chunk.fromArray(text.getBytes))
        val tmpKey = Random.alphanumeric.take(10).mkString

        for {
          _             <- multipartUpload(bucketName, tmpKey, data)(1)
          contentLength <- getObjectMetadata(bucketName, tmpKey).map(_.contentLength) <*
                             deleteObject(bucketName, tmpKey)
        } yield assertTrue(contentLength > 0L)
      },
      test("multipart with parrallelism = 1") {
        val (dataLength, data) = randomNEStream()
        val tmpKey             = Random.alphanumeric.take(10).mkString

        for {
          _             <- multipartUpload(bucketName, tmpKey, data)(1)
          contentLength <- getObjectMetadata(bucketName, tmpKey).map(_.contentLength) <*
                             deleteObject(bucketName, tmpKey)
        } yield assertTrue(contentLength == dataLength.toLong)
      },
      test("multipart with invalid parallelism value 0") {
        val data   = ZStream.empty
        val tmpKey = Random.alphanumeric.take(10).mkString
        val io     = multipartUpload(bucketName, tmpKey, data)(0)
        io.exit.map(assert(_)(dies(hasMessage(equalTo("parallelism must be > 0. 0 is invalid")))))
      },
      test("multipart with invalid partSize value 0") {
        val tmpKey        = Random.alphanumeric.take(10).mkString
        val invalidOption = MultipartUploadOptions.fromPartSize(0)
        val io            = multipartUpload(bucketName, tmpKey, ZStream.empty, invalidOption)(1)
        io.exit.map(assert(_)(dies(hasMessage(equalTo(s"Invalid part size 0.0 Mb, minimum size is 5 Mb")))))
      },
      test("multipart object when the content is empty") {
        val data   = ZStream.empty
        val tmpKey = Random.alphanumeric.take(10).mkString

        for {
          _             <- multipartUpload(bucketName, tmpKey, data)(1)
          contentLength <- getObjectMetadata(bucketName, tmpKey).map(_.contentLength) <*
                             deleteObject(bucketName, tmpKey)
        } yield assertTrue(contentLength == 0L)
      },
      test("multipart object when the content type is not provided") {
        val (_, data) = randomNEStream()
        val tmpKey    = Random.alphanumeric.take(10).mkString

        for {
          _           <- multipartUpload(bucketName, tmpKey, data)(4)
          contentType <- getObjectMetadata(bucketName, tmpKey).map(_.contentType) <*
                           deleteObject(bucketName, tmpKey)
        } yield assertTrue(contentType == "binary/octet-stream")
      },
      test("multipart object when there is a content type and metadata") {
        val metadata  = Map("key1" -> "value1")
        val (_, data) = randomNEStream()
        val tmpKey    = Random.alphanumeric.take(10).mkString

        for {
          _              <- multipartUpload(
                              bucketName,
                              tmpKey,
                              data,
                              MultipartUploadOptions.fromUploadOptions(
                                UploadOptions(metadata, ObjectCannedACL.PRIVATE, Some("application/json"))
                              )
                            )(4)
          objectMetadata <- getObjectMetadata(bucketName, tmpKey) <* deleteObject(bucketName, tmpKey)
        } yield assertTrue(objectMetadata.contentType == "application/json") &&
          assertTrue(objectMetadata.metadata.map { case (k, v) => k.toLowerCase -> v } == Map("key1" -> "value1"))
      },
      test("multipart object when the chunk size and parallelism are customized") {
        val (dataSize, data) = randomNEStream()
        val tmpKey           = Random.alphanumeric.take(10).mkString

        for {
          _             <- multipartUpload(bucketName, tmpKey, data, MultipartUploadOptions.fromPartSize(10 * PartSize.Mega))(4)
          contentLength <- getObjectMetadata(bucketName, tmpKey).map(_.contentLength) <*
                             deleteObject(bucketName, tmpKey)
        } yield assertTrue(contentLength == dataSize.toLong)
      },
      test("stream lines") {

        for {
          list <- streamLines(bucketName, "dir1/user.csv").runCollect
        } yield assertTrue(list.headOption.get == "John,Doe,120 jefferson st.,Riverside, NJ, 08075") &&
          assertTrue(list.lastOption.get == "Marie,White,20 time square,Bronx, NY,08220")
      },
      test("stream lines - invalid key") {
        for {
          succeed <- streamLines(bucketName, "blah").runCollect.fold(_ => false, _ => true)
        } yield assertTrue(!succeed)
      },
      test("put object when the content type is not provided") {
        val (dataSize, data) = randomNEStream()
        val tmpKey           = Random.alphanumeric.take(10).mkString

        for {
          _             <- putObject(bucketName, tmpKey, dataSize.toLong, data)
          contentLength <- getObjectMetadata(bucketName, tmpKey).map(_.contentLength) <*
                             deleteObject(bucketName, tmpKey)
        } yield assertTrue(dataSize.toLong == contentLength)
      },
      test("put object when there is a content type and metadata") {
        val _metadata        = Map("key1" -> "value1")
        val (dataSize, data) = randomNEStream()
        val tmpKey           = Random.alphanumeric.take(10).mkString

        for {
          _              <- putObject(
                              bucketName,
                              tmpKey,
                              dataSize.toLong,
                              data,
                              UploadOptions.from(_metadata, "application/json")
                            )
          objectMetadata <- getObjectMetadata(bucketName, tmpKey) <* deleteObject(bucketName, tmpKey)
        } yield assertTrue(objectMetadata.contentType == "application/json") &&
          assertTrue(objectMetadata.metadata.map { case (k, v) => k.toLowerCase -> v } == Map("key1" -> "value1"))
      }
    ) @@ sequential

}
