package zio.s3

import java.net.URI
import java.util.UUID
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import zio.blocking.Blocking
import zio.nio.core.file.{Path => ZPath}
import zio.nio.file.{Files => ZFiles}
import zio.stream.{ZStream, ZTransducer}
import zio.test.Assertion._
import zio.test._
import zio.{Chunk, ZLayer}

import scala.util.Random

object S3LiveSpec extends DefaultRunnableSpec {
  private val root = ZPath("minio/data")

  private val s3 =
    live(Region.CA_CENTRAL_1, S3Credentials("TESTKEY", "TESTSECRET"), Some(URI.create("http://localhost:9000")))
      .mapError(TestFailure.die(_))

  override def spec =
    S3Suite.spec("S3LiveSpec", root).provideCustomLayerShared(s3)
}

object S3TestSpec extends DefaultRunnableSpec {
  private val root = ZPath("test-data")

  private val s3: ZLayer[Blocking, TestFailure[Any], S3] =
    zio.s3.stub(root).mapError(TestFailure.fail)

  override def spec =
    S3Suite.spec("S3TestSpec", root).provideCustomLayerShared(Blocking.live >>> s3)
}

object S3Suite {
  val bucketName = "bucket-1"

  def spec(label: String, root: ZPath): Spec[S3 with Blocking, TestFailure[Exception], TestSuccess] =
    suite(label)(
      testM("listAllObjects") {
        for {
          list <- listAllObjects(bucketName).runCollect
        } yield assert(list.map(_.key))(hasSameElements(List("console.log", "dir1/hello.txt", "dir1/user.csv")))
      },
      testM("list buckets") {
        for {
          buckets <- listBuckets
        } yield assert(buckets.map(_.name))(equalTo(Chunk.single(bucketName)))
      },
      testM("list objects") {
        for {
          succeed <- listObjects(bucketName)
        } yield assert(succeed.bucketName)(equalTo(bucketName)) && assert(
          succeed.objectSummaries.map(s => s.bucketName -> s.key)
        )(
          hasSameElements(
            List(
              (bucketName, "console.log"),
              (bucketName, "dir1/hello.txt"),
              (bucketName, "dir1/user.csv")
            )
          )
        )
      },
      testM("list objects with prefix") {
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
      testM("list objects with not match prefix") {
        for {
          succeed <- listObjects(bucketName, ListObjectOptions.from("blah", 10))
        } yield assert(succeed.bucketName -> succeed.objectSummaries)(
          equalTo(bucketName -> Chunk.empty)
        )
      },
      testM("list objects with delimiter") {
        for {
          succeed <- listObjects(bucketName, ListObjectOptions(Some("dir1/"), 10,  Some("/"), None))
        } yield assert(succeed.bucketName -> succeed.objectSummaries.map(_.key))(
          equalTo(bucketName -> Chunk("dir1/hello.txt", "dir1/user.csv"))
        )
      },
      testM("list objects with startAfter dir1/hello.txt") {
        for {
          succeed <- listObjects(bucketName, ListObjectOptions.fromStartAfter("dir1/hello.txt"))
        } yield assert(succeed.bucketName -> succeed.objectSummaries.map(_.key).sorted)(
          equalTo(bucketName -> Chunk("dir1/user.csv"))
        )
      },
      testM("create bucket") {
        val bucketTmp = UUID.randomUUID().toString
        for {
          succeed <- createBucket(bucketTmp)
          _       <- ZFiles.delete(root / bucketTmp)
        } yield assert(succeed)(isUnit)
      },
      testM("create empty bucket name fail") {

        for {
          succeed <- createBucket("")
                       .foldCause(_ => false, _ => true)
        } yield assert(succeed)(isFalse)
      },
      testM("create bucket already exist") {
        for {
          succeed <- createBucket(bucketName)
                       .foldCause(_ => false, _ => true)
        } yield assert(succeed)(isFalse)
      },
      testM("delete bucket") {
        val bucketTmp = UUID.randomUUID().toString

        for {
          _       <- createBucket(bucketTmp)
          succeed <- deleteBucket(bucketTmp)
        } yield assert(succeed)(isUnit)
      },
      testM("delete bucket dont exist") {
        for {
          succeed <- deleteBucket(UUID.randomUUID().toString).foldCause(_ => false, _ => true)
        } yield assert(succeed)(isFalse)
      },
      testM("exists bucket") {
        for {
          succeed <- isBucketExists(bucketName)
        } yield assert(succeed)(isTrue)

      },
      testM("exists bucket - invalid identifier") {
        for {
          succeed <- isBucketExists(UUID.randomUUID().toString)
        } yield assert(succeed)(isFalse)
      },
      testM("delete object") {
        val objectTmp = UUID.randomUUID().toString

        for {
          _       <- ZFiles.createFile(root / bucketName / objectTmp)
          succeed <- deleteObject(bucketName, objectTmp)
        } yield assert(succeed)(isUnit)
      },
      testM("delete object - invalid identifier") {
        for {
          succeed <- deleteObject(bucketName, UUID.randomUUID().toString)
        } yield assert(succeed)(isUnit)
      },
      testM("get object") {
        for {
          content <- getObject(bucketName, "dir1/hello.txt")
                       .transduce(ZTransducer.utf8Decode)
                       .runCollect
        } yield assert(content.mkString)(equalTo("""|Hello ZIO s3
                                                    |this is a beautiful day""".stripMargin))
      },
      testM("get object - invalid identifier") {
        for {
          succeed <- getObject(bucketName, UUID.randomUUID().toString)
                       .transduce(ZTransducer.utf8Decode)
                       .runCollect
                       .fold(_ => false, _ => true)
        } yield assert(succeed)(isFalse)
      },
      testM("get nextObjects") {
        for {
          token   <- listObjects(bucketName, ListObjectOptions.fromMaxKeys(1)).map(_.nextContinuationToken)
          listing <- getNextObjects(S3ObjectListing.from(bucketName, token))
        } yield assert(listing.objectSummaries)(isNonEmpty)
      },
      testM("get nextObjects - invalid token") {
        for {
          succeed <- getNextObjects(S3ObjectListing.from(bucketName, Some(""))).foldCause(_ => false, _ => true)
        } yield assert(succeed)(isFalse)

      },
      testM("put object") {
        val c             = Chunk.fromArray("Hello F World".getBytes)
        val contentLength = c.length.toLong
        val data          = ZStream.fromChunks(c)
        val tmpKey        = Random.alphanumeric.take(10).mkString

        for {
          _                   <- putObject(bucketName, tmpKey, contentLength, data)
          objectContentLength <- getObjectMetadata(bucketName, tmpKey).map(_.contentLength) <*
                                   ZFiles.delete(root / bucketName / tmpKey)
        } yield assert(objectContentLength)(equalTo(contentLength))

      },
      testM("multipart object") {
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
                             ZFiles.delete(root / bucketName / tmpKey)
        } yield assert(contentLength)(isGreaterThan(0L))
      },
      testM("multipart with parrallelism = 1") {
        val (dataLength, data) = randomNEStream
        val tmpKey             = Random.alphanumeric.take(10).mkString

        for {
          _             <- multipartUpload(bucketName, tmpKey, data)(1)
          contentLength <- getObjectMetadata(bucketName, tmpKey).map(_.contentLength) <*
                             ZFiles.delete(root / bucketName / tmpKey)
        } yield assert(contentLength)(equalTo(dataLength.toLong))
      },
      testM("multipart with invalid parallelism value 0") {
        val data   = ZStream.empty
        val tmpKey = Random.alphanumeric.take(10).mkString
        val io     = multipartUpload(bucketName, tmpKey, data)(0)
        assertM(io.run)(dies(hasMessage(equalTo("parallelism must be > 0. 0 is invalid"))))
      },
      testM("multipart with invalid partSize value 0") {
        val tmpKey        = Random.alphanumeric.take(10).mkString
        val invalidOption = MultipartUploadOptions.fromPartSize(0)
        val io            = multipartUpload(bucketName, tmpKey, ZStream.empty, invalidOption)(1)
        assertM(io.run)(dies(hasMessage(equalTo(s"Invalid part size 0.0 Mb, minimum size is 5 Mb"))))
      },
      testM("multipart object when the content is empty") {
        val data   = ZStream.empty
        val tmpKey = Random.alphanumeric.take(10).mkString

        for {
          _             <- multipartUpload(bucketName, tmpKey, data)(1)
          contentLength <- getObjectMetadata(bucketName, tmpKey).map(_.contentLength) <*
                             ZFiles.delete(root / bucketName / tmpKey)
        } yield assert(contentLength)(equalTo(0L))
      },
      testM("multipart object when the content type is not provided") {
        val (_, data) = randomNEStream
        val tmpKey    = Random.alphanumeric.take(10).mkString

        for {
          _           <- multipartUpload(bucketName, tmpKey, data)(4)
          contentType <- getObjectMetadata(bucketName, tmpKey).map(_.contentType) <*
                           ZFiles.delete(root / bucketName / tmpKey)
        } yield assert(contentType)(equalTo("binary/octet-stream"))
      },
      testM("multipart object when there is a content type and metadata") {
        val metadata  = Map("key1" -> "value1")
        val (_, data) = randomNEStream
        val tmpKey    = Random.alphanumeric.take(10).mkString

        for {
          _              <- multipartUpload(
                              bucketName,
                              tmpKey,
                              data,
                              MultipartUploadOptions.fromUploadOptions(UploadOptions(metadata, ObjectCannedACL.PRIVATE, Some("application/json")))
                            )(4)
          objectMetadata <- getObjectMetadata(bucketName, tmpKey) <* ZFiles.delete(root / bucketName / tmpKey)
        } yield assert(objectMetadata.contentType)(equalTo("application/json")) &&
          assert(objectMetadata.metadata.map { case (k, v) => k.toLowerCase -> v })(equalTo(Map("key1" -> "value1")))
      },
      testM("multipart object when the chunk size and parallelism are customized") {
        val (dataSize, data) = randomNEStream
        val tmpKey           = Random.alphanumeric.take(10).mkString

        for {
          _             <- multipartUpload(bucketName, tmpKey, data, MultipartUploadOptions.fromPartSize(10 * PartSize.Mega))(4)
          contentLength <- getObjectMetadata(bucketName, tmpKey).map(_.contentLength) <*
                             ZFiles.delete(root / bucketName / tmpKey)
        } yield assert(contentLength)(equalTo(dataSize.toLong))
      },
      testM("stream lines") {

        for {
          list <- streamLines(bucketName, "dir1/user.csv").runCollect
        } yield assert(list.headOption)(isSome(equalTo("John,Doe,120 jefferson st.,Riverside, NJ, 08075"))) &&
          assert(list.lastOption)(isSome(equalTo("Marie,White,20 time square,Bronx, NY,08220")))
      },
      testM("stream lines - invalid key") {
        for {
          succeed <- streamLines(bucketName, "blah").runCollect.fold(_ => false, _ => true)
        } yield assert(succeed)(isFalse)
      },
      testM("put object when the content type is not provided") {
        val (dataSize, data) = randomNEStream
        val tmpKey           = Random.alphanumeric.take(10).mkString

        for {
          _           <- putObject(bucketName, tmpKey, dataSize.toLong, data)
          contentType <- getObjectMetadata(bucketName, tmpKey).map(_.contentType) <*
                           ZFiles.delete(root / bucketName / tmpKey)
        } yield assert(contentType)(equalTo("application/octet-stream"))
      },
      testM("put object when there is a content type and metadata") {
        val _metadata        = Map("key1" -> "value1")
        val (dataSize, data) = randomNEStream
        val tmpKey           = Random.alphanumeric.take(10).mkString

        for {
          _              <- putObject(
                              bucketName,
                              tmpKey,
                              dataSize.toLong,
                              data,
                              UploadOptions.from(_metadata, "application/json")
                            )
          objectMetadata <- getObjectMetadata(bucketName, tmpKey) <* ZFiles.delete(root / bucketName / tmpKey)
        } yield assert(objectMetadata.contentType)(equalTo("application/json")) &&
          assert(objectMetadata.metadata.map { case (k, v) => k.toLowerCase -> v })(equalTo(Map("key1" -> "value1")))
      }
    )

  //TODO remove and use generator
  private[this] def randomNEStream = {
    val size  = PartSize.Min + Random.nextInt(100)
    val bytes = new Array[Byte](size)
    Random.nextBytes(bytes)
    (size, ZStream.fromChunks(Chunk.fromArray(bytes)))
  }
}
