package zio.s3

import java.net.URI
import java.nio.file.attribute.PosixFileAttributes
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
    live(Region.CA_CENTRAL_1.id(), S3Credentials("TESTKEY", "TESTSECRET"), Some(URI.create("http://localhost:9000")))
      .mapError(TestFailure.die(_))

  override def spec = {
    suite("S3LiveSpec") (
      S3Suite.spec("Common spec", root),
      S3Suite.liveSpec("Live spec", root)
    ).provideCustomLayerShared(s3)
  }
}

object S3TestSpec extends DefaultRunnableSpec {
  private val root = ZPath("test-data")

  private val s3: ZLayer[Blocking, TestFailure[Any], S3] = zio.s3.test(root).mapError(TestFailure.fail(_))
  override def spec                                      = S3Suite.spec("S3TestSpec", root).provideCustomLayerShared(Blocking.live >>> s3)
}

object S3Suite {
  val bucketName = "bucket-1"

  def spec(label: String, root: ZPath): Spec[S3, TestFailure[Exception], TestSuccess] =
    suite(label)(
      testM("listDescendant") {
        for {
          list <- listObjectsDescendant(bucketName, "").runCollect
        } yield assert(list.map(_.key))(hasSameElements(List("console.log", "dir1/hello.txt", "dir1/user.csv")))
      },
      testM("list buckets") {
        for {
          buckets <- listBuckets
        } yield assert(buckets.map(_.name))(equalTo(Chunk.single(bucketName)))
      },
      testM("list objects") {
        for {
          succeed <- listObjects_(bucketName)
        } yield assert(succeed.bucketName)(equalTo(bucketName)) && assert(succeed.objectSummaries)(
          hasSameElements(
            List(
              S3ObjectSummary(bucketName, "console.log"),
              S3ObjectSummary(bucketName, "dir1/hello.txt"),
              S3ObjectSummary(bucketName, "dir1/user.csv")
            )
          )
        )
      },
      testM("list objects with prefix") {
        for {
          succeed <- listObjects(bucketName, "console", 10)
        } yield assert(succeed)(
          equalTo(
            S3ObjectListing(bucketName, Chunk.single(S3ObjectSummary(bucketName, "console.log")), None)
          )
        )
      },
      testM("list objects with not match prefix") {
        for {
          succeed <- listObjects(bucketName, "blah", 10)
        } yield assert(succeed)(
          equalTo(
            S3ObjectListing(bucketName, Chunk.empty, None)
          )
        )
      },
      testM("create bucket") {
        val bucketTmp = UUID.randomUUID().toString
        for {
          succeed <- createBucket(bucketTmp)
          _       <- ZFiles.delete(root / bucketTmp).provideLayer(Blocking.live)
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
          _       <- ZFiles.createFile(root / bucketName / objectTmp).provideLayer(Blocking.live)
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
          token   <- listObjects(bucketName, "", 1).map(_.nextContinuationToken)
          listing <- getNextObjects(S3ObjectListing(bucketName, Chunk.empty, token))
        } yield assert(listing.objectSummaries)(isNonEmpty)
      },
      testM("get nextObjects - invalid token") {
        for {
          succeed <- getNextObjects(S3ObjectListing(bucketName, Chunk.empty, Some(""))).foldCause(_ => false, _ => true)
        } yield assert(succeed)(isFalse)

      },
      testM("put object") {
        val c      = Chunk.fromArray("Hello F World".getBytes)
        val data   = ZStream.fromChunks(c)
        val tmpKey = Random.alphanumeric.take(10).mkString

        for {
          _        <- putObject(bucketName, tmpKey, c.length.toLong, "text/plain", data, metadata = Map.empty)
          fileSize <- (ZFiles
                          .readAttributes[PosixFileAttributes](root / bucketName / tmpKey)
                          .map(_.size()) <* ZFiles.delete(root / bucketName / tmpKey)).provideLayer(Blocking.live)
        } yield assert(fileSize)(isGreaterThan(0L))

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
          _        <- multipartUpload(bucketName, tmpKey, "application/octet-stream", data)
          fileSize <- (ZFiles
                          .readAttributes[PosixFileAttributes](root / bucketName / tmpKey)
                          .map(_.size()) <* ZFiles.delete(root / bucketName / tmpKey)).provideLayer(Blocking.live)
        } yield assert(fileSize)(isGreaterThan(0L))
      },
      testM("stream lines") {

        for {
          list <- streamLines(S3ObjectSummary(bucketName, "dir1/user.csv")).runCollect
        } yield assert(list.headOption)(isSome(equalTo("John,Doe,120 jefferson st.,Riverside, NJ, 08075"))) &&
          assert(list.lastOption)(isSome(equalTo("Marie,White,20 time square,Bronx, NY,08220")))
      },
      testM("stream lines - invalid key") {
        for {
          succeed <- streamLines(S3ObjectSummary(bucketName, "blah")).runCollect.fold(_ => false, _ => true)
        } yield assert(succeed)(isFalse)
      }
    )

  def liveSpec(label: String, root: ZPath): Spec[S3, TestFailure[Exception], TestSuccess] =
    suite(label)(
      testM("multipart object when the chunk size and parallelism are customized") {
        val chunkSize = MinChunkSize + 123
        val dataSize  = MinChunkSize * 10
        val bytes     = new Array[Byte](dataSize)
        Random.nextBytes(bytes)
        val data      = ZStream.fromChunks(Chunk.fromArray(bytes))
        val tmpKey    = Random.alphanumeric.take(10).mkString

        for {
          _        <- multipartUpload(
            bucketName,
            tmpKey,
            "application/octet-stream",
            data,
            chunkSize = chunkSize,
            parallelism = 4
          )
          fileSize <- (ZFiles
            .readAttributes[PosixFileAttributes](root / bucketName / tmpKey)
            .map(_.size()) <* ZFiles.delete(root / bucketName / tmpKey)).provideLayer(Blocking.live)
        } yield assert(fileSize)(equalTo(dataSize.toLong))

      },
      testM("multipart object when the chunk size is smaller than minimum") {
        val chunkSize = MinChunkSize - 100
        val dataSize  = MinChunkSize * 10
        val bytes     = new Array[Byte](dataSize)
        Random.nextBytes(bytes)
        val data   = ZStream.fromChunks(Chunk.fromArray(bytes))
        val tmpKey = Random.alphanumeric.take(10).mkString

        for {
          uploadResult <- multipartUpload(bucketName, tmpKey, "application/octet-stream", data, chunkSize = chunkSize).either
        } yield assert(uploadResult)(isLeft(Assertion.anything))
      },
      testM("multipart object when the content is empty") {
        val data   = ZStream.empty
        val tmpKey = Random.alphanumeric.take(10).mkString

        for {
          _        <- multipartUpload(bucketName, tmpKey, "application/octet-stream", data)
          uploaded <- ZFiles.exists(root / bucketName / tmpKey).provideLayer(Blocking.live)
        } yield assert(uploaded)(isFalse)
      },
      testM("multipart object when there is a canned acl") {
        val dataSize = 100
        val bytes    = new Array[Byte](dataSize)
        Random.nextBytes(bytes)
        val data     = ZStream.fromChunks(Chunk.fromArray(bytes))
        val tmpKey   = Random.alphanumeric.take(10).mkString

        for {
          _        <- multipartUpload(
            bucketName,
            tmpKey,
            "application/octet-stream",
            data,
            cannedAcl = ObjectCannedACL.AUTHENTICATED_READ
          )
          fileSize <- (ZFiles
            .readAttributes[PosixFileAttributes](root / bucketName / tmpKey)
            .map(_.size()) <* ZFiles.delete(root / bucketName / tmpKey)).provideLayer(Blocking.live)
        } yield assert(fileSize)(equalTo(dataSize.toLong))
      }
    )

}
