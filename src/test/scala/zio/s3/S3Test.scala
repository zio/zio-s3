package zio.s3

import java.net.URI
import java.util.UUID

import software.amazon.awssdk.regions.Region
import zio.{ Chunk, Managed }
import zio.blocking.Blocking
import zio.nio.file.{ Files, Path }
import zio.stream.{ ZSink, ZStreamChunk }
import zio.test.Assertion._
import zio.test._

import scala.util.Random

object S3Test
    extends DefaultRunnableSpec(
      suite("S3")(
        testM("list buckets") {
          val test = for {
            listing <- listBuckets()
          } yield assert(listing.buckets.map(_.name), equalTo(List("bucket-1")))

          test.provideManaged(utils.s3)
        },
        testM("list objects") {
          val test =
            for {
              succeed <- listObjects_("bucket-1")
            } yield assert(
              succeed,
              equalTo(
                S3ObjectListing(
                  "bucket-1",
                  List(
                    S3ObjectSummary("bucket-1", "console.log"),
                    S3ObjectSummary("bucket-1", "dir1/hello.txt"),
                    S3ObjectSummary("bucket-1", "dir1/user.csv")
                  ),
                  None
                )
              )
            )

          test.provideManaged(utils.s3)
        },
        testM("list objects with prefix") {
          val test =
            for {
              succeed <- listObjects("bucket-1", "console", 10)
            } yield assert(
              succeed,
              equalTo(
                S3ObjectListing("bucket-1", List(S3ObjectSummary("bucket-1", "console.log")), None)
              )
            )
          test.provideManaged(utils.s3)
        },
        testM("list objects with not match prefix") {
          val test =
            for {
              succeed <- listObjects("bucket-1", "blah", 10)
            } yield assert(
              succeed,
              equalTo(
                S3ObjectListing("bucket-1", Nil, None)
              )
            )
          test.provideManaged(utils.s3)
        },
        testM("create bucket") {
          val bucketTmp = UUID.randomUUID().toString
          val test =
            for {
              succeed <- createBucket(bucketTmp)
              _       <- Files.delete(Path(s"minio/data/$bucketTmp")).provide(Blocking.Live)
            } yield assert(succeed, isUnit)

          test.provideManaged(utils.s3)
        },
        testM("create invalid bucket") {
          val test =
            for {
              succeed <- createBucket("bucket_$1")
                          .foldCause(_ => false, _ => true)
            } yield assert(succeed, isFalse)
          test.provideManaged(utils.s3)
        },
        testM("create bucket already exist") {
          val test =
            for {
              succeed <- createBucket("bucket-1")
                          .foldCause(_ => false, _ => true)
            } yield assert(succeed, isFalse)
          test.provideManaged(utils.s3)
        },
        testM("delete bucket") {
          val bucketTmp = UUID.randomUUID().toString

          val test =
            for {
              _       <- createBucket(bucketTmp)
              succeed <- deleteBucket(bucketTmp)
            } yield assert(succeed, isUnit)
          test.provideManaged(utils.s3)
        },
        testM("delete bucket dont exist") {
          val test =
            for {
              succeed <- deleteBucket(UUID.randomUUID().toString).foldCause(_ => false, _ => true)
            } yield assert(succeed, isFalse)
          test.provideManaged(utils.s3)
        },
        testM("exists bucket") {
          val test =
            for {
              succeed <- isBucketExists("bucket-1")
            } yield assert(succeed, isTrue)
          test.provideManaged(utils.s3)
        },
        testM("exists bucket - invalid identifier") {
          val test =
            for {
              succeed <- isBucketExists(UUID.randomUUID().toString)
            } yield assert(succeed, isFalse)
          test.provideManaged(utils.s3)
        },
        testM("delete object") {
          val objectTmp = UUID.randomUUID().toString

          val test =
            for {
              _       <- Files.createFile(Path(s"minio/data/bucket-1/$objectTmp")).provide(Blocking.Live)
              succeed <- deleteObject("bucket-1", objectTmp)
            } yield assert(succeed, isUnit)
          test.provideManaged(utils.s3)
        },
        testM("delete object - invalid identifier") {
          val test =
            for {
              succeed <- deleteObject("bucket-1", UUID.randomUUID().toString)
            } yield assert(succeed, isUnit)
          test.provideManaged(utils.s3)
        },
        testM("get object") {
          val test =
            for {
              content <- getObject("bucket-1", "dir1/hello.txt")
                          .run(ZSink.utf8DecodeChunk)
            } yield assert(content, equalTo("""|Hello ZIO s3
                                               |this is a beautiful day""".stripMargin))
          test.provideManaged(utils.s3)
        },
        testM("get object - invalid identifier") {
          val test =
            for {
              succeed <- getObject("bucket-1", UUID.randomUUID().toString)
                          .run(ZSink.utf8DecodeChunk)
                          .fold(_ => false, _ => true)
            } yield assert(succeed, isFalse)
          test.provideManaged(utils.s3)
        },
        testM("get nextObjects") {
          val test =
            for {
              token   <- listObjects("bucket-1", "", 1).map(_.nextContinuationToken)
              listing <- getNextObjects(S3ObjectListing("bucket-1", Nil, token))
            } yield assert(listing.objectSummaries, isNonEmpty)
          test.provideManaged(utils.s3)
        },
        testM("get nextObjects - invalid token") {
          val test =
            for {
              listing <- getNextObjects(S3ObjectListing("bucket-1", Nil, Some("blah")))
            } yield assert(listing, equalTo(S3ObjectListing("bucket-1", Nil, None)))
          test.provideManaged(utils.s3)
        },
        testM("put object") {
          val c      = Chunk.fromArray("Hello F World".getBytes)
          val data   = ZStreamChunk.fromChunks(c)
          val tmpKey = Random.alphanumeric.take(10).mkString

          val test = for {
            _         <- putObject_("bucket-1", tmpKey, c.length, data)
            fileExist <- Files.deleteIfExists(Path(s"minio/data/bucket-1/$tmpKey")).provide(Blocking.Live)
          } yield assert(fileExist, isTrue)

          test.provideManaged(utils.s3)
        }
//        testM("put/get object") {
//          val data = ZStreamChunk.fromChunks(Chunk.fromArray("Hello F World".getBytes))
//
//          val test = for {
//            succeed <- putObject("bucket-1", "console.log", data).fold(_ => false, _ => true)
//          } yield assert(succeed, isFalse)
//
//          test.provideManaged(utils.s3)
//        }
      )
    )

object utils {

  val s3: Managed[S3Failure, S3] = for {
    settings <- S3Settings
                 .from(
                   Region.CA_CENTRAL_1,
                   S3Credentials("TESTKEY", "TESTSECRET"),
                   Some(URI.create("http://localhost:9000"))
                 )
                 .toManaged_
    client <- S3.Live.connect(settings)
  } yield client
}
