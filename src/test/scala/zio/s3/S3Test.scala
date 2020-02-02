package zio.s3

import java.net.URI
import java.nio.file.attribute.PosixFileAttributes
import java.util.UUID

import software.amazon.awssdk.regions.Region
import zio.Chunk
import zio.blocking.Blocking
import zio.nio.file.{ Files, Path }
import zio.stream.{ ZSink, ZStream, ZStreamChunk }
import zio.test.Assertion._
import zio.test._

import scala.util.Random
import utils._

object S3Test
    extends DefaultRunnableSpec(
      suite("S3")(
        testM("list buckets") {
          val test = for {
            listing <- listBuckets()
          } yield assert(listing.buckets.map(_.name), equalTo(List(bucketName)))

          test.provideManaged(utils.s3)
        },
        testM("list objects") {
          val test =
            for {
              succeed <- listObjects_(bucketName)
            } yield assert(
              succeed,
              equalTo(
                S3ObjectListing(
                  bucketName,
                  List(
                    S3ObjectSummary(bucketName, "console.log"),
                    S3ObjectSummary(bucketName, "dir1/hello.txt"),
                    S3ObjectSummary(bucketName, "dir1/user.csv")
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
              succeed <- listObjects(bucketName, "console", 10)
            } yield assert(
              succeed,
              equalTo(
                S3ObjectListing(bucketName, List(S3ObjectSummary(bucketName, "console.log")), None)
              )
            )
          test.provideManaged(utils.s3)
        },
        testM("list objects with not match prefix") {
          val test =
            for {
              succeed <- listObjects(bucketName, "blah", 10)
            } yield assert(
              succeed,
              equalTo(
                S3ObjectListing(bucketName, Nil, None)
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
              succeed <- createBucket(bucketName)
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
              succeed <- isBucketExists(bucketName)
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
              _       <- Files.createFile(Path(s"minio/data/$bucketName/$objectTmp")).provide(Blocking.Live)
              succeed <- deleteObject(bucketName, objectTmp)
            } yield assert(succeed, isUnit)
          test.provideManaged(utils.s3)
        },
        testM("delete object - invalid identifier") {
          val test =
            for {
              succeed <- deleteObject(bucketName, UUID.randomUUID().toString)
            } yield assert(succeed, isUnit)
          test.provideManaged(utils.s3)
        },
        testM("get object") {
          val test =
            for {
              content <- getObject(bucketName, "dir1/hello.txt")
                          .run(ZSink.utf8DecodeChunk)
            } yield assert(content, equalTo("""|Hello ZIO s3
                                               |this is a beautiful day""".stripMargin))
          test.provideManaged(utils.s3)
        },
        testM("get object - invalid identifier") {
          val test =
            for {
              succeed <- getObject(bucketName, UUID.randomUUID().toString)
                          .run(ZSink.utf8DecodeChunk)
                          .fold(_ => false, _ => true)
            } yield assert(succeed, isFalse)
          test.provideManaged(utils.s3)
        },
        testM("get nextObjects") {
          val test =
            for {
              token   <- listObjects(bucketName, "", 1).map(_.nextContinuationToken)
              listing <- getNextObjects(S3ObjectListing(bucketName, Nil, token))
            } yield assert(listing.objectSummaries, isNonEmpty)
          test.provideManaged(utils.s3)
        },
        testM("get nextObjects - invalid token") {
          val test =
            for {
              listing <- getNextObjects(S3ObjectListing(bucketName, Nil, Some("blah")))
            } yield assert(listing, equalTo(S3ObjectListing(bucketName, Nil, None)))
          test.provideManaged(utils.s3)
        },
        testM("put object") {
          val c      = Chunk.fromArray("Hello F World".getBytes)
          val data   = ZStreamChunk.fromChunks(c)
          val tmpKey = Random.alphanumeric.take(10).mkString

          val test = for {
            _ <- putObject_(bucketName, tmpKey, c.length.toLong, data)
            fileSize <- Files
                         .readAttributes[PosixFileAttributes](Path(s"minio/data/$bucketName/$tmpKey"))
                         .map(_.size())
                         .provide(Blocking.Live)
            fileExist <- Files.deleteIfExists(Path(s"minio/data/$bucketName/$tmpKey")).provide(Blocking.Live)
          } yield assert(fileExist, isTrue) && assert(fileSize, isGreaterThan(0L))

          test.provideManaged(utils.s3)
        },
        testM("multipart object") {
          val text = """Lorem ipsum dolor sit amet, consectetur adipiscing elit.
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

          val data   = ZStream.fromChunk(Chunk.fromArray(text.getBytes))
          val tmpKey = Random.alphanumeric.take(10).mkString

          val test = for {
            _ <- multipartUpload(10)(bucketName, tmpKey, "application/octet-stream", data)
            fileSize <- Files
                         .readAttributes[PosixFileAttributes](Path(s"minio/data/$bucketName/$tmpKey"))
                         .map(_.size())
                         .provide(Blocking.Live)
            fileExist <- Files.deleteIfExists(Path(s"minio/data/$bucketName/$tmpKey")).provide(Blocking.Live)
          } yield assert(fileExist, isTrue) && assert(fileSize, isGreaterThan(0L))

          test.provideManaged(utils.s3)
        },
        testM("stream lines") {
          val test =
            for {
              list <- streamLines(S3ObjectSummary(bucketName, "dir1/user.csv")).runCollect
            } yield assert(list.headOption, isSome(equalTo("John,Doe,120 jefferson st.,Riverside, NJ, 08075"))) && assert(
              list.lastOption,
              isSome(equalTo("Marie,White,20 time square,Bronx, NY,08220"))
            )

          test.provideManaged(utils.s3)
        },
        testM("stream lines - invalid key") {
          val test = for {
            succeed <- streamLines(S3ObjectSummary(bucketName, "blah")).runCollect.fold(_ => false, _ => true)
          } yield assert(succeed, isFalse)

          test.provideManaged(utils.s3)
        },
        testM("listDescendant") {
          val test = for {
            list <- listObjectsDescendant(bucketName, "").runCollect
          } yield assert(list.map(_.key), hasSameElements(List("console.log", "dir1/hello.txt", "dir1/user.csv")))

          test.provideManaged(utils.s3)
        }
      )
    )

object utils {
  val bucketName = "bucket-1"

  val s3 = for {
    settings <- S3Settings
                 .from(Region.CA_CENTRAL_1, S3Credentials("TESTKEY", "TESTSECRET"))
                 .toManaged_
    client <- S3.Live.connect(
               settings,
               Some(URI.create("http://localhost:9000"))
             )
  } yield client
}
