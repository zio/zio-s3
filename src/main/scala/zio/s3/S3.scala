/*
 * Copyright 2017-2020 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.s3

import java.io.{ FileInputStream, FileOutputStream }
import java.nio.ByteBuffer
import java.nio.file.attribute.PosixFileAttributes
import java.time.Instant
import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.core.async.{ AsyncRequestBody, AsyncResponseTransformer, SdkPublisher }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import zio.blocking.Blocking
import zio.interop.javaz.fromCompletionStage
import zio.interop.reactiveStreams._
import zio.stream.{ ZSink, ZStream, ZStreamChunk }
import zio.nio.file._
import zio.nio.file.{ Path => ZPath }
import zio.{ Chunk, Managed, Task, UIO, ZIO, ZManaged }

import scala.collection.JavaConverters._

trait S3 {
  val s3: S3.Service[Any]
}

object S3 {

  trait Service[R] {
    def createBucket(bucketName: String): ZIO[R, S3Exception, Unit]

    def deleteBucket(bucketName: String): ZIO[R, S3Exception, Unit]

    def isBucketExists(bucketName: String): ZIO[R, S3Exception, Boolean]

    def listBuckets(): ZIO[R, S3Exception, S3BucketListing]

    def deleteObject(bucketName: String, key: String): ZIO[R, S3Exception, Unit]

    def getObject(bucketName: String, key: String): ZStreamChunk[R, S3Exception, Byte]

    def listObjects(bucketName: String, prefix: String, maxKeys: Int): ZIO[R, S3Exception, S3ObjectListing]

    def getNextObjects(listing: S3ObjectListing): ZIO[R, S3Exception, S3ObjectListing]

    def putObject[R1](
      bucketName: String,
      key: String,
      contentLength: Long,
      contentType: String,
      content: ZStreamChunk[R1, Throwable, Byte]
    ): ZIO[R with R1, S3Exception, Unit]

    def execute[T](f: S3AsyncClient => CompletableFuture[T]): ZIO[R, S3Exception, T]
  }

  final class Live private (unsafeClient: S3AsyncClient) extends Service[Any] {

    override def deleteObject(bucketName: String, key: String): ZIO[Any, S3Exception, Unit] =
      execute(_.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(key).build())).unit

    override def createBucket(bucketName: String): ZIO[Any, S3Exception, Unit] =
      execute(_.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())).unit

    override def deleteBucket(bucketName: String): ZIO[Any, S3Exception, Unit] =
      execute(_.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())).unit

    override def isBucketExists(bucketName: String): ZIO[Any, S3Exception, Boolean] =
      execute(_.headBucket(HeadBucketRequest.builder().bucket(bucketName).build()))
        .map(_ => true)
        .catchSome {
          case _: NoSuchBucketException => Task.succeed(false)
        }

    override def listBuckets(): ZIO[Any, S3Exception, S3BucketListing] =
      execute(_.listBuckets())
        .map(S3BucketListing(_))

    override def getObject(bucketName: String, key: String): ZStreamChunk[Any, S3Exception, Byte] =
      ZStreamChunk(
        ZStream
          .fromEffect(
            execute(
              _.getObject[StreamResponse](
                GetObjectRequest.builder().bucket(bucketName).key(key).build(),
                StreamAsyncResponseTransformer(new CompletableFuture[StreamResponse]())
              )
            )
          )
          .flatMap(identity)
          .mapError(S3.S3ExceptionLike)
      )

    override def listObjects(bucketName: String, prefix: String, maxKeys: Int): ZIO[Any, S3Exception, S3ObjectListing] =
      execute(
        _.listObjectsV2(ListObjectsV2Request.builder().maxKeys(maxKeys).bucket(bucketName).prefix(prefix).build())
      ).map(S3ObjectListing(_))

    override def getNextObjects(listing: S3ObjectListing): ZIO[Any, S3Exception, S3ObjectListing] =
      listing.nextContinuationToken
        .fold[ZIO[Any, S3Exception, S3ObjectListing]](
          ZIO.succeed(listing.copy(nextContinuationToken = None, objectSummaries = Nil))
        ) { token =>
          execute(
            _.listObjectsV2(ListObjectsV2Request.builder().bucket(listing.bucketName).continuationToken(token).build())
          ).map(S3ObjectListing(_))
        }

    override def putObject[R1](
      bucketName: String,
      key: String,
      contentLength: Long,
      contentType: String,
      content: ZStreamChunk[R1, Throwable, Byte]
    ): ZIO[Any with R1, S3Exception, Unit] =
      content.chunks
        .map(c => ByteBuffer.wrap(c.toArray))
        .toPublisher
        .flatMap(
          publisher =>
            execute(
              _.putObject(
                PutObjectRequest
                  .builder()
                  .bucket(bucketName)
                  .contentLength(contentLength)
                  .contentType(contentType)
                  .key(key)
                  .build(),
                AsyncRequestBody.fromPublisher(publisher)
              )
            )
        )
        .unit

    def execute[T](f: S3AsyncClient => CompletableFuture[T]): ZIO[Any, S3Exception, T] =
      fromCompletionStage(UIO.apply(f(unsafeClient))).refineToOrDie[S3Exception]
  }

  private[s3] case class S3ExceptionLike(error: Throwable)
      extends S3Exception(S3Exception.builder().message(error.getMessage).cause(error.getCause))

  type StreamResponse = ZStream[Any, Throwable, Chunk[Byte]]

  final private[s3] case class StreamAsyncResponseTransformer(cf: CompletableFuture[StreamResponse])
      extends AsyncResponseTransformer[GetObjectResponse, StreamResponse] {
    override def prepare(): CompletableFuture[StreamResponse] = cf

    override def onResponse(response: GetObjectResponse): Unit = ()

    override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit =
      cf.complete(publisher.toStream().map(ChunkUtils.fromByteBuffer))

    override def exceptionOccurred(error: Throwable): Unit =
      cf.completeExceptionally(error)
  }

  object Live {

    def connect(region: Region, credentials: S3Credentials): Managed[S3Failure, S3] =
      for {
        settings <- S3Settings.from(region, credentials, None).toManaged_
        client   <- connect(settings)
      } yield client

    def connect(settings: S3Settings): Managed[S3Failure, S3] =
      ZManaged
        .fromAutoCloseable(
          Task {
            val builder = S3AsyncClient
              .builder()
              .credentialsProvider(
                StaticCredentialsProvider.create(
                  AwsBasicCredentials.create(settings.credentials.accessKeyId, settings.credentials.secretAccessKey)
                )
              )
              .region(settings.region)
            settings.uriEndpoint.foreach(builder.endpointOverride)
            builder.build()
          }
        )
        .map(
          client =>
            new S3 {
              val s3 = new Live(client)
            }
        )
        .mapError(e => ConnectionError(e.getMessage, e.getCause))
  }

  final class Test private (path: ZPath) extends Service[Blocking] {

    override def createBucket(bucketName: String): ZIO[Blocking, S3Exception, Unit] =
      Files.createDirectory(path / bucketName).mapError(S3ExceptionLike)

    override def deleteBucket(bucketName: String): ZIO[Blocking, S3Exception, Unit] =
      Files.delete(path / bucketName).mapError(S3ExceptionLike)

    override def isBucketExists(bucketName: String): ZIO[Blocking, S3Exception, Boolean] =
      Files.exists(path / bucketName)

    override def listBuckets(): ZIO[Blocking, S3Exception, S3BucketListing] =
      Files
        .list(path)
        .filterM(p => Files.readAttributes[PosixFileAttributes](p).map(_.isDirectory))
        .mapM { p =>
          Files
            .readAttributes[PosixFileAttributes](p)
            .map(attr => S3Bucket(p.filename.toString, attr.creationTime().toInstant))
        }
        .runCollect
        .mapError(S3ExceptionLike)
        .map(S3BucketListing(_))

    override def deleteObject(bucketName: String, key: String): ZIO[Blocking, S3Exception, Unit] =
      Files.delete(path / bucketName / key).mapError(S3ExceptionLike(_))

    override def getObject(bucketName: String, key: String): ZStreamChunk[Blocking, S3Exception, Byte] = ZStreamChunk {
      ZStream
        .managed(ZManaged.fromAutoCloseable(Task(new FileInputStream((path / bucketName / key).toFile))))
        .flatMap(ZStream.fromInputStream(_, 2048).chunks)
        .mapError(S3ExceptionLike)
    }

    override def listObjects(
      bucketName: String,
      prefix: String,
      maxKeys: Int
    ): ZIO[Blocking, S3Exception, S3ObjectListing] =
      Files
        .find(path / bucketName) { (p, _) =>
          p.filename.toString().startsWith(prefix)
        }
        .take(maxKeys)
        .map(_.filename.toString())
        .runCollect
        .map(_.map(S3ObjectSummary(bucketName, _)))
        .map(S3ObjectListing(bucketName, _, None))
        .mapError(S3ExceptionLike)

    //TODO can we do better ???
    override def getNextObjects(listing: S3ObjectListing): ZIO[Blocking, S3Exception, S3ObjectListing] =
      ZIO.succeed(listing)

    override def putObject[R1](
      bucketName: String,
      key: String,
      contentLength: Long,
      contentString: String,
      content: ZStreamChunk[R1, Throwable, Byte]
    ): ZIO[Blocking with R1, S3Exception, Unit] =
      ZManaged
        .fromAutoCloseable(Task(new FileOutputStream((path / bucketName / key).toFile)))
        .use(os => content.run(ZSink.fromOutputStream(os)).unit)
        .mapError(S3ExceptionLike)

    override def execute[T](f: S3AsyncClient => CompletableFuture[T]): ZIO[Blocking, S3Exception, T] = ???
  }

//  object Test {
//
//    def connect(path: Path): ZManaged[Blocking, Nothing, S3] =
//      ZManaged.succeed(new S3 {
//        protected override val s3: Service[Any] = new Test(ZPath.fromJava(path))
//      })
//  }
}

case class S3BucketListing(buckets: List[S3Bucket])

object S3BucketListing {

  def apply(resp: ListBucketsResponse): S3BucketListing =
    S3BucketListing(resp.buckets().asScala.toList.map(S3Bucket(_)))
}

case class S3Bucket(name: String, creationDate: Instant)

object S3Bucket {
  def apply(bucket: Bucket): S3Bucket = new S3Bucket(bucket.name(), bucket.creationDate())
}

case class S3ObjectListing(
  bucketName: String,
  objectSummaries: List[S3ObjectSummary],
  nextContinuationToken: Option[String]
)

object S3ObjectListing {

  def apply(r: ListObjectsV2Response): S3ObjectListing =
    S3ObjectListing(
      r.name(),
      r.contents().asScala.toList.map(o => S3ObjectSummary(r.name(), o.key())),
      Option(r.nextContinuationToken())
    )
}

case class S3ObjectSummary(bucketName: String, key: String)
