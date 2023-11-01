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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.async.{ AsyncRequestBody, AsyncResponseTransformer, SdkPublisher }
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.{ S3AsyncClient, S3AsyncClientBuilder }
import zio._
import zio.interop.reactivestreams._
import zio.s3.Live.{ StreamAsyncResponseTransformer, StreamResponse }
import zio.s3.S3Bucket.S3BucketListing
import zio.s3.errors._
import zio.s3.errors.syntax._
import zio.stream.{ Stream, ZChannel, ZPipeline, ZSink, ZStream }

import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

/**
 * Service use to wrap the unsafe amazon s3 client and access safely to s3 storage
 *
 * @param unsafeClient: Amazon Async S3 Client
 */
final class Live(unsafeClient: S3AsyncClient) extends S3 {

  override def createBucket(bucketName: String): IO[S3Exception, Unit] =
    execute(_.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())).unit

  override def deleteBucket(bucketName: String): IO[S3Exception, Unit] =
    execute(_.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())).unit

  override def isBucketExists(bucketName: String): IO[S3Exception, Boolean] =
    execute(_.headBucket(HeadBucketRequest.builder().bucket(bucketName).build()))
      .as(true)
      .catchSome {
        case _: NoSuchBucketException => ZIO.succeed(false)
      }

  override val listBuckets: IO[S3Exception, S3BucketListing] =
    execute(_.listBuckets())
      .map(r => S3Bucket.fromBuckets(r.buckets().asScala.toList))

  override def getObject(bucketName: String, key: String): Stream[S3Exception, Byte] =
    ZStream
      .fromZIO(
        execute(
          _.getObject[StreamResponse](
            GetObjectRequest.builder().bucket(bucketName).key(key).build(),
            StreamAsyncResponseTransformer(new CompletableFuture[StreamResponse]())
          )
        )
      )
      .flatMap(identity)
      .flattenChunks
      .mapErrorCause(_.flatMap(_.asS3Exception()))

  override def getObjectMetadata(bucketName: String, key: String): IO[S3Exception, ObjectMetadata] =
    execute(_.headObject(HeadObjectRequest.builder().bucket(bucketName).key(key).build()))
      .map(ObjectMetadata.fromResponse)

  override def deleteObject(bucketName: String, key: String): IO[S3Exception, Unit] =
    execute(_.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(key).build())).unit

  override def listObjects(bucketName: String, options: ListObjectOptions): IO[S3Exception, S3ObjectListing] =
    execute(
      _.listObjectsV2(
        ListObjectsV2Request
          .builder()
          .maxKeys(options.maxKeys.intValue())
          .bucket(bucketName)
          .delimiter(options.delimiter.orNull)
          .startAfter(options.starAfter.orNull)
          .prefix(options.prefix.orNull)
          .build()
      )
    ).map(S3ObjectListing.fromResponse)

  override def getNextObjects(listing: S3ObjectListing): IO[S3Exception, S3ObjectListing] =
    listing.nextContinuationToken
      .fold[ZIO[Any, S3Exception, S3ObjectListing]](
        ZIO.succeed(listing.copy(nextContinuationToken = None, objectSummaries = Chunk.empty))
      ) { token =>
        execute(
          _.listObjectsV2(
            ListObjectsV2Request
              .builder()
              .bucket(listing.bucketName)
              .continuationToken(token)
              .prefix(listing.prefix.orNull)
              .build()
          )
        ).map(S3ObjectListing.fromResponse)
      }

  override def putObject[R](
    bucketName: String,
    key: String,
    contentLength: Long,
    content: ZStream[R, Throwable, Byte],
    options: UploadOptions,
    contentMD5: Option[String] = None
  ): ZIO[R, S3Exception, Unit] =
    content
      .mapErrorCause(_.flatMap(_.asS3Exception()))
      .mapChunks(c => Chunk(ByteBuffer.wrap(c.toArray)))
      .toPublisher
      .flatMap { publisher =>
        execute(
          _.putObject(
            {
              val builder = PutObjectRequest
                .builder()
                .bucket(bucketName)
                .contentLength(contentLength)
                .key(key)
                .metadata(options.metadata.asJava)
                .acl(options.cannedAcl)

              List(
                (b: PutObjectRequest.Builder) =>
                  options.contentType
                    .fold(b)(b.contentType),
                (b: PutObjectRequest.Builder) =>
                  contentMD5
                    .fold(b)(b.contentMD5)
              ).foldLeft(builder) { case (b, f) => f(b) }.build()
            },
            AsyncRequestBody.fromPublisher(publisher)
          )
        )
      }
      .unit

  def multipartUploadSink(bucketName: String, key: String, options: MultipartUploadOptions)(
    parallelism: Int
  ): ZSink[Any, S3Exception, Byte, Byte, Unit] = {

    val startUpload = ZSink.fromZIO(
      execute(
        _.createMultipartUpload {
          val builder = CreateMultipartUploadRequest
            .builder()
            .bucket(bucketName)
            .key(key)
            .metadata(options.uploadOptions.metadata.asJava)
            .acl(options.uploadOptions.cannedAcl)
          options.uploadOptions.contentType
            .fold(builder)(builder.contentType)
            .build()
        }
      )
        .map(_.uploadId())
    )

    def completeUpload(uploadId: String, parts: Chunk[CompletedPart]) =
      ZSink.fromZIO(
        execute(
          _.completeMultipartUpload(
            CompleteMultipartUploadRequest
              .builder()
              .bucket(bucketName)
              .key(key)
              .multipartUpload(CompletedMultipartUpload.builder().parts(parts.asJavaCollection).build())
              .uploadId(uploadId)
              .build()
          )
        )
      )

    def uploadParts(uploadId: String): ZSink[Any, S3Exception, Byte, Nothing, Chunk[CompletedPart]] = {
      def go(
        hasFirst: Boolean,
        partNumber: Int
      ): ZChannel[Any, S3Exception, Chunk[Byte], Any, S3Exception, (Chunk[Byte], Int), Unit] =
        ZChannel.readWith(
          (in: Chunk[Byte]) => ZChannel.write(in -> partNumber) *> go(hasFirst = true, partNumber + 1),
          (err: S3Exception) => ZChannel.fail(err),
          (_: Any) =>
            if (hasFirst) ZChannel.unit
            else ZChannel.write(Chunk.empty -> partNumber) *> ZChannel.unit
        )

      def writer(
        uploadId: String
      ): ZChannel[Any, Nothing, Chunk[Byte], Any, S3Exception, Chunk[CompletedPart], Unit] =
        go(hasFirst = false, 1)
          .mapOutZIOPar(parallelism) {
            case (chunk, partNumber) =>
              execute(
                _.uploadPart(
                  UploadPartRequest
                    .builder()
                    .bucket(bucketName)
                    .key(key)
                    .partNumber(partNumber)
                    .uploadId(uploadId)
                    .contentLength(chunk.length.toLong)
                    .build(),
                  AsyncRequestBody.fromBytes(chunk.toArray)
                )
              ).map(r => Chunk.single(CompletedPart.builder().partNumber(partNumber).eTag(r.eTag()).build()))
          }
          .mapErrorCause(_.flatMap(_.asS3Exception()))

      ZSink.fromChannel(writer(uploadId)).collectLeftover.map(_._2)
    }

    val writeThrough = ZSink.fromChannel(ZChannel.identity[S3Exception, Chunk[Byte], Any])

    val uploader = for {
      uploadId <- startUpload
      parts    <- uploadParts(uploadId).zipParLeft(writeThrough)
      _        <- completeUpload(uploadId, parts)
    } yield ()

    ZSink.unwrap(for {
      _ <- ZIO.dieMessage(s"parallelism must be > 0. $parallelism is invalid").unless(parallelism > 0)
      _ <-
        ZIO
          .dieMessage(
            s"Invalid part size ${Math.floor(options.partSize.toDouble / PartSize.Mega.toDouble * 100d) / 100d} Mb, minimum size is ${PartSize.Min / PartSize.Mega} Mb"
          )
          .unless(options.partSize >= PartSize.Min)
    } yield ZPipeline.rechunk[Byte](options.partSize) >>> uploader)
  }

  def execute[T](f: S3AsyncClient => CompletableFuture[T]): ZIO[Any, S3Exception, T] =
    ZIO.fromCompletionStage(f(unsafeClient)).refineOrDie {
      case s3: S3Exception   => s3
      case sdk: SdkException => SdkError(sdk)
    }
}

object Live {

  def connect[R](
    region: S3Region,
    provider: RIO[R with Scope, AwsCredentialsProvider],
    uriEndpoint: Option[URI]
  ): ZIO[R with Scope, ConnectionError, S3] =
    for {
      credentials <- provider.mapError(e => ConnectionError(e.getMessage, e.getCause))
      builder     <- ZIO.succeed {
                       val builder = S3AsyncClient
                         .builder()
                         .credentialsProvider(credentials)
                         .region(region.region)
                       uriEndpoint.foreach(builder.endpointOverride)
                       builder
                     }
      service     <- connect(builder)
    } yield service

  def connect[R](builder: S3AsyncClientBuilder): ZIO[R with Scope, ConnectionError, S3] =
    ZIO
      .fromAutoCloseable(ZIO.attempt(builder.build()))
      .mapBoth(e => ConnectionError(e.getMessage, e.getCause), new Live(_))

  type StreamResponse = ZStream[Any, Throwable, Chunk[Byte]]

  final private[s3] case class StreamAsyncResponseTransformer(cf: CompletableFuture[StreamResponse])
      extends AsyncResponseTransformer[GetObjectResponse, StreamResponse] {
    override def prepare(): CompletableFuture[StreamResponse] = cf

    override def onResponse(response: GetObjectResponse): Unit = ()

    override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = {
      cf.complete(publisher.toZIOStream().map(Chunk.fromByteBuffer))
      ()
    }

    override def exceptionOccurred(error: Throwable): Unit = {
      cf.completeExceptionally(error)
      ()
    }
  }

}
