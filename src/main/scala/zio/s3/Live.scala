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

import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.core.async.{ AsyncRequestBody, AsyncResponseTransformer, SdkPublisher }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._
import zio.interop.reactivestreams._
import zio.s3.Live.{ S3ExceptionUtils, StreamAsyncResponseTransformer, StreamResponse }
import zio.s3.S3Bucket.S3BucketListing
import zio.stream.{ Stream, ZSink, ZStream }
import zio.{ Tag, _ }

import scala.jdk.CollectionConverters._

/**
 * Service use to wrap the unsafe amazon s3 client and access safely to s3 storage
 *
 * @param unsafeClient: Amazon Async S3 Client
 */
final class Live(unsafeClient: S3AsyncClient) extends S3.Service {

  override def createBucket(bucketName: String): IO[S3Exception, Unit] =
    execute(_.createBucket(CreateBucketRequest.builder().bucket(bucketName).build())).unit

  override def deleteBucket(bucketName: String): IO[S3Exception, Unit] =
    execute(_.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build())).unit

  override def isBucketExists(bucketName: String): IO[S3Exception, Boolean] =
    execute(_.headBucket(HeadBucketRequest.builder().bucket(bucketName).build()))
      .map(_ => true)
      .catchSome {
        case _: NoSuchBucketException => Task.succeed(false)
      }

  override val listBuckets: IO[S3Exception, S3BucketListing] =
    execute(_.listBuckets())
      .map(r => S3Bucket.fromBuckets(r.buckets().asScala.toList))

  override def getObject(bucketName: String, key: String): Stream[S3Exception, Byte] =
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
      .flattenChunks
      .mapError(S3ExceptionUtils.fromThrowable)

  override def getObjectMetadata(bucketName: String, key: String): IO[S3Exception, ObjectMetadata] =
    execute(_.headObject(HeadObjectRequest.builder().bucket(bucketName).key(key).build()))
      .map(ObjectMetadata.fromResponse)

  override def deleteObject(bucketName: String, key: String): IO[S3Exception, Unit] =
    execute(_.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(key).build())).unit

  override def listObjects(bucketName: String, prefix: String, maxKeys: Long): IO[S3Exception, S3ObjectListing] =
    execute(
      _.listObjectsV2(
        ListObjectsV2Request.builder().maxKeys(maxKeys.intValue()).bucket(bucketName).prefix(prefix).build()
      )
    ).map(S3ObjectListing.fromResponse)

  override def getNextObjects(listing: S3ObjectListing): IO[S3Exception, S3ObjectListing] =
    listing.nextContinuationToken
      .fold[ZIO[Any, S3Exception, S3ObjectListing]](
        ZIO.succeed(listing.copy(nextContinuationToken = None, objectSummaries = Chunk.empty))
      ) { token =>
        execute(
          _.listObjectsV2(ListObjectsV2Request.builder().bucket(listing.bucketName).continuationToken(token).build())
        ).map(S3ObjectListing.fromResponse)
      }

  override def putObject[R <: zio.Has[_]: Tag](
    bucketName: String,
    key: String,
    contentLength: Long,
    content: ZStream[R, Throwable, Byte],
    options: UploadOptions
  ): ZIO[R, S3Exception, Unit] =
    content
      .mapChunks(Chunk.single)
      .map(c => ByteBuffer.wrap(c.toArray))
      .toPublisher
      .flatMap(publisher =>
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
              options.contentType
                .fold(builder)(builder.contentType)
                .build()
            },
            AsyncRequestBody.fromPublisher(publisher)
          )
        )
      )
      .unit

  def multipartUpload[R <: zio.Has[_]: Tag](
    bucketName: String,
    key: String,
    content: ZStream[R, Throwable, Byte],
    options: MultipartUploadOptions
  )(parallelism: Int): ZIO[R, S3Exception, Unit] =
    for {
      _        <- ZIO.cond(
                    parallelism > 0,
                    (),
                    S3ExceptionUtils.fromThrowable(
                      new IllegalArgumentException(s"parallelism must be > 0. $parallelism is invalid")
                    )
                  )

      _        <- ZIO.cond(
                    options.partSize >= PartSize.Min,
                    (),
                    S3ExceptionUtils.fromThrowable(
                      new IllegalArgumentException(
                        s"Invalid part size ${Math.floor(options.partSize.toDouble / PartSize.Mega.toDouble * 100d) / 100d} Mb, minimum size is ${PartSize.Min / PartSize.Mega} Mb"
                      )
                    )
                  )

      uploadId <- execute(
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
                  ).map(_.uploadId())

      parts    <- ZStream
                    .managed(
                      content
                        .chunkN(options.partSize)
                        .mapChunks(Chunk.single)
                        .peel(ZSink.head[Chunk[Byte]])
                    )
                    .flatMap {
                      case (Some(head), rest) => ZStream(head) ++ rest
                      case (None, _)          => ZStream(Chunk.empty)
                    }
                    .zipWithIndex
                    .mapMPar(parallelism) {
                      case (chunk, partNumber) =>
                        execute(
                          _.uploadPart(
                            UploadPartRequest
                              .builder()
                              .bucket(bucketName)
                              .key(key)
                              .partNumber(partNumber.toInt + 1)
                              .uploadId(uploadId)
                              .contentLength(chunk.length.toLong)
                              .build(),
                            AsyncRequestBody.fromBytes(chunk.toArray)
                          )
                        ).map(r => CompletedPart.builder().partNumber(partNumber.toInt + 1).eTag(r.eTag()).build())
                    }
                    .runCollect
                    .mapError(S3ExceptionUtils.fromThrowable)

      _        <- execute(
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
    } yield ()

  def execute[T](f: S3AsyncClient => CompletableFuture[T]): ZIO[Any, S3Exception, T] =
    ZIO.fromCompletionStage(f(unsafeClient)).refineToOrDie[S3Exception]
}

object Live {

  def connect(
    region: Region,
    credentials: S3Credentials,
    uriEndpoint: Option[URI]
  ): Managed[ConnectionError, S3.Service] =
    S3Settings(region, credentials).toManaged_
      .mapError(e => ConnectionError(e.getMessage, e.getCause))
      .flatMap(connect(_, uriEndpoint))

  def connect(settings: S3Settings, uriEndpoint: Option[URI]): Managed[ConnectionError, S3.Service] =
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
            .region(settings.s3Region.region)
          uriEndpoint.foreach(builder.endpointOverride)
          builder.build()
        }
      )
      .map(new Live(_))
      .mapError(e => ConnectionError(e.getMessage, e.getCause))

  object S3ExceptionUtils {

    private[s3] def fromThrowable(error: Throwable): S3Exception =
      S3Exception.builder().message(error.getMessage).cause(error.getCause).build().asInstanceOf[S3Exception]
  }

  type StreamResponse = ZStream[Any, Throwable, Chunk[Byte]]

  final private[s3] case class StreamAsyncResponseTransformer(cf: CompletableFuture[StreamResponse])
      extends AsyncResponseTransformer[GetObjectResponse, StreamResponse] {
    override def prepare(): CompletableFuture[StreamResponse] = cf

    override def onResponse(response: GetObjectResponse): Unit = ()

    override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = {
      cf.complete(publisher.toStream().map(Chunk.fromByteBuffer))
      ()
    }

    override def exceptionOccurred(error: Throwable): Unit = {
      cf.completeExceptionally(error)
      ()
    }
  }

}
