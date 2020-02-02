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

package zio

import java.net.URI
import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.S3Exception
import zio.blocking.Blocking
import zio.s3.S3Bucket.S3BucketListing
import zio.stream.{StreamChunk, ZSink, ZStream, ZStreamChunk}
import zio.nio.core.file.{Path => ZPath}

package object s3 {
  type S3          = Has[S3.Service]
  type S3Stream[A] = ZStream[S3, S3Exception, A]

  object S3 {

    trait Service {
      def createBucket(bucketName: String): IO[S3Exception, Unit]

      def deleteBucket(bucketName: String): IO[S3Exception, Unit]

      def isBucketExists(bucketName: String): IO[S3Exception, Boolean]

      val listBuckets: IO[S3Exception, S3BucketListing]

      def deleteObject(bucketName: String, key: String): IO[S3Exception, Unit]

      def getObject(bucketName: String, key: String): StreamChunk[S3Exception, Byte]

      def listObjects(bucketName: String, prefix: String, maxKeys: Long): IO[S3Exception, S3ObjectListing]

      def getNextObjects(listing: S3ObjectListing): IO[S3Exception, S3ObjectListing]

      def putObject[R <: zio.Has[_]: Tagged](
        bucketName: String,
        key: String,
        contentLength: Long,
        contentType: String,
        content: ZStreamChunk[R, Throwable, Byte]
      ): ZIO[R, S3Exception, Unit]

      def multipartUpload[R <: zio.Has[_]: Tagged](n: Int)(
        bucketName: String,
        key: String,
        contentType: String,
        content: ZStreamChunk[R, Throwable, Byte]
      ): ZIO[R, S3Exception, Unit]

      def execute[T](f: S3AsyncClient => CompletableFuture[T]): IO[S3Exception, T]
    }
  }

  def live(region: String, credentials: S3Credentials): Layer[ConnectionError, S3] =
    live(region, credentials, None)

  def live(region: String, credentials: S3Credentials, uriEndpoint: Option[URI]): Layer[ConnectionError, S3] =
    ZLayer.fromManaged(Live.connect(region, credentials, uriEndpoint))

  val live: ZLayer[S3Settings, ConnectionError, S3] = ZLayer.fromFunctionManaged(Live.connect(_, None))

  def test(path: ZPath): ZLayer[Blocking, Any, S3] =
    ZLayer.fromFunction(Test.connect(path))

  def listObjectsDescendant(bucketName: String, prefix: String): S3Stream[S3ObjectSummary] =
    ZStream.accessStream[S3](env =>
      ZStream
        .fromEffect(env.get.listObjects(bucketName, prefix, 1000))
        .flatMap(
          paginate(_).mapConcat(_.objectSummaries)
        )
    )

  def paginate(initialListing: S3ObjectListing): S3Stream[S3ObjectListing] =
    ZStream.accessStream[S3](env =>
      ZStream.paginateM(initialListing) {
        case current @ S3ObjectListing(_, _, None) => ZIO.succeed(current -> None)
        case current                               => env.get.getNextObjects(current).map(next => current -> Some(next))
      }
    )

  def streamLines(objectSummary: S3ObjectSummary): S3Stream[String] = ZStream.accessStream[S3](
    _.get
      .getObject(objectSummary.bucketName, objectSummary.key)
      .chunks
      .aggregate(ZSink.utf8DecodeChunk)
      .aggregate(ZSink.splitLines)
      .mapConcatChunk(identity)
  )

  def createBucket(bucketName: String): ZIO[S3, S3Exception, Unit] =
    ZIO.accessM(_.get.createBucket(bucketName))

  def deleteBucket(bucketName: String): ZIO[S3, S3Exception, Unit] =
    ZIO.accessM(_.get.deleteBucket(bucketName))

  def isBucketExists(bucketName: String): ZIO[S3, S3Exception, Boolean] =
    ZIO.accessM(_.get.isBucketExists(bucketName))

  def listBuckets(): ZIO[S3, S3Exception, S3BucketListing] =
    ZIO.accessM(_.get.listBuckets)

  def deleteObject(bucketName: String, key: String): ZIO[S3, S3Exception, Unit] =
    ZIO.accessM(_.get.deleteObject(bucketName, key))

  def getObject(bucketName: String, key: String): ZStreamChunk[S3, S3Exception, Byte] =
    ZStreamChunk(ZStream.accessStream(_.get.getObject(bucketName, key).chunks))

  def listObjects_(bucketName: String): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.accessM(_.get.listObjects(bucketName, "", 1000))

  def listObjects(bucketName: String, prefix: String, maxKeys: Long): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.accessM(_.get.listObjects(bucketName, prefix, maxKeys))

  def getNextObjects(listing: S3ObjectListing): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.accessM(_.get.getNextObjects(listing))

  def putObject[R <: Has[_]: Tagged](
    bucketName: String,
    key: String,
    contentLength: Long,
    contentType: String,
    content: ZStreamChunk[R, Throwable, Byte]
  ): ZIO[S3 with R, S3Exception, Unit] =
    ZIO.accessM[S3 with R](_.get.putObject(bucketName, key, contentLength, contentType, content))

  def multipartUpload[R <: Has[_]: Tagged](n: Int)(
    bucketName: String,
    key: String,
    contentType: String,
    content: ZStreamChunk[R, Throwable, Byte]
  ): ZIO[S3 with R, S3Exception, Unit] =
    ZIO.accessM[S3 with R](_.get.multipartUpload(n)(bucketName, key, contentType, content))

  def execute[T](f: S3AsyncClient => CompletableFuture[T]): ZIO[S3, S3Exception, T] =
    ZIO.accessM(_.get.execute(f))

}
