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
import zio.nio.core.file.{ Path => ZPath }
import zio.s3.S3Bucket.S3BucketListing
import zio.stream.{ Stream, ZStream, ZTransducer }

package object s3 {
  type S3          = Has[S3.Service]
  type S3Stream[A] = ZStream[S3, S3Exception, A]

  /**
   * The `S3` module provides access to a s3 amazon storage.
   * All operations are async since we are relying on the amazon async client
   */
  object S3 {

    trait Service {

      /**
       * Create a bucket
       *
       * @param bucketName name of the bucket
       */
      def createBucket(bucketName: String): IO[S3Exception, Unit]

      /**
       * Delete bucket, the operation fail if bucket is not present
       *
       * @param bucketName name of the bucket
       */
      def deleteBucket(bucketName: String): IO[S3Exception, Unit]

      /**
       * Check if bucket exists
       *
       * @param bucketName name of the bucket
       */
      def isBucketExists(bucketName: String): IO[S3Exception, Boolean]

      /**
       * List all available buckets
       */
      val listBuckets: IO[S3Exception, S3BucketListing]

      /**
       * delete an object from a bucket, if not present it will succeed
       *
       * @param bucketName name of the bucket
       * @param key object identifier to remove
       */
      def deleteObject(bucketName: String, key: String): IO[S3Exception, Unit]

      /**
       * Read an object from a bucket, the operation fail if object is not present
       *
       * @param bucketName name of the bucket
       * @param key object identifier to read
       * @return
       */
      def getObject(bucketName: String, key: String): Stream[S3Exception, Byte]

      /**
       * list all object for a specific bucket
       *
       * @param bucketName name of the bucket
       * @param prefix filter all object key by the prefix
       * @param maxKeys max total number of objects
       */
      def listObjects(bucketName: String, prefix: String, maxKeys: Long): IO[S3Exception, S3ObjectListing]

      /**
       * Fetch the next object listing from a specific object listing.
       *
       * @param listing listing to use as a start
       */
      def getNextObjects(listing: S3ObjectListing): IO[S3Exception, S3ObjectListing]

      /**
       * Store data object into a specific bucket
       *
       * @param bucketName name of the bucket
       * @param key unique object identifier
       *
       * @param contentLength length of the data in bytes
       * @param contentType content type of the object (json, csv, txt, binary, ...)
       * @param content object data
       * @return
       */
      def putObject[R <: zio.Has[_]: Tag](
        bucketName: String,
        key: String,
        contentLength: Long,
        contentType: String,
        content: ZStream[R, Throwable, Byte]
      ): ZIO[R, S3Exception, Unit]

      /**
       * *
       *
       * Store data object into a specific bucket, minimun size of the data is 5 Mb to use multipartt upload (restriction from amazon API)
       *
       * @param bucketName name of the bucket
       * @param key unique object identifier
       * @param contentType content type of the object (json, csv, txt, binary, ...)
       * @param content object data
       */
      def multipartUpload[R <: zio.Has[_]: Tag](
        bucketName: String,
        key: String,
        contentType: String,
        content: ZStream[R, Throwable, Byte]
      ): ZIO[R, S3Exception, Unit]

      /**
       * *
       * expose safely amazon s3 async client
       *
       * @param f call any operations on s3 async client
       * @tparam T value type to return
       */
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

  /**
   * List all descendant objects of a bucket
   * Fetch all objects recursively of all nested directory by traversing all of them
   *
   * @param bucketName name of the bucket
   * @param prefix filter all object identifier which start with this `prefix`
   */
  def listObjectsDescendant(bucketName: String, prefix: String): S3Stream[S3ObjectSummary] =
    ZStream.accessStream[S3](env =>
      ZStream
        .fromEffect(env.get.listObjects(bucketName, prefix, 1000))
        .flatMap(
          paginate(_).mapConcat(_.objectSummaries)
        )
    )

  /**
   * List all objects by traversing all nested directories
   *
   * @param initialListing object listing to start with
   * @return
   */
  def paginate(initialListing: S3ObjectListing): S3Stream[S3ObjectListing] =
    ZStream.accessStream[S3](env =>
      ZStream.paginateM(initialListing) {
        case current @ S3ObjectListing(_, _, None) => ZIO.succeed(current -> None)
        case current                               => env.get.getNextObjects(current).map(next => current -> Some(next))
      }
    )

  /**
   * Read an object by lines
   *
   * @param objectSummary object to read define by a bucketName and object key
   */
  def streamLines(objectSummary: S3ObjectSummary): S3Stream[String] =
    ZStream.accessStream[S3](
      _.get
        .getObject(objectSummary.bucketName, objectSummary.key)
        .transduce(ZTransducer.utf8Decode)
        .transduce(ZTransducer.splitLines)
    )

  def createBucket(bucketName: String): ZIO[S3, S3Exception, Unit] =
    ZIO.accessM(_.get.createBucket(bucketName))

  def deleteBucket(bucketName: String): ZIO[S3, S3Exception, Unit] =
    ZIO.accessM(_.get.deleteBucket(bucketName))

  def isBucketExists(bucketName: String): ZIO[S3, S3Exception, Boolean] =
    ZIO.accessM(_.get.isBucketExists(bucketName))

  val listBuckets: ZIO[S3, S3Exception, S3BucketListing] =
    ZIO.accessM(_.get.listBuckets)

  def deleteObject(bucketName: String, key: String): ZIO[S3, S3Exception, Unit] =
    ZIO.accessM(_.get.deleteObject(bucketName, key))

  def getObject(bucketName: String, key: String): ZStream[S3, S3Exception, Byte] =
    ZStream.accessStream(_.get.getObject(bucketName, key))

  /**
   * Same as listObjects with default values for an empty prefix and sets the maximum number of object max to `1000`
   *
   * @param bucketName name of the bucket
   */
  def listObjects_(bucketName: String): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.accessM(_.get.listObjects(bucketName, "", 1000))

  def listObjects(bucketName: String, prefix: String, maxKeys: Long): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.accessM(_.get.listObjects(bucketName, prefix, maxKeys))

  def getNextObjects(listing: S3ObjectListing): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.accessM(_.get.getNextObjects(listing))

  def putObject[R <: Has[_]: Tag](
    bucketName: String,
    key: String,
    contentLength: Long,
    contentType: String,
    content: ZStream[R, Throwable, Byte]
  ): ZIO[S3 with R, S3Exception, Unit] =
    ZIO.accessM[S3 with R](_.get.putObject(bucketName, key, contentLength, contentType, content))

  def multipartUpload[R <: Has[_]: Tag](
    bucketName: String,
    key: String,
    contentType: String,
    content: ZStream[R, Throwable, Byte]
  ): ZIO[S3 with R, S3Exception, Unit] =
    ZIO.accessM[S3 with R](_.get.multipartUpload(bucketName, key, contentType, content))

  def execute[T](f: S3AsyncClient => CompletableFuture[T]): ZIO[S3, S3Exception, T] =
    ZIO.accessM(_.get.execute(f))
}
