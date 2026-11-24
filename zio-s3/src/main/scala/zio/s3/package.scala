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

import software.amazon.awssdk.auth.credentials.{ AwsCredentials, AwsCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.S3Exception
import zio.nio.file.{ Path => ZPath }
import zio.s3.S3Bucket.S3BucketListing
import zio.s3.errors._
import zio.s3.providers.const
import zio.stream.ZStream

import java.net.URI
import java.util.concurrent.CompletableFuture

package object s3 {

  /**
   * A stream that needs the `S3` service in its environment and fails with an
   * `S3Exception`.
   */
  type S3Stream[A] = ZStream[S3, S3Exception, A]

  /**
   * Builds an `S3Settings` layer from a region and an effect that gives the AWS
   * credentials. The layer fails with an `S3Exception` when the credentials effect
   * fails, and with an `InvalidSettings` error when the region is not in the AWS SDK
   * region list (see `S3Region.from`).
   */
  def settings[R](region: Region, cred: ZIO[R, S3Exception, AwsCredentials]): ZLayer[R, S3Exception, S3Settings] =
    ZLayer(cred.flatMap(S3Settings.from(region, _)))

  /**
   * Builds an `S3` layer for `region`, signed with fixed `credentials`. `uriEndpoint`
   * replaces the default AWS endpoint, for S3-compatible storage. `forcePathStyle` is
   * passed to the AWS client when it is defined; when it is `None`, the SDK default is
   * used. The layer fails with an `InvalidSettings` error when the region is not in the
   * AWS SDK region list, and with a `ConnectionError` when the client cannot be built.
   * It dies when `uriEndpoint` has no scheme.
   *
   * {{{
   * import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
   * import software.amazon.awssdk.regions.Region
   * import software.amazon.awssdk.services.s3.model.S3Exception
   * import zio._
   * import zio.s3._
   *
   * val s3Layer: Layer[S3Exception, S3] =
   *   live(Region.US_EAST_1, AwsBasicCredentials.create("access-key", "secret-key"))
   * }}}
   */
  def live(
    region: Region,
    credentials: AwsCredentials,
    uriEndpoint: Option[URI] = None,
    forcePathStyle: Option[Boolean] = None
  ): Layer[S3Exception, S3] =
    liveZIO(region, const(credentials), uriEndpoint, forcePathStyle)

  /**
   * Like `live`, but the credentials provider comes from an effect that runs when the
   * layer is built. The AWS SDK asks the provider for credentials when it signs a
   * request. A failure of the provider effect becomes a `ConnectionError`.
   */
  def liveZIO[R](
    region: Region,
    provider: RIO[R, AwsCredentialsProvider],
    uriEndpoint: Option[URI] = None,
    forcePathStyle: Option[Boolean] = None
  ): ZLayer[R, S3Exception, S3] =
    ZLayer.scoped[R](
      ZIO
        .fromEither(S3Region.from(region))
        .flatMap(Live.connect[R](_, provider, uriEndpoint, forcePathStyle))
    )

  /**
   * Builds an `S3` layer from the `S3Settings` in the environment. The client uses the
   * default AWS endpoint and does not force path-style access. The layer fails with a
   * `ConnectionError` when the client cannot be built.
   */
  val live: ZLayer[S3Settings, ConnectionError, S3] = ZLayer.scoped(
    ZIO.serviceWithZIO[S3Settings](s => Live.connect(s.s3Region, const(s.credentials), None))
  )

  /**
   * Builds a test `S3` layer that keeps buckets and objects under the directory
   * `path`. The directory must exist already. See `Test` for the differences from
   * the AWS service.
   *
   * {{{
   * import zio._
   * import zio.nio.file.{ Path => ZPath }
   * import zio.s3._
   *
   * val testLayer: ZLayer[Any, Nothing, S3] = stub(ZPath("target/s3-stub"))
   * }}}
   *
   * @param path directory that holds the buckets
   */
  def stub(path: ZPath): ZLayer[Any, Nothing, S3] =
    ZLayer.fromZIO(Test.connect(path))

  /**
   * Streams the summaries of the objects in a bucket, using the `S3` service in the
   * environment. See `S3.listAllObjects`.
   *
   * {{{
   * import software.amazon.awssdk.services.s3.model.S3Exception
   * import zio._
   * import zio.s3._
   * import zio.stream._
   *
   * val keys: ZStream[S3, S3Exception, String] =
   *   listAllObjects("reports", ListObjectOptions.fromMaxKeys(500)).map(_.key)
   * }}}
   */
  def listAllObjects(bucketName: String): S3Stream[S3ObjectSummary] =
    ZStream.serviceWithStream[S3](_.listAllObjects(bucketName))

  /**
   * Streams the summaries of the objects that `options` selects, using the `S3`
   * service in the environment. See `S3.listAllObjects`.
   */
  def listAllObjects(bucketName: String, options: ListObjectOptions): S3Stream[S3ObjectSummary] =
    ZStream.serviceWithStream[S3](_.listAllObjects(bucketName, options))

  /**
   * Streams `initialListing` and the following pages, using the `S3` service in the
   * environment. See `S3.paginate`.
   */
  def paginate(initialListing: S3ObjectListing): S3Stream[S3ObjectListing] =
    ZStream.serviceWithStream[S3](_.paginate(initialListing))

  /**
   * Streams an object as text, one `String` per line, using the `S3` service in the
   * environment. See `S3.streamLines`.
   */
  def streamLines(bucketName: String, key: String): S3Stream[String] =
    ZStream.serviceWithStream[S3](_.streamLines(bucketName, key))

  /**
   * Creates a bucket, using the `S3` service in the environment. See `S3.createBucket`.
   */
  def createBucket(bucketName: String): ZIO[S3, S3Exception, Unit] =
    ZIO.serviceWithZIO(_.createBucket(bucketName))

  /**
   * Deletes a bucket, using the `S3` service in the environment. See `S3.deleteBucket`.
   */
  def deleteBucket(bucketName: String): ZIO[S3, S3Exception, Unit] =
    ZIO.serviceWithZIO(_.deleteBucket(bucketName))

  /**
   * Checks whether a bucket exists, using the `S3` service in the environment. See
   * `S3.isBucketExists`.
   */
  def isBucketExists(bucketName: String): ZIO[S3, S3Exception, Boolean] =
    ZIO.serviceWithZIO(_.isBucketExists(bucketName))

  /**
   * Lists the buckets, using the `S3` service in the environment. See `S3.listBuckets`.
   */
  val listBuckets: ZIO[S3, S3Exception, S3BucketListing] =
    ZIO.serviceWithZIO(_.listBuckets)

  /**
   * Deletes an object, using the `S3` service in the environment. See
   * `S3.deleteObject`.
   */
  def deleteObject(bucketName: String, key: String): ZIO[S3, S3Exception, Unit] =
    ZIO.serviceWithZIO(_.deleteObject(bucketName, key))

  /**
   * Streams the bytes of an object, using the `S3` service in the environment. See
   * `S3.getObject`.
   */
  def getObject(bucketName: String, key: String): ZStream[S3, S3Exception, Byte] =
    ZStream.serviceWithStream(_.getObject(bucketName, key))

  /**
   * Reads the metadata of an object, using the `S3` service in the environment. See
   * `S3.getObjectMetadata`.
   */
  def getObjectMetadata(bucketName: String, key: String): ZIO[S3, S3Exception, ObjectMetadata] =
    ZIO.serviceWithZIO(_.getObjectMetadata(bucketName, key))

  /**
   * Lists the first page of objects in a bucket, using the `S3` service in the
   * environment. The page has no prefix and at most 1000 keys. See `S3.listObjects`.
   *
   * @param bucketName name of the bucket
   */
  def listObjects(bucketName: String): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.serviceWithZIO(_.listObjects(bucketName))

  /**
   * Lists one page of objects in a bucket, using the `S3` service in the environment.
   * See `S3.listObjects`.
   *
   * @param bucketName name of the bucket
   * @param options prefix, page size, delimiter and start-after key of the page
   */
  def listObjects(bucketName: String, options: ListObjectOptions): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.serviceWithZIO(_.listObjects(bucketName, options))

  /**
   * Fetches the page that follows `listing`, using the `S3` service in the environment.
   * See `S3.getNextObjects`.
   */
  def getNextObjects(listing: S3ObjectListing): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.serviceWithZIO(_.getNextObjects(listing))

  /**
   * Stores an object, using the `S3` service in the environment. See `S3.putObject`
   * for the parameters and the differences between `Live` and the stub.
   */
  def putObject[R](
    bucketName: String,
    key: String,
    contentLength: Long,
    content: ZStream[R, Throwable, Byte],
    options: UploadOptions = UploadOptions.default,
    contentMD5: Option[String] = None
  ): ZIO[S3 with R, S3Exception, Unit] =
    ZIO.serviceWithZIO[S3](_.putObject(bucketName, key, contentLength, content, options, contentMD5))

  /**
   * Stores `content` in parts, using the `S3` service in the environment. The
   * parallelism is the second parameter list. See `S3.multipartUpload`.
   */
  def multipartUpload[R](
    bucketName: String,
    key: String,
    content: ZStream[R, Throwable, Byte],
    options: MultipartUploadOptions = MultipartUploadOptions.default
  )(parallelism: Int): ZIO[S3 with R, S3Exception, Unit] =
    ZIO.serviceWithZIO[S3](_.multipartUpload(bucketName, key, content, options)(parallelism))

  /**
   * Runs `f` on the AWS async client of the `S3` service in the environment. See
   * `S3.execute`.
   */
  def execute[T](f: S3AsyncClient => CompletableFuture[T]): ZIO[S3, S3Exception, T] =
    ZIO.serviceWithZIO(_.execute(f))
}
