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
import zio.s3.providers.const
import zio.stream.ZStream

import java.net.URI
import java.util.concurrent.CompletableFuture

package object s3 {
  type S3Stream[A] = ZStream[S3, S3Exception, A]

  def live(region: Region, credentials: AwsCredentials, uriEndpoint: Option[URI] = None): Layer[S3Exception, S3] =
    liveZIO(region, const(credentials.accessKeyId, credentials.secretAccessKey), uriEndpoint)

  def liveZIO[R](
    region: Region,
    provider: RIO[R with Scope, AwsCredentialsProvider],
    uriEndpoint: Option[URI] = None
  ): ZLayer[R, S3Exception, S3] =
    ZLayer.scoped[R](
      ZIO
        .fromEither(S3Region.from(region))
        .flatMap(Live.connect[R](_, provider, uriEndpoint))
    )

  def settings[R](region: Region, cred: ZIO[R, S3Exception, AwsCredentials]): ZLayer[R, S3Exception, S3Settings] =
    ZLayer(cred.flatMap(S3Settings.from(region, _)))

  val live: ZLayer[S3Settings, ConnectionError, S3] = ZLayer.scoped(
    ZIO.serviceWithZIO[S3Settings](s =>
      Live.connect(s.s3Region, const(s.credentials.accessKeyId, s.credentials.secretAccessKey), None)
    )
  )

  def stub(path: ZPath): ZLayer[Any, Nothing, S3] =
    ZLayer.succeed(Test.connect(path))

  def listAllObjects(bucketName: String): S3Stream[S3ObjectSummary] =
    ZStream.serviceWithStream[S3](_.listAllObjects(bucketName))

  def listAllObjects(bucketName: String, options: ListObjectOptions): S3Stream[S3ObjectSummary] =
    ZStream.serviceWithStream[S3](_.listAllObjects(bucketName, options))

  def paginate(initialListing: S3ObjectListing): S3Stream[S3ObjectListing] =
    ZStream.serviceWithStream[S3](_.paginate(initialListing))

  def streamLines(bucketName: String, key: String): S3Stream[String] =
    ZStream.serviceWithStream[S3](_.streamLines(bucketName, key))

  def createBucket(bucketName: String): ZIO[S3, S3Exception, Unit] =
    ZIO.serviceWithZIO(_.createBucket(bucketName))

  def deleteBucket(bucketName: String): ZIO[S3, S3Exception, Unit] =
    ZIO.serviceWithZIO(_.deleteBucket(bucketName))

  def isBucketExists(bucketName: String): ZIO[S3, S3Exception, Boolean] =
    ZIO.serviceWithZIO(_.isBucketExists(bucketName))

  val listBuckets: ZIO[S3, S3Exception, S3BucketListing] =
    ZIO.serviceWithZIO(_.listBuckets)

  def deleteObject(bucketName: String, key: String): ZIO[S3, S3Exception, Unit] =
    ZIO.serviceWithZIO(_.deleteObject(bucketName, key))

  def getObject(bucketName: String, key: String): ZStream[S3, S3Exception, Byte] =
    ZStream.serviceWithStream(_.getObject(bucketName, key))

  def getObjectMetadata(bucketName: String, key: String): ZIO[S3, S3Exception, ObjectMetadata] =
    ZIO.serviceWithZIO(_.getObjectMetadata(bucketName, key))

  /**
   * Same as listObjects with default values for an empty prefix and sets the maximum number of object max to `1000`
   *
   * @param bucketName name of the bucket
   */
  def listObjects(bucketName: String): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.serviceWithZIO(_.listObjects(bucketName))

  def listObjects(bucketName: String, options: ListObjectOptions): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.serviceWithZIO(_.listObjects(bucketName, options))

  def getNextObjects(listing: S3ObjectListing): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.serviceWithZIO(_.getNextObjects(listing))

  def putObject[R](
    bucketName: String,
    key: String,
    contentLength: Long,
    content: ZStream[R, Throwable, Byte],
    options: UploadOptions = UploadOptions.default
  ): ZIO[S3 with R, S3Exception, Unit] =
    ZIO.serviceWithZIO[S3](_.putObject(bucketName, key, contentLength, content, options))

  /**
   * Same as multipartUpload with default parallelism = 1
   *
   * @param bucketName name of the bucket
   * @param key unique object identifier
   * @param content object data
   * @param options the optional configurations of the multipart upload
   */
  def multipartUpload[R](
    bucketName: String,
    key: String,
    content: ZStream[R, Throwable, Byte],
    options: MultipartUploadOptions = MultipartUploadOptions.default
  )(parallelism: Int): ZIO[S3 with R, S3Exception, Unit] =
    ZIO.serviceWithZIO[S3](_.multipartUpload(bucketName, key, content, options)(parallelism))

  def execute[T](f: S3AsyncClient => CompletableFuture[T]): ZIO[S3, S3Exception, T] =
    ZIO.serviceWithZIO(_.execute(f))
}
