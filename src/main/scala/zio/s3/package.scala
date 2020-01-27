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

import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.S3Exception
import zio.s3.ZIOStreamUtils._
import zio.stream.{ ZSink, ZStream, ZStreamChunk }

package object s3 extends S3.Service[S3] {
  type S3Stream[A] = ZStream[S3, S3Exception, A]

  def listObjectsDescendant(bucketName: String, prefix: String): S3Stream[S3ObjectSummary] = accessStream[S3] { env =>
    ZStream
      .fromEffect(env.s3.listObjects(bucketName, prefix, 1000))
      .flatMap(
        paginate(_).mapConcat(_.objectSummaries)
      )
  }

  def paginate(initialListing: S3ObjectListing): S3Stream[S3ObjectListing] = accessStream[S3] { env =>
    ZStream.paginate(initialListing) {
      case current @ S3ObjectListing(_, _, None) => ZIO.succeed(current -> None)
      case current                               => env.s3.getNextObjects(current).map(next => current -> Some(next))
    }
  }

  def streamLines(objectSummary: S3ObjectSummary): S3Stream[String] = accessStream[S3] {
    _.s3
      .getObject(objectSummary.bucketName, objectSummary.key)
      .chunks
      .aggregate(ZSink.utf8DecodeChunk)
      .aggregate(ZSink.splitLines)
      .mapConcatChunk(identity)
  }

  def createBucket(bucketName: String): ZIO[S3, S3Exception, Unit] =
    ZIO.accessM(_.s3.createBucket(bucketName))

  def deleteBucket(bucketName: String): ZIO[S3, S3Exception, Unit] =
    ZIO.accessM(_.s3.deleteBucket(bucketName))

  def isBucketExists(bucketName: String): ZIO[S3, S3Exception, Boolean] =
    ZIO.accessM(_.s3.isBucketExists(bucketName))

  def listBuckets(): ZIO[S3, S3Exception, S3BucketListing] =
    ZIO.accessM(_.s3.listBuckets())

  def deleteObject(bucketName: String, key: String): ZIO[S3, S3Exception, Unit] =
    ZIO.accessM(_.s3.deleteObject(bucketName, key))

  def getObject(bucketName: String, key: String): ZStreamChunk[S3, S3Exception, Byte] =
    ZStreamChunk(accessStream(_.s3.getObject(bucketName, key).chunks))

  def listObjects_(bucketName: String): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.accessM(_.s3.listObjects(bucketName, "", 1000))

  def listObjects(bucketName: String, prefix: String, maxKeys: Int): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.accessM(_.s3.listObjects(bucketName, prefix, maxKeys))

  def getNextObjects(listing: S3ObjectListing): ZIO[S3, S3Exception, S3ObjectListing] =
    ZIO.accessM(_.s3.getNextObjects(listing))

  def putObject_[R1 <: S3](
    bucketName: String,
    key: String,
    contentLength: Long,
    content: ZStreamChunk[R1, Throwable, Byte]
  ): ZIO[R1, S3Exception, Unit] =
    ZIO.accessM(_.s3.putObject(bucketName, key, contentLength, "application/octet-stream", content))

  def putObject[R1 <: S3](
    bucketName: String,
    key: String,
    contentLength: Long,
    contentType: String,
    content: ZStreamChunk[R1, Throwable, Byte]
  ): ZIO[R1, S3Exception, Unit] =
    ZIO.accessM(_.s3.putObject(bucketName, key, contentLength, contentType, content))

  def execute[T](f: S3AsyncClient => CompletableFuture[T]): ZIO[S3, S3Exception, T] =
    ZIO.accessM(_.s3.execute(f))

  def multipartUpload[R1 <: S3](n: Int)(
    bucketName: String,
    key: String,
    contentType: String,
    content: ZStreamChunk[R1, Throwable, Byte]
  ): ZIO[R1, S3Exception, Unit] =
    ZIO.accessM(_.s3.multipartUpload(n)(bucketName, key, contentType, content))
}
