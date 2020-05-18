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
import java.nio.file.attribute.PosixFileAttributes
import java.util.UUID
import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.S3Exception
import zio._
import zio.blocking.Blocking
import zio.nio.core.file.{ Path => ZPath }
import zio.nio.file.Files
import zio.s3.Live.S3ExceptionLike
import zio.s3.S3Bucket._
import zio.stream.{ Stream, ZSink, ZStream }
import zio.Tag

/**
 * Stub Service which is back by a filesystem storage
 */
object Test {

  def connect(path: ZPath): Blocking => S3.Service = { blocking =>
    new S3.Service {
      override def createBucket(bucketName: String): IO[S3Exception, Unit] =
        Files.createDirectory(path / bucketName).mapError(S3ExceptionLike).provide(blocking)

      override def deleteBucket(bucketName: String): IO[S3Exception, Unit] =
        Files.delete(path / bucketName).mapError(S3ExceptionLike).provide(blocking)

      override def isBucketExists(bucketName: String): IO[S3Exception, Boolean] =
        Files.exists(path / bucketName).provide(blocking)

      override val listBuckets: IO[S3Exception, S3BucketListing] =
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
          .provide(blocking)

      override def deleteObject(bucketName: String, key: String): IO[S3Exception, Unit] =
        Files.deleteIfExists(path / bucketName / key).mapError(S3ExceptionLike(_)).provide(blocking).unit

      override def getObject(bucketName: String, key: String): Stream[S3Exception, Byte] =
        ZStream
          .managed(ZManaged.fromAutoCloseable(Task(new FileInputStream((path / bucketName / key).toFile))))
          .flatMap(ZStream.fromInputStream(_, 2048))
          .mapError(S3ExceptionLike)
          .provide(blocking)

      override def listObjects(
        bucketName: String,
        prefix: String,
        maxKeys: Long
      ): IO[S3Exception, S3ObjectListing] =
        Files
          .find(path / bucketName) { (p, _) =>
            p.filename.toString().startsWith(prefix)
          }
          .filterM(p => Files.readAttributes[PosixFileAttributes](p).map(_.isRegularFile))
          .map(f => S3ObjectSummary(bucketName, (path / bucketName).relativize(f).toString()))
          .runCollect
          .map {
            case list if list.size > maxKeys =>
              S3ObjectListing(bucketName, list.take(maxKeys.toInt), Some(UUID.randomUUID().toString))
            case list => S3ObjectListing(bucketName, list, None)
          }
          .mapError(S3ExceptionLike)
          .provide(blocking)

      override def getNextObjects(listing: S3ObjectListing): IO[S3Exception, S3ObjectListing] =
        listing.nextContinuationToken match {
          case Some(token) if token.nonEmpty => listObjects(listing.bucketName, "", 100)
          case _                             => IO.fail(S3ExceptionLike(new IllegalArgumentException("Empty token is invalid")))
        }

      override def putObject[R <: zio.Has[_]: Tag](
        bucketName: String,
        key: String,
        contentLength: Long,
        contentType: String,
        content: ZStream[R, Throwable, Byte]
      ): ZIO[R, S3Exception, Unit] =
        ZManaged
          .fromAutoCloseable(Task(new FileOutputStream((path / bucketName / key).toFile)))
          .use(os => content.run(ZSink.fromOutputStream(os)).unit)
          .mapError(S3ExceptionLike)
          .provideSomeLayer[R](ZLayer.succeed(blocking.get))

      override def execute[T](f: S3AsyncClient => CompletableFuture[T]): IO[S3Exception, T] =
        IO.fail(
          S3ExceptionLike(new NotImplementedError("Not implemented error - please don't call execute() S3 Test mode"))
        )

      override def multipartUpload[R <: zio.Has[_]: Tag](
        bucketName: String,
        key: String,
        contentType: String,
        content: ZStream[R, Throwable, Byte]
      ): ZIO[R, S3Exception, Unit] =
        putObject(bucketName, key, 0, contentType, content.chunkN(10))
    }
  }
}
