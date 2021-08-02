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

import java.io.FileInputStream
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.util.UUID
import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.S3Exception
import zio._
import zio.blocking.Blocking
import zio.nio.channels.FileChannel
import zio.nio.core.file.{ Path => ZPath }
import zio.nio.file.Files
import zio.s3.S3Bucket._
import zio.stream.{ Stream, ZStream }
import java.io.FileNotFoundException

/**
 * Stub Service which is back by a filesystem storage
 */
object Test {

  private def fileNotFound(err: FileNotFoundException): S3Exception =
    S3Exception
      .builder()
      .message("Key does not exist.")
      .cause(err)
      .statusCode(404)
      .build()
      .asInstanceOf[S3Exception]

  def connect(path: ZPath): Blocking => S3.Service = { blocking =>
    type ContentType = String
    type Metadata    = Map[String, String]

    new S3.Service {
      private val refDb: Ref[Map[String, (ContentType, Metadata)]] =
        Ref.unsafeMake(Map.empty[String, (ContentType, Metadata)])

      override def createBucket(bucketName: String): IO[S3Exception, Unit] =
        Files.createDirectory(path / bucketName).orDie.provide(blocking)

      override def deleteBucket(bucketName: String): IO[S3Exception, Unit] =
        Files.delete(path / bucketName).orDie.provide(blocking)

      override def isBucketExists(bucketName: String): IO[S3Exception, Boolean] =
        Files.exists(path / bucketName).provide(blocking)

      override val listBuckets: IO[S3Exception, S3BucketListing] =
        Files
          .list(path)
          .filterM(p => Files.readAttributes[BasicFileAttributes](p).map(_.isDirectory))
          .mapM { p =>
            Files
              .readAttributes[BasicFileAttributes](p)
              .map(attr => S3Bucket(p.filename.toString, attr.creationTime().toInstant))
          }
          .runCollect
          .orDie
          .provide(blocking)

      override def deleteObject(bucketName: String, key: String): IO[S3Exception, Unit] =
        Files.deleteIfExists(path / bucketName / key).orDie.provide(blocking).unit

      override def getObject(bucketName: String, key: String): Stream[S3Exception, Byte] =
        ZStream
          .managed(ZManaged.fromAutoCloseable(Task(new FileInputStream((path / bucketName / key).toFile))))
          .flatMap(ZStream.fromInputStream(_, 2048))
          .refineOrDie {
            case e: FileNotFoundException => fileNotFound(e)
          }
          .provide(blocking)

      override def getObjectMetadata(bucketName: String, key: String): IO[S3Exception, ObjectMetadata] =
        (for {
          (contentType, metadata) <- refDb.get.map(_.getOrElse(bucketName + key, "" -> Map.empty[String, String]))

          file <- Files
                    .readAttributes[BasicFileAttributes](path / bucketName / key)
                    .map(p => ObjectMetadata(metadata, contentType, p.size()))
                    .provide(blocking)
        } yield file).orDie

      override def listObjects(
        bucketName: String,
        options: ListObjectOptions
      ): IO[S3Exception, S3ObjectListing] =
        Files
          .find(path / bucketName) {
            case (p, _) if options.delimiter.nonEmpty =>
              options.prefix.fold(true)((path / bucketName).relativize(p).toString().startsWith)
            case (p, _)                               =>
              options.prefix.fold(true)(p.filename.toString().startsWith)
          }
          .mapM(p => Files.readAttributes[BasicFileAttributes](p).map(a => a -> p))
          .filter { case (attr, _) => attr.isRegularFile }
          .map {
            case (attr, f) =>
              S3ObjectSummary(
                bucketName,
                (path / bucketName).relativize(f).toString(),
                attr.lastModifiedTime().toInstant,
                attr.size()
              )
          }
          .runCollect
          .map(
            _.sortBy(_.key)
              .mapAccum(options.starAfter) {
                case (Some(startWith), o) =>
                  if (startWith.startsWith(o.key))
                    None            -> Chunk.empty
                  else
                    Some(startWith) -> Chunk.empty
                case (_, o)               =>
                  None -> Chunk(o)
              }
              ._2
              .flatten
          )
          .map {
            case list if list.size > options.maxKeys =>
              S3ObjectListing(
                bucketName,
                options.delimiter,
                options.starAfter,
                list.take(options.maxKeys.toInt),
                Some(UUID.randomUUID().toString),
                None
              )
            case list                                =>
              S3ObjectListing(bucketName, options.delimiter, options.starAfter, list, None, None)
          }
          .orDie
          .provide(blocking)

      override def getNextObjects(listing: S3ObjectListing): IO[S3Exception, S3ObjectListing] =
        listing.nextContinuationToken match {
          case Some(token) if token.nonEmpty => listObjects(listing.bucketName, ListObjectOptions.fromMaxKeys(100))
          case _                             => ZIO.dieMessage("Empty token is invalid")
        }

      override def putObject[R](
        bucketName: String,
        key: String,
        contentLength: Long,
        content: ZStream[R, Throwable, Byte],
        options: UploadOptions
      ): ZIO[R, S3Exception, Unit] =
        (for {
          _       <-
            refDb.update(db =>
              db + (bucketName + key -> (options.contentType.getOrElse("application/octet-stream") -> options.metadata))
            )
          filePath = path / bucketName / key
          _       <- filePath.parent
                       .map(parentPath => Files.createDirectories(parentPath).provide(blocking))
                       .getOrElse(ZIO.unit)
          _       <- FileChannel
                       .open(filePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)
                       .provide(blocking)
                       .use(channel => content.foreachChunk(channel.writeChunk))
        } yield ()).orDie

      override def execute[T](f: S3AsyncClient => CompletableFuture[T]): IO[S3Exception, T] =
        IO.dieMessage("Not implemented error - please don't call execute() S3 Test mode")

      override def multipartUpload[R](
        bucketName: String,
        key: String,
        content: ZStream[R, Throwable, Byte],
        options: MultipartUploadOptions
      )(parallelism: Int): ZIO[R, S3Exception, Unit] = {
        val _contentType = options.uploadOptions.contentType.orElse(Some("binary/octet-stream"))

        for {
          _ <- ZIO.dieMessage(s"parallelism must be > 0. $parallelism is invalid").unless(parallelism > 0)
          _ <-
            ZIO
              .dieMessage(
                s"Invalid part size ${Math.floor(options.partSize.toDouble / PartSize.Mega.toDouble * 100d) / 100d} Mb, minimum size is ${PartSize.Min / PartSize.Mega} Mb"
              )
              .unless(options.partSize >= PartSize.Min)
          _ <- putObject(
                 bucketName,
                 key,
                 0,
                 content.chunkN(options.partSize),
                 options.uploadOptions.copy(contentType = _contentType)
               )
        } yield ()
      }
    }
  }
}
