package zio.s3

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.S3Exception
import zio.s3.S3Bucket.S3BucketListing
import zio.s3.errors.DecodingException
import zio.stream.{ Stream, ZPipeline, ZStream }
import zio.{ IO, ZIO }

import java.nio.charset.CharacterCodingException
import java.util.concurrent.CompletableFuture

/**
 * An S3 service: operations on the buckets and objects of an Amazon S3 storage.
 * Each operation returns a ZIO effect or a stream. The service built by `live`
 * calls the AWS async client. The stub built by `stub` keeps buckets and objects
 * as directories and files on the local filesystem.
 *
 * {{{
 * import software.amazon.awssdk.services.s3.model.S3Exception
 * import zio._
 * import zio.s3._
 * import zio.stream._
 *
 * val program: ZIO[S3, S3Exception, Long] =
 *   for {
 *     _     <- createBucket("reports")
 *     _     <- putObject("reports", "a.txt", 5L, ZStream.fromIterable("hello".getBytes))
 *     lines <- streamLines("reports", "a.txt").runCount
 *   } yield lines
 * }}}
 *
 * @see [[https://zio.dev/zio-s3/ ZIO S3 documentation]]
 */
trait S3 { self =>

  /**
   * Creates a bucket named `bucketName`.
   *
   * @param bucketName name of the bucket
   */
  def createBucket(bucketName: String): IO[S3Exception, Unit]

  /**
   * Deletes a bucket. The effect fails with an `S3Exception` when `Live` finds no
   * bucket with this name. The stub dies in that case instead.
   *
   * @param bucketName name of the bucket
   */
  def deleteBucket(bucketName: String): IO[S3Exception, Unit]

  /**
   * Returns `true` when the bucket exists and `false` when it does not. With `Live`,
   * a missing bucket is reported as `false`; other errors fail with an `S3Exception`.
   *
   * @param bucketName name of the bucket
   */
  def isBucketExists(bucketName: String): IO[S3Exception, Boolean]

  /**
   * Lists the buckets of the storage with their creation dates. The stub lists the
   * directories under its root path.
   */
  val listBuckets: IO[S3Exception, S3BucketListing]

  /**
   * Deletes an object from a bucket. The effect succeeds when the object is missing.
   *
   * @param bucketName name of the bucket
   * @param key object identifier to remove
   */
  def deleteObject(bucketName: String, key: String): IO[S3Exception, Unit]

  /**
   * Streams the bytes of an object. The stream fails with an `S3Exception` when the
   * object is missing.
   *
   * @param bucketName name of the bucket
   * @param key object identifier to read
   */
  def getObject(bucketName: String, key: String): Stream[S3Exception, Byte]

  /**
   * Retrieves metadata from an object without returning the object itself.
   * This operation is useful if you're only interested in an object's metadata.
   * @param bucketName name of the bucket
   * @param key object identifier to read
   * @return the [[ObjectMetadata]]
   */
  def getObjectMetadata(bucketName: String, key: String): IO[S3Exception, ObjectMetadata]

  /**
   * Lists the first page of objects in a bucket, using `ListObjectOptions.default`:
   * no prefix and at most 1000 keys. Use [[paginate]] or [[getNextObjects]] to read
   * the following pages.
   *
   * @param bucketName name of the bucket
   */
  def listObjects(bucketName: String): IO[S3Exception, S3ObjectListing] =
    listObjects(bucketName, ListObjectOptions.default)

  /**
   * Lists one page of objects in a bucket. `options` sets the prefix, the page size,
   * the delimiter and the key to start after. The listing has a continuation token
   * in `nextContinuationToken` when more objects exist.
   *
   * @param bucketName name of the bucket
   * @param options prefix, page size, delimiter and start-after key of the page
   */
  def listObjects(bucketName: String, options: ListObjectOptions): IO[S3Exception, S3ObjectListing]

  /**
   * Fetches the page that follows `listing`, using the continuation token of
   * `listing`. `Live` returns an empty listing without sending a request when
   * `listing` has no token. The stub dies with the message `Empty token is invalid`
   * in that case.
   *
   * @param listing listing to use as a start
   */
  def getNextObjects(listing: S3ObjectListing): IO[S3Exception, S3ObjectListing]

  /**
   * Stores the bytes of `content` as the object `key` in a bucket. An object with the
   * same key is replaced. `Live` sends `contentLength` to AWS; the stub ignores it
   * and ignores `contentMD5`.
   *
   * AWS S3 requires a `Content-MD5` header, or an SDK checksum header, for an upload to
   * a bucket with an Object Lock retention period. Pass the base64 MD5 of the content
   * as `contentMD5`:
   *
   * {{{
   * import software.amazon.awssdk.utils.Md5Utils
   * import scala.util.Random
   *
   * val bytes  = Random.nextString(65536).getBytes()
   * val contentMD5 = Some(Md5Utils.md5AsBase64(bytes))
   * }}}
   *
   * @param bucketName name of the bucket
   * @param key unique object identifier
   * @param contentLength number of bytes in `content`
   * @param content object data
   * @param options metadata, canned ACL and content type of the object
   * @param contentMD5 base64 MD5 of the content, sent as the `Content-MD5` header
   */
  def putObject[R](
    bucketName: String,
    key: String,
    contentLength: Long,
    content: ZStream[R, Throwable, Byte],
    options: UploadOptions = UploadOptions.default,
    contentMD5: Option[String] = None
  ): ZIO[R, S3Exception, Unit]

  /**
   * Stores `content` as one object, sent in parts of `options.partSize` bytes. Up to
   * `parallelism` parts are uploaded at the same time. The last part may be smaller
   * than `partSize`.
   *
   * The effect dies when `parallelism` is not greater than zero, with a message such as
   * `parallelism must be > 0. 0 is invalid`, and when `partSize` is below 5 MiB (5 * 1024
   * * 1024 bytes), with a message such as `Invalid part size 1.0 Mb, minimum size is 5 Mb`.
   * Neither check sends a request.
   *
   * `Live` does not send an AbortMultipartUpload request when the upload fails after it
   * was created, so the parts that were uploaded stay in the bucket. The stub writes
   * `content` as one object and checks only `parallelism` and `partSize`. The S3 upper
   * limits (5 GiB per part, 10,000 parts) are not checked.
   *
   * @param bucketName name of the bucket
   * @param key unique object identifier
   * @param content object data
   * @param options upload options and the part size of the upload
   * @param parallelism number of parts that are uploaded at the same time
   */
  def multipartUpload[R](
    bucketName: String,
    key: String,
    content: ZStream[R, Throwable, Byte],
    options: MultipartUploadOptions = MultipartUploadOptions.default
  )(parallelism: Int): ZIO[R, S3Exception, Unit]

  /**
   * Streams an object as text, one `String` per line. The bytes are decoded as UTF-8.
   * The stream fails with a `DecodingException` when the bytes are not valid UTF-8,
   * and with an `S3Exception` when the object cannot be read.
   *
   * @param bucketName name of the bucket
   * @param key object identifier to read
   */
  def streamLines(bucketName: String, key: String): Stream[S3Exception, String] =
    (self.getObject(bucketName, key) >>> ZPipeline.utf8Decode >>> ZPipeline.splitLines).refineOrDie {
      case ex: S3Exception              => ex
      case ex: CharacterCodingException => DecodingException(ex)
    }

  /**
   * Streams the summaries of the objects in a bucket. The following pages are requested
   * one page ahead of the stream: a page is emitted after the request for the page that
   * follows it returns. The first page uses `ListObjectOptions.default`, with up to 1000
   * keys.
   *
   * @param bucketName name of the bucket
   */
  def listAllObjects(bucketName: String): Stream[S3Exception, S3ObjectSummary] =
    listAllObjects(bucketName, ListObjectOptions.default)

  /**
   * Streams the summaries of the objects in a bucket, starting with the first page that
   * `options` describes. With `Live`, the pages after the first one send only the prefix
   * with the continuation token, so they use the S3 default page size of 1000 keys and no
   * delimiter.
   *
   * @param bucketName name of the bucket
   * @param options prefix, page size, delimiter and start-after key of the first page
   */
  def listAllObjects(bucketName: String, options: ListObjectOptions): Stream[S3Exception, S3ObjectSummary] =
    ZStream
      .fromZIO(self.listObjects(bucketName, options))
      .flatMap(
        paginate(_).mapConcat(_.objectSummaries)
      )

  /**
   * Streams `initialListing` and then each following page. Each page is emitted after the
   * request for the page that follows it returns, and the stream ends after a page
   * without a token.
   *
   * @param initialListing object listing to start with
   */
  def paginate(initialListing: S3ObjectListing): Stream[S3Exception, S3ObjectListing] =
    ZStream.paginateZIO(initialListing) {
      case current @ S3ObjectListing(_, _, _, _, None, _) => ZIO.succeed(current -> None)
      case current                                        => self.getNextObjects(current).map(next => current -> Some(next))
    }

  /**
   * Runs `f` on the AWS async client and returns its result. `Live` fails with the
   * `S3Exception` of the request, or with an `SdkError` for another AWS SDK exception,
   * and dies with any other exception. The stub dies, because it has no AWS client.
   *
   * @param f call any operations on the S3 async client
   * @tparam T value type to return
   */
  def execute[T](f: S3AsyncClient => CompletableFuture[T]): IO[S3Exception, T]
}
