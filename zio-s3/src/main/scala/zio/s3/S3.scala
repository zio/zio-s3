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
 * The `S3` module provides access to a s3 amazon storage.
 * All operations are async since we are relying on the amazon async client
 */
trait S3 { self =>

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
   * Retrieves metadata from an object without returning the object itself.
   * This operation is useful if you're only interested in an object's metadata.
   * @param bucketName name of the bucket
   * @param key object identifier to read
   * @return the [[ObjectMetadata]]
   */
  def getObjectMetadata(bucketName: String, key: String): IO[S3Exception, ObjectMetadata]

  /**
   * list all object for a specific bucket
   *
   * @param bucketName name of the bucket
   */
  def listObjects(bucketName: String): IO[S3Exception, S3ObjectListing] =
    listObjects(bucketName, ListObjectOptions.default)

  def listObjects(bucketName: String, options: ListObjectOptions): IO[S3Exception, S3ObjectListing]

  /**
   * Fetch the next object listing from a specific object listing.
   *
   * @param listing listing to use as a start
   */
  def getNextObjects(listing: S3ObjectListing): IO[S3Exception, S3ObjectListing]

  /**
   * Store data object into a specific bucket
   *
   * ==Example of creating a contentMD5 option==
   *
   * The md5 option is required when the target bucket is configured with object locking, otherwise
   * the AWS S3 API will not accept the [[putObject]] request.
   *
   * {{{
   *  import software.amazon.awssdk.utils.Md5Utils
   *  import scala.util.Random
   *
   *  val bytes  = Random.nextString(65536).getBytes()
   *  val contentMD5 = Some(Md5Utils.md5AsBase64(bytes))
   * }}}
   *
   * @param bucketName name of the bucket
   * @param key unique object identifier
   * @param contentLength length of the data in bytes
   * @param content object data
   * @param contentMD5 [[Option]] of [[String]] containing the MD5 hash of the content encoded as base64
   * @return
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
   * *
   *
   * Store data object into a specific bucket, minimum size of the data is 5 Mb to use multipart upload (restriction from amazon API)
   *
   * @param bucketName name of the bucket
   * @param key unique object identifier
   * @param content object data
   * @param options the optional configurations of the multipart upload
   * @param parallelism the number of parallel requests to upload chunks
   */
  def multipartUpload[R](
    bucketName: String,
    key: String,
    content: ZStream[R, Throwable, Byte],
    options: MultipartUploadOptions = MultipartUploadOptions.default
  )(parallelism: Int): ZIO[R, S3Exception, Unit]

  /**
   * Read an object by lines
   *
   * @param bucketName name of the bucket
   * @param key: unique key of the object
   */
  def streamLines(bucketName: String, key: String): Stream[S3Exception, String] =
    (self.getObject(bucketName, key) >>> ZPipeline.utf8Decode >>> ZPipeline.splitLines).refineOrDie {
      case ex: S3Exception              => ex
      case ex: CharacterCodingException => DecodingException(ex)
    }

  /**
   * List all descendant objects of a bucket
   * Fetch all objects recursively of all nested directory by traversing all of them
   *
   * @param bucketName name of the bucket
   *
   * MaxKeys have a default value to 1000 elements
   */
  def listAllObjects(bucketName: String): Stream[S3Exception, S3ObjectSummary] =
    listAllObjects(bucketName, ListObjectOptions.default)

  def listAllObjects(bucketName: String, options: ListObjectOptions): Stream[S3Exception, S3ObjectSummary] =
    ZStream
      .fromZIO(self.listObjects(bucketName, options))
      .flatMap(
        paginate(_).mapConcat(_.objectSummaries)
      )

  /**
   * List all objects by traversing all nested directories
   *
   * @param initialListing object listing to start with
   * @return
   */
  def paginate(initialListing: S3ObjectListing): Stream[S3Exception, S3ObjectListing] =
    ZStream.paginateZIO(initialListing) {
      case current @ S3ObjectListing(_, _, _, _, None, _) => ZIO.succeed(current -> None)
      case current                                        => self.getNextObjects(current).map(next => current -> Some(next))
    }

  /**
   * *
   * expose safely amazon s3 async client
   *
   * @param f call any operations on s3 async client
   * @tparam T value type to return
   */
  def execute[T](f: S3AsyncClient => CompletableFuture[T]): IO[S3Exception, T]
}
