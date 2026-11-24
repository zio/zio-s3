package zio.s3

import software.amazon.awssdk.services.s3.model.ObjectCannedACL

/**
 * The options of a listing of the objects in a bucket.
 *
 * @param prefix only the objects whose key starts with `prefix` are listed
 * @param maxKeys number of keys in the first page. `ListObjectOptions.default` uses 1000, which is also the most that S3 returns in one page. `getNextObjects` does not send it, so the following pages use the S3 default
 * @param delimiter character that groups keys. On the first page, keys that contain it after the prefix are left out of the object summaries
 * @param starAfter the listing starts after this key, in key order
 */
final case class ListObjectOptions(
  prefix: Option[String],
  maxKeys: Long,
  delimiter: Option[String],
  starAfter: Option[String]
)

/**
 * Constructors of `ListObjectOptions`.
 */
object ListObjectOptions {

  /**
   * Options with no prefix, no delimiter, no start-after key and a page size of 1000.
   */
  val default: ListObjectOptions = ListObjectOptions(None, MaxKeys.Max, None, None)

  /**
   * Options that list the keys which start with `prefix`, with at most `maxKeys` keys in
   * the first page.
   *
   * @param prefix prefix of the keys to list
   * @param maxKeys number of keys in the first page
   */
  def from(prefix: String, maxKeys: Long): ListObjectOptions =
    ListObjectOptions(Option(prefix), maxKeys, None, None)

  /**
   * Options with no prefix and at most `maxKeys` keys in the first page.
   *
   * @param maxKeys number of keys in the first page
   */
  def fromMaxKeys(maxKeys: Long): ListObjectOptions =
    ListObjectOptions(None, maxKeys, None, None)

  /**
   * Options that start the listing after `startAfter`, with 1000 keys per page.
   *
   * @param startAfter key to start the listing after
   */
  def fromStartAfter(startAfter: String): ListObjectOptions =
    ListObjectOptions(None, MaxKeys.Max, None, Option(startAfter))
}

/**
 * Page sizes of object listings.
 */
object MaxKeys {

  /**
   * The default page size of an object listing: 1000 keys.
   */
  val Max: Long = 1000L
}

/**
 * The options of an object upload.
 *
 * @param metadata the user-defined metadata without the x-amz-meta- prefix
 * @param cannedAcl a canned ACL, `ObjectCannedACL.PRIVATE` by default
 * @param contentType the content type of the object (application/json, application/zip, text/plain, ...). When `None`, zio-s3 sets no content type and the AWS SDK sends its default: `application/octet-stream` for `putObject` and `binary/octet-stream` for `multipartUpload`
 */
final case class UploadOptions(
  metadata: Map[String, String],
  cannedAcl: ObjectCannedACL,
  contentType: Option[String]
)

/**
 * Constructors of `UploadOptions`.
 */
object UploadOptions {

  /**
   * Options with no metadata, the private canned ACL and no content type.
   */
  val default: UploadOptions = UploadOptions(Map.empty, ObjectCannedACL.PRIVATE, None)

  /**
   * Options with `metadata`, the private canned ACL and `contentType`.
   *
   * @param metadata the user-defined metadata
   * @param contentType the content type of the object
   */
  def from(metadata: Map[String, String], contentType: String): UploadOptions =
    UploadOptions(metadata, ObjectCannedACL.PRIVATE, Option(contentType))

  /**
   * Options with `contentType`, no metadata and the private canned ACL.
   *
   * @param contentType the content type of the object
   */
  def fromContentType(contentType: String): UploadOptions =
    UploadOptions(Map.empty, ObjectCannedACL.PRIVATE, Option(contentType))

  /**
   * Options with `metadata`, the private canned ACL and no content type.
   *
   * @param metadata the user-defined metadata
   */
  def fromMetadata(metadata: Map[String, String]): UploadOptions =
    UploadOptions(metadata, ObjectCannedACL.PRIVATE, None)
}

/**
 * The options of a multipart upload.
 *
 * @param uploadOptions the options of the object, see [[UploadOptions]]
 * @param partSize the size of each part in bytes, at least 5 MiB (5 * 1024 * 1024 bytes). A smaller value makes the upload die before it starts
 */
final case class MultipartUploadOptions(
  uploadOptions: UploadOptions,
  partSize: Int
)

/**
 * Constructors of `MultipartUploadOptions`.
 */
object MultipartUploadOptions {

  /**
   * Options with the default upload options and parts of 5 MiB.
   */
  val default: MultipartUploadOptions = MultipartUploadOptions(UploadOptions.default, PartSize.Min)

  /**
   * Options with `options` and parts of 5 MiB.
   *
   * @param options the options of the object
   */
  def fromUploadOptions(options: UploadOptions): MultipartUploadOptions =
    MultipartUploadOptions(options, PartSize.Min)

  /**
   * Options with the default upload options and parts of `partSize` bytes.
   *
   * @param partSize size of each part in bytes
   */
  def fromPartSize(partSize: Int): MultipartUploadOptions =
    MultipartUploadOptions(UploadOptions.default, partSize)
}

/**
 * Sizes in bytes, for the part size of multipart uploads.
 */
object PartSize {

  /**
   * 1024 bytes.
   */
  final val Kilo: Int = 1024

  /**
   * 1024 * 1024 bytes.
   */
  final val Mega: Int = 1024 * Kilo

  /**
   * 1024 * 1024 * 1024 bytes.
   */
  final val Giga: Int = 1024 * Mega

  /**
   * The smallest part size of a multipart upload: 5 MiB (5 * 1024 * 1024 bytes).
   */
  final val Min: Int = 5 * Mega
}
