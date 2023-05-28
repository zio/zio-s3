package zio.s3

import software.amazon.awssdk.services.s3.model.ObjectCannedACL

/**
 * The options of the listing object inside a bucket.
 *
 * @param prefix filter all object identifier which start with this `prefix`
 * @param maxKeys max total number of objects, default value is 1000 elements
 * @param delimiter A delimiter is a character you use to group keys, default value is empty
 * @param starAfter Starts listing after this specified key. StartAfter can be any key in the bucket, default value is empty
 */
final case class ListObjectOptions(
  prefix: Option[String],
  maxKeys: Long,
  delimiter: Option[String],
  starAfter: Option[String]
)

object ListObjectOptions {
  val default: ListObjectOptions = ListObjectOptions(None, MaxKeys.Max, None, None)

  def from(prefix: String, maxKeys: Long): ListObjectOptions =
    ListObjectOptions(Option(prefix), maxKeys, None, None)

  def fromMaxKeys(maxKeys: Long): ListObjectOptions =
    ListObjectOptions(None, maxKeys, None, None)

  def fromStartAfter(startAfter: String): ListObjectOptions =
    ListObjectOptions(None, MaxKeys.Max, None, Option(startAfter))
}

object MaxKeys {
  val Max: Long = 1000L
}

/**
 * The options of the multipart upload and the put object request.
 *
 * @param metadata the user-defined metadata without the "x-amz-meta-" prefix
 * @param cannedAcl a canned acl, defaults to "private"
 * @param contentType the content type of the object (application/json, application/zip, text/plain, ...)
 */
final case class UploadOptions(
  metadata: Map[String, String],
  cannedAcl: ObjectCannedACL,
  contentType: Option[String]
)

object UploadOptions {
  val default: UploadOptions = UploadOptions(Map.empty, ObjectCannedACL.PRIVATE, None)

  def from(metadata: Map[String, String], contentType: String): UploadOptions =
    UploadOptions(metadata, ObjectCannedACL.PRIVATE, Option(contentType))

  def fromContentType(contentType: String): UploadOptions =
    UploadOptions(Map.empty, ObjectCannedACL.PRIVATE, Option(contentType))

  def fromMetadata(metadata: Map[String, String]): UploadOptions =
    UploadOptions(metadata, ObjectCannedACL.PRIVATE, None)
}

/**
 * The upload options that are specific to multipart uploads
 *
 * @param uploadOptions [[UploadOptions]]
 * @param partSize the size of the part in bytes, the minimum is 5 MB
 */
final case class MultipartUploadOptions(
  uploadOptions: UploadOptions,
  partSize: Int
)

object MultipartUploadOptions {
  val default: MultipartUploadOptions = MultipartUploadOptions(UploadOptions.default, PartSize.Min)

  def fromUploadOptions(options: UploadOptions): MultipartUploadOptions =
    MultipartUploadOptions(options, PartSize.Min)

  def fromPartSize(partSize: Int): MultipartUploadOptions =
    MultipartUploadOptions(UploadOptions.default, partSize)
}

object PartSize {
  final val Kilo: Int = 1024
  final val Mega: Int = 1024 * Kilo
  final val Giga: Int = 1024 * Mega

  //part size limit is 5Mb, required by amazon api
  final val Min: Int = 5 * Mega
}
