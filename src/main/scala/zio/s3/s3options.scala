package zio.s3

import software.amazon.awssdk.services.s3.model.ObjectCannedACL

/**
 * The options of the multipart upload and the put object request.
 *
 * @param metadata the user-defined metadata without the "x-amz-meta-" prefix
 * @param cannedAcl a canned acl, defaults to "private"
 * @param contentType the content type of the object (application/json, application/zip, text/plain, ...)
 */
case class UploadOptions(
  metadata: Map[String, String] = Map.empty,
  cannedAcl: ObjectCannedACL = ObjectCannedACL.PRIVATE,
  contentType: Option[String] = None
)

/**
 * The upload options that are specific to multipart uploads
 *
 * @param uploadOptions [[UploadOptions]]
 * @param partSize the size of the part in bytes, the minimum is 5 MB
 */
case class MultipartUploadOptions(
  uploadOptions: UploadOptions = UploadOptions(),
  partSize: Int = PartSize.Min
)

object PartSize {
  final val Kilo: Int = 1024
  final val Mega: Int = 1024 * Kilo
  final val Giga: Int = 1024 * Mega

  //part size limit is 5Mb, required by amazon api
  final val Min: Int = 5 * Mega
}
