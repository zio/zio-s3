package zio.s3

import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import zio.s3.MultipartUploadOptions.MinPartSize

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
 * @param parallelism the number of parallel requests to upload chunks, default to 1
 */
case class MultipartUploadOptions(
  uploadOptions: UploadOptions = UploadOptions(),
  partSize: Int = MinPartSize,
  parallelism: Int = 1
)

object MultipartUploadOptions {
  final val MinPartSize: Int = 5 * 1024 * 1024 // 5 MB
}
