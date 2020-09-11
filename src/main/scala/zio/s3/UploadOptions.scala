package zio.s3

import software.amazon.awssdk.services.s3.model.{ CreateMultipartUploadRequest, ObjectCannedACL, PutObjectRequest }

import scala.jdk.CollectionConverters._

/**
 * The options of the multipart upload and the put object request.
 * @param metadata the user-defined metadata without the "x-amz-meta-" prefix
 * @param cannedAcl a canned acl, defaults to [[ObjectCannedACL.PRIVATE]]
 * @param contentType the content type of the object (application/json, application/zip, text/plain, ...)
 */
case class UploadOptions(
  metadata: Map[String, String] = Map.empty,
  cannedAcl: ObjectCannedACL = ObjectCannedACL.PRIVATE,
  contentType: Option[String] = None
)

object UploadOptions {
  final val MinMultipartPartSize: Int = 5 * 1024 * 1024 // 5 MB

  final val default = UploadOptions()

  def multipartUploadBuilder(options: UploadOptions): CreateMultipartUploadRequest.Builder = {
    val builder = CreateMultipartUploadRequest
      .builder()
      .metadata(options.metadata.asJava)
      .acl(options.cannedAcl)

    options.contentType.fold(builder)(builder.contentType)
  }

  def putObjectBuilder(options: UploadOptions): PutObjectRequest.Builder = {
    val builder = PutObjectRequest
      .builder()
      .metadata(options.metadata.asJava)
      .acl(options.cannedAcl)

    options.contentType.fold(builder)(builder.contentType)
  }

}
