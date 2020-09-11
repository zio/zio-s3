package zio.s3

import software.amazon.awssdk.services.s3.model.{ ObjectCannedACL, PutObjectRequest }

import scala.jdk.CollectionConverters._

/**
 * The options of the multipart upload and the put object request.
 * @param metadata the user-defined metadata without the "x-amz-meta-" prefix
 * @param cannedAcl a canned acl, defaults to "private"
 * @param contentType the content type of the object (application/json, application/zip, text/plain, ...)
 */
case class UploadOptions(
  metadata: Map[String, String] = Map.empty,
  cannedAcl: ObjectCannedACL = ObjectCannedACL.PRIVATE,
  contentType: Option[String] = None
)

object UploadOptions {
  final val default = UploadOptions()

  def putObjectBuilder(options: UploadOptions): PutObjectRequest.Builder = {
    val builder = PutObjectRequest
      .builder()
      .metadata(options.metadata.asJava)
      .acl(options.cannedAcl)

    options.contentType.fold(builder)(builder.contentType)
  }

}
