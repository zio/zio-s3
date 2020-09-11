package zio.s3

import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import zio.s3.MultipartUploadOptions.MinMultipartPartSize

import scala.jdk.CollectionConverters._

/**
 * The upload options that are specific to multipart uploads
 *
 * @param uploadOptions [[UploadOptions]]
 * @param partSize the size of the part in bytes, the minimum is 5 MB
 * @param parallelism the number of parallel requests to upload chunks, default to 1
 */
case class MultipartUploadOptions(
  uploadOptions: UploadOptions = UploadOptions.default,
  partSize: Int = MinMultipartPartSize,
  parallelism: Int = 1
)

object MultipartUploadOptions {
  final val MinMultipartPartSize: Int = 5 * 1024 * 1024 // 5 MB

  final val default = MultipartUploadOptions()

  def multipartUploadBuilder(options: MultipartUploadOptions): CreateMultipartUploadRequest.Builder = {
    val builder = CreateMultipartUploadRequest
      .builder()
      .metadata(options.uploadOptions.metadata.asJava)
      .acl(options.uploadOptions.cannedAcl)

    options.uploadOptions.contentType.fold(builder)(builder.contentType)
  }

}
