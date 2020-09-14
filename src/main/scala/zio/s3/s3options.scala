package zio.s3

import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import zio.IO

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
  partSize: PartSize = PartSize.Min
)

sealed trait PartSize { self =>
  val size: Int

  def + (other: Int): PartSize = PartSize(self.size + other)

  def * (n: Int): PartSize = PartSize(self.size * n)
}

object PartSize {
  final val Kilo: Int = 1024
  final val Mega: Int = 1024 * Kilo
  final val Giga: Int = 1024 * Mega

  //part size limit is 5Mb, required by amazon api
  final val Min: PartSize = PartSize(5 * Mega)

  private[s3] def apply(v: Int): PartSize =
    new PartSize {
      val size: Int = v
    }

  /**
   * Create a Part Size, minimum value is 5 Mb
   *
   * @param partSize size in bytes for the default Multipart Upload
   */
  def from(partSize: Int): IO[InvalidPartSize, PartSize] =
    partSize match {
      case s if s >= Min.size =>
        IO.succeed(new PartSize {
          val size: Int = s
        })
      case invalid            =>
        val invalidSize =
          if (partSize >= Mega) s"${Math.floor(invalid.toDouble / Mega * 100) / 100} Mb"
          else s"${Math.floor(invalid.toDouble / Kilo * 100) / 100} Kb"

        IO.fail(
          InvalidPartSize(s"Invalid part size $invalidSize, minimum size is ${Min.size / Mega} Mb", invalid)
        )
    }
}
