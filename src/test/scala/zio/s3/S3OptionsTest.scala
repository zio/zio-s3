package zio.s3

import zio.test.Assertion._
import zio.test._

object S3OptionsTest extends DefaultRunnableSpec {

  def spec =
    suite("option")(
      testM("invalid partSize") {
        for {
          failure <- PartSize
                       .from(1024)
                       .foldCause(_.failureOption.map(_.message).mkString, _ => "")
        } yield assert(failure)(equalTo(s"Invalid part size 1.0 Kb, minimum size is 5 Mb"))
      },
      testM("valid partSize") {
        for {
          success <- PartSize.from(10 * PartSize.Mega.toInt)
        } yield assert(success.size)(equalTo(10 * PartSize.Mega))
      }
    )
}
