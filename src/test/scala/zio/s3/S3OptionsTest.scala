package zio.s3

import zio.s3.PartSize.Mega
import zio.test.Assertion._
import zio.test.{ test, testM, _ }

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
          success <- PartSize.from(10 * Mega.toInt)
        } yield assert(success.size)(equalTo(10 * Mega))
      },
      test("add partSize - 7Mb") {
        assert((PartSize.Min + (2 * Mega)).size)(equalTo(7 * Mega))
      },
      test("multiply partSize - 10Mb") {
        assert((PartSize.Min * 2).size)(equalTo(10 * Mega))
      }
    )
}
