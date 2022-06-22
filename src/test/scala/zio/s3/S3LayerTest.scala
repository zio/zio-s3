package zio.s3

import java.net.URI
import software.amazon.awssdk.regions.Region
import zio.test.Assertion._
import zio.test._

object S3LayerTest extends ZIOSpecDefault {

  override def spec: Spec[Any, Nothing] =
    suite("S3LayerSpec")(
      test("using ZIO[R with Scope, E, A] in liveZIO compiles") {
        typeCheck(
          """liveZIO(Region.CA_CENTRAL_1, providers.default, Some(URI.create("http://localhost:9000")))"""
        ).map(assert(_)(isRight))
      }
    )
}
