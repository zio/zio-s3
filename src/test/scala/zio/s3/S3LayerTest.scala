package zio.s3

import java.net.URI
import software.amazon.awssdk.regions.Region
import zio.test.Assertion._
import zio.test._

object S3LayerSpec extends DefaultRunnableSpec {
  override def spec =
    suite("S3LayerSpec")(
      testM("using ZManaged[R, E, A] in liveM compiles") {
        assertM(typeCheck("""liveM(Region.CA_CENTRAL_1, CredentialsProviders.default, Some(URI.create("http://localhost:9000")))"""))(isRight)
      }
    )
}