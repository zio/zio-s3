package zio.s3

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import zio.s3.providers._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ UIO, ZIO }

object S3ProvidersTest extends DefaultRunnableSpec {

  def setProps(props: (String, String)*): UIO[Unit] =
    UIO {
      props.foreach {
        case (k, v) =>
          System.setProperty(k, v)
      }
    }

  def unsetProps(keys: String*): UIO[Unit] =
    UIO {
      keys.foreach(System.clearProperty)
    }

  def spec =
    suite("Providers")(
      testM("cred with const") {
        assertM(const("k", "v").useNow.map(_.resolveCredentials()))(
          equalTo(AwsBasicCredentials.create("k", "v"))
        )
      },
      testM("cred with default fallback const") {
        assertM(
          (env <> const("k", "v")).useNow.map(_.resolveCredentials())
        )(equalTo(AwsBasicCredentials.create("k", "v")))
      },
      testM("cred in system properties") {
        for {
          cred <- system.use(p => ZIO(p.resolveCredentials()))
        } yield assert(cred)(equalTo(AwsBasicCredentials.create("k1", "s1")))
      } @@ flaky @@ around_(
        setProps(("aws.accessKeyId", "k1"), ("aws.secretAccessKey", "s1")),
        unsetProps("aws.accessKeyId", "aws.secretAccessKey")
      ),
      testM("no cred in system properties") {
        for {
          failure <- system.useNow.flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      } @@ around_(
        unsetProps("aws.accessKeyId", "aws.secretAccessKey"),
        UIO.unit
      ),
      testM("no cred in environment properties") {
        for {
          failure <- env.useNow.flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      testM("no cred in profile") {
        for {
          failure <- profile.useNow.flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      testM("no cred in container") {
        for {
          failure <- container.useNow.flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      testM("no cred in instance profile credentials") {
        for {
          failure <- instanceProfile.useNow.flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      testM("no cred in webidentity credentials") {
        for {
          failure <- webIdentity.useNow.flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      testM("settings from invalid creds") {
        for {
          failure <- settings(
                       Region.AF_SOUTH_1,
                       system.useNow.map(_.resolveCredentials())
                     ).build.useNow.flip
        } yield assert(failure.getMessage)(isNonEmptyString)
      },
      testM("no cred when chain all providers") {
        for {
          failure <- default.use(c => ZIO.effect(c.resolveCredentials())).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      }
    ) @@ sequential
}
