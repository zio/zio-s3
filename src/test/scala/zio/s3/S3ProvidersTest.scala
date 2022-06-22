package zio.s3

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import zio.s3.providers._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ Scope, UIO, ZIO }

object S3ProvidersTest extends ZIOSpecDefault {

  def setProps(props: (String, String)*): UIO[Unit] =
    ZIO.succeed {
      props.foreach {
        case (k, v) =>
          System.setProperty(k, v)
      }
    }

  def unsetProps(keys: String*): UIO[Unit] =
    ZIO.succeed {
      keys.foreach(System.clearProperty)
    }

  def spec: Spec[TestEnvironment with Scope, Any] =
    suite("Providers")(
      test("cred with const") {
        ZIO
          .scoped(const("k", "v").map(_.resolveCredentials()))
          .map(res => assertTrue(res == AwsBasicCredentials.create("k", "v")))
      },
      test("cred with default fallback const") {
        ZIO
          .scoped((env <> const("k", "v")).map(_.resolveCredentials()))
          .map(res => assertTrue(res == AwsBasicCredentials.create("k", "v")))
      },
      test("cred in system properties") {
        for {
          cred <- ZIO.scoped(system.flatMap(p => ZIO.attempt(p.resolveCredentials())))
        } yield assertTrue(cred == AwsBasicCredentials.create("k1", "s1"))
      } @@ flaky @@ around(
        setProps(("aws.accessKeyId", "k1"), ("aws.secretAccessKey", "s1")),
        unsetProps("aws.accessKeyId", "aws.secretAccessKey")
      ),
      test("no cred in system properties") {
        for {
          failure <- ZIO.scoped(system).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      } @@ around(
        unsetProps("aws.accessKeyId", "aws.secretAccessKey"),
        ZIO.unit
      ),
      test("no cred in environment properties") {
        for {
          failure <- ZIO.scoped(env).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("no cred in profile") {
        for {
          failure <- ZIO.scoped(profile).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("no cred in named profile") {
        for {
          failure <- ZIO.scoped(profile(Some("name"))).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("no cred in container") {
        for {
          failure <- ZIO.scoped(container).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("no cred in instance profile credentials") {
        for {
          failure <- ZIO.scoped(instanceProfile).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("no cred in webidentity credentials") {
        for {
          failure <- ZIO.scoped(webIdentity).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      },
      test("settings from invalid creds") {
        for {
          failure <- ZIO
                       .scoped(
                         settings(
                           Region.AF_SOUTH_1,
                           ZIO.scoped(system).map(_.resolveCredentials())
                         ).build
                       )
                       .flip
        } yield assert(failure.getMessage)(isNonEmptyString)
      },
      test("no cred when chain all providers") {
        for {
          failure <- ZIO.scoped(default.flatMap(c => ZIO.attempt(c.resolveCredentials()))).flip.map(_.getMessage)
        } yield assert(failure)(isNonEmptyString)
      }
    ) @@ sequential
}
