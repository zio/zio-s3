/*
 * Copyright 2017-2020 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.s3

import software.amazon.awssdk.regions.Region
import zio.{ IO, ZIO }

final case class S3Credentials(accessKeyId: String, secretAccessKey: String)

sealed trait S3Region {
  val region: Region
}

object S3Region {

  def fromRegion(region: Region): Either[InvalidSettings, S3Region] =
    region match {
      case r if Region.regions().contains(r) => Right(new S3Region { val region = r })
      case r                                 => Left(InvalidSettings(s"Invalid aws region provided : ${r.id}"))
    }

  def fromString(value: String): Either[InvalidSettings, S3Region] = fromRegion(Region.of(value))
}

final case class S3Settings(s3Region: S3Region, credentials: S3Credentials)

object S3Settings {

  def from(region: String, credentials: S3Credentials): IO[InvalidSettings, S3Settings] =
    ZIO.fromEither(S3Region.fromString(region)).map(S3Settings(_, credentials))
}
