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

final private[s3] case class S3Settings(region: Region, credentials: S3Credentials)

object S3Settings {

  def from(region: Region, credentials: S3Credentials): IO[InvalidSettings, S3Settings] =
    for {
      region <- ZIO
                 .succeed(region)
                 .filterOrFail(Region.regions().contains(_))(
                   InvalidSettings(s"Invalid aws region provided : ${region.toString}")
                 )
    } yield S3Settings(region, credentials)
}
