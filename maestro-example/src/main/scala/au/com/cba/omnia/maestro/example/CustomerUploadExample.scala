//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.maestro.example

import com.twitter.scalding.Args

import scalaz._, Scalaz._

import org.apache.hadoop.conf.Configuration

import au.com.cba.omnia.maestro.api.Maestro
import au.com.cba.omnia.maestro.example.thrift.Customer

import com.cba.omnia.edge.hdfs.{Error, Ok, Result}

class CustomerUploadExample(args: Args) extends Maestro[Customer](args) {
  val hdfsRoot    = args("hdfsRoot")
  val sourceRoot  = args("sourceRoot")
  val archiveRoot = args("archivedir")
  val conf        = new Configuration
  val domain      = "customer"
  val timeFormat  = "yyyyMMddHHmm"

  val byDateResult = Maestro.upload(conf, domain, "by_date", timeFormat, sourceRoot, archiveRoot, hdfsRoot)
  val byIdResult   = Maestro.upload(conf, domain, "by_id", timeFormat, sourceRoot, archiveRoot, hdfsRoot)

  List(byDateResult, byIdResult).sequence_ match {
    case Error(_) => {
      // do not post-process files when there was an error copying them
      // report the error here
      ()
    }
    case Ok(()) => {
      // continue with post-processing here
      ()
    }
  }
}
