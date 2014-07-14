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

package au.com.cba.omnia.maestro.core
package tributary

import scalaz.\&/.{This, That, Both}

import com.cba.omnia.edge.hdfs.{Hdfs, Result}

/** Unexpected errors stopping us from processing a file */
case class Failure(msg: String, cause: Option[Throwable])
    extends Throwable(msg, cause.getOrElse(null))

/** Utility functions for `Failure` */
object Failure {
  /** Wrap the error (if any) in a `Result` */
  def wrap[A](msg: String)(result: Result[A]): Result[A] =
    result.fold(a => Result.ok(a), err => Result.error(msg, err match {
      case This(inner)      => Failure(inner, None)
      case That(exn)        => exn
      case Both(inner, exn) => Failure(inner, Some(exn))
    }))

  /* Wrap the error (if any) from a `Hdfs` action */
  def wrapHdfs[A](msg: String)(hdfs: Hdfs[A]): Hdfs[A] =
    EdgeOp.mapResult(wrap[A](msg))(hdfs)
}
