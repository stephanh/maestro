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
package upload

import scalaz.\&/.{This, That, Both}

import com.cba.omnia.edge.hdfs.{Hdfs, Result}

/**
  * Operations on Hdfs and Result that belong on edge.
  *
  * TODO move these operations to the appropriate place in edge.
  */
object EdgeOp {

  /** Returns an exception Result if a predicate is false. */
  def failIf(b: Boolean)(msg: String): Result[Unit] =
    if (b) Result.fail(msg) else Result.ok(())

  /** Fails the Hdfs computation if a predicate is false. */
  def failHdfsIf(b: Boolean)(msg: String): Hdfs[Unit] =
    Hdfs.result(failIf(b)(msg))

  /**
    * Runs the second action if the first one failes.
    *
    * If the first action fails, only the results of the second
    * acion are returned.
    */
  def or[A](hdfs1: Hdfs[A], hdfs2: Hdfs[A]) =
    Hdfs(conf => hdfs1.run(conf).fold(a => Result.ok(a), _ => hdfs2.run(conf)))

  /**
    * Maps over the result of a Hdfs action, allowing you to
    * work with the errors as well as the success value.
    */
  def mapResult[A,B](f: Result[A] => Result[B])(hdfs: Hdfs[A]) =
    Hdfs(hdfs.run andThen f)

  /**
    * Wrap the error (if any) in a `Result`
    *
    * In order to compose errors, I have to view the Result error type as
    * representing a list of errors. This function treats the error's string
    * field, if it exists, as the first item in the list, followed by the
    * error's exception field, followed by the error's exception cause chain.
    */
  def wrapErr[A](msg: String)(result: Result[A]): Result[A] =
    result.fold(a => Result.ok(a), err => Result.error(msg, err match {
      case This(inner)      => new Throwable(inner)
      case That(exn)        => exn
      case Both(inner, exn) => new Throwable(inner, exn)
    }))

  /** Wrap the error (if any) from a `Hdfs` action
    *
    * In order to compose errors, I have to view the Result error type as
    * representing a list of errors. This function treats the error's string
    * field, if it exists, as the first item in the list, followed by the
    * error's exception field, followed by the error's exception cause chain.
    */
  def wrapHdfsErr[A](msg: String)(hdfs: Hdfs[A]): Hdfs[A] =
    EdgeOp.mapResult(wrapErr[A](msg))(hdfs)
}
