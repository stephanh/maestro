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
package codec

import au.com.cba.omnia.maestro.core.data._
import scalaz._, Scalaz._
import org.apache.thrift.protocol.TField

case class Describe[A](name: String, metadata: List[(String, TField)])

object Describe {
  def describe[A: Describe]: List[(String, TField)] =
    Describe.of[A].metadata

  def of[A: Describe]: Describe[A] =
    implicitly[Describe[A]]
}
