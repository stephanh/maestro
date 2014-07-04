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

package au.com.cba.omnia.maestro.schema
package commands

import com.quantifind.sumac.ArgMain
import com.quantifind.sumac.validation._
import com.quantifind.sumac.FieldArgs

import scala.io.Source


/** Arguments for `Squash` command .*/
class SquashArgs extends FieldArgs {
  @Required
  var histogram: String = _
}


// Squash a histogram.
//  For example, if we have Real:100 and Digits:100 then we only need
//  to keep Digits:100, because all strings of Digits are also Reals.
object Squash extends ArgMain[SquashArgs]
{
  def main(args: SquashArgs): Unit = {

    // Read the histogram file
    val strHistogram =
      Source.fromFile(args.histogram)
        .getLines
        .map { _ + "\n" }
        .reduceLeft (_+_)

      println(strHistogram)
  }
}
