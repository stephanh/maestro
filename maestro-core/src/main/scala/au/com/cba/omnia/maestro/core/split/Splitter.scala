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
package split

import scala.collection.JavaConverters._

import com.google.common.base.{Splitter => GoogSplitter}

/**
  * Represents the notion of splitting a string into parts
  *
  * There is no requirement that `splitter.run(s).mkString == s`.
  * However, it should be theoretically possible to write a function
  * `List[String] => String` that reverses the split on good input.
  *
  * Originally the idea was that Splitter would return `String \/ List[String]`.
  * However, the code I wrote in [[au.com.cba.omnia.maestro.core.task.Load]]
  * to handle this caused scalding serialization to stop working, so now the
  * splitters try to do "sensible" things if they see something incorrect, where
  * "sensible" means return a result that will cause a sensible error later.
  */
sealed trait Splitter
case class Custom(run: String => List[String]) extends Splitter
case class Delimited(delimiter: String) extends Splitter
case class Fixed(lengths: List[Int]) extends Splitter

/** Factory for [[au.com.cba.omnia.maestro.core.split.Splitter]] instances.  */
object Splitter {

  def split(splitter: Splitter, s: String) = splitter match {
    case Custom(splitter) => splitter(s)
    case Delimited(delimiter) => GoogSplitter.on(delimiter).split(s).asScala.toList
    case Fixed(lengths) => {
      val starts = lengths.scanLeft(0)(_ + _)
      val ends = starts.tail
      val totalLength = starts.last

      // we try to be "sensible" by ensuring that, if s has incorrect length,
      // the number of values we return != lengths.length

      if (s.length > totalLength) {
        // if we have extra characters, add an extra column to contain them
        // so we return lengths.length + 1 values, and
        // hopefully the parsing framework will throw an error on this later
        (starts, ends).zipped.map(s.substring(_,_)) ++ List(s.substring(totalLength, s.length))
      }
      else if (s.length < totalLength) {
        // if we have less characters, we only return fields we have complete text for
        // thus ensuring we return less values then lengths.length
        // hopefully the parsing framework will throw an error on this later
        val endsPresent = ends.takeWhile(_ <= s.length)
        (starts, endsPresent).zipped.map(s.substring(_,_))
      }
      else {
        (starts, ends).zipped.map(s.substring(_,_))
      }
    }
  }

  /** Creates a splitter with a custom splitting function */
  def custom(splitter: String => List[String]) = Custom(splitter)

  /**
    * Creates a splitter that splits delimited strings.
    *
    * Valid for all input.
    */
  def delimited(delimiter: String) = Delimited(delimiter)

  /**
    * Creates a splitter that splits according to predefined column lengths.
    *
    * Valid on strings containing exactly the number of characters required to
    * fill all columns.
    *
    * When passed invalid input (wrong number of characters), we ensure that
    * `Splitter.fixed(lengths).run(invalid).length != lengths.length`.
    */
  def fixed(lengths: List[Int]) = Fixed(lengths)
}
