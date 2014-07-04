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
package jobs

import scala.collection._
import org.apache.hadoop.fs._
import com.twitter.scalding._, Dsl._, TDsl._

import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.hive.Input


// ----------------------------------------------------------------------------
// Hadoop job for tasting tables to see what's in them.
class Taste(args : Args) 
  extends Job(args) 
{
  val nameInput   = args("input")
  val fileOutput  = args("output")

  val filesInput  = Input.list(nameInput)

  val pipeInput   = MultipleTextLineFiles(filesInput.map{_.toString} :_*)

  val pipeOutput  = TextLine(fileOutput)

  // Read input files.
  pipeInput.read

    // The TextLine reader adds a line number field when reading, 
    // but we only want the line data.
    .project('line)

    // Build a map of which types are matched by each line.
    .map    ('line -> 'num)(Taste.classifyPSV) 

    // Aggregate the maps for each line on separate reducers.
    .groupRandomly(16) { g : GroupBuilder 
      => g.reduce ('num   -> 'count)(Taste.combineRowCounts)
    }

    // Sum up the final results on a single reducer.
    .groupAll { g : GroupBuilder 
      => g.reduce ('count -> 'total)(Taste.combineRowCounts)
    }

    // Show the classifications in a human readable format.
    .map  ('total -> 'str) { counts : Array[Array[Int]]
      => Schema.showCountsRow (Classifier.all, counts)}
    .project('str)

    // Write the counts out to file.
    .write (pipeOutput)
}


// ----------------------------------------------------------------------------
object Taste 
{ 
  // Get all the possible classifications for each field in a PSV.
  def classifyPSV (s : String)  : Array[Array[Int]] 
    = s .split('|')
        .map (classifyField)


  // Get all the possible classifications for a given field.
  def classifyField (s : String) : Array[Int] 
    = Classifier.all
        .map {_.likeness(s)}
        .map {like => if (like >= 1.0) 1 else 0}


  // Combine the classification counts for two rows.
  def combineRowCounts
      ( xs1 : Array[Array[Int]]
      , xs2 : Array[Array[Int]]) : Array[Array[Int]]
    = xs1 .zip (xs2) .map { x12 => x12 match { 
        case (x1, x2) => combineFieldCounts(x1, x2) } }


  // Combine the classification counts for two fields.
  def combineFieldCounts
      ( xs1 : Array[Int]
      , xs2 : Array[Int]) : Array[Int]
    = xs1 .zip (xs2) .map { x12 => x12 match { 
        case (x1, x2) => (x1 + x2) } }
}

