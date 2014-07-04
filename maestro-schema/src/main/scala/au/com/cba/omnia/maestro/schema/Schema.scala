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

import scala.collection.immutable._
import au.com.cba.omnia.maestro.schema.hive._
import au.com.cba.omnia.maestro.schema.syntax._

// TODO: We want the counts that match as well as the average likeliness.
// This will help distinguish balances from reals.


/** The schema for a table.
 *
 *  @param database name of the database
 *  @param table name of the table
 *  @param ignore SQL ignore (for header fields and others)
 *  @param columnSpecs column specifications
 */
case class TableSpec(
  database    : String,
  table       : String,
  ignore      : Seq[Schema.Ignore],
  columnSpecs : Seq[ColumnSpec]
)


/** Column specification.
 *
 *  @param name name of the column
 *  @param syntax base syntax (corresponds closely to Hive encoding)
 *  @param tope most-likely Tope
 *  @param histogram histogram of Tope instances that were found
 *  @param comment comment associated with the column
 */
case class ColumnSpec(
  name        : String,
  hivetype    : HiveType.HiveType,
  format      : Option[Format],
  histogram   : Histogram,
  comment     : String)
{
  def matches(s : String): Boolean
    = format match {
        case None     => true
        case Some(f)  => f.matches(s) }
}


/** Data format for a column.
 *  This contains the possible classifiers for the data value.
 */
case class Format(
  list        : List[Classifier])
{
  // Append two formats.
  def ++(that : Format) : Format 
    = Format(this.list ++ that.list)

  // Check if this string matches any of the classifiers in the Format.
  def matches(s : String): Boolean
    = list
        .map    { cl => cl.matches(s) }
        .reduce (_ || _)

  // Pretty print a Format.
  def pretty : String
    = list 
        .map {_.name}
        .reduceLeft {(n, rest) => n ++ " + " ++ rest }
}


case class Histogram(
  counts      : Map[Classifier, Int]
)



object Schema {

  /** Base trait for column entries that are ignored. */
  sealed trait Ignore

  /**
   * Ignore entries in a field that are equal to some value.
   *
   * Encoded as
   * {{{
   *   name = "VALUE"
   * }}}
   * in the text format.
   *
   *
   * @param field field to check
   * @param value value to ignore in this field
   */
  case class IgnoreEq(field: String, value: String) extends Ignore

  /**
   * Ignore entries in a field that are equal to null.
   *
   * Encoded as
   * {{{
   *   name is null
   * }}}
   * in the text format.
   *
   * @param field field to ignore when it is null
   */
  case class IgnoreNull(field: String) extends Ignore


  // Show the classification counts for row of the table.
  // The counts for each field go on their own line.
  def showCountsRow 
    ( classifications : Array[Classifier]
    , counts          : Array[Array[Int]]) 
    : String
    = counts
        .map { counts => showCountsField(classifications, counts) }
        .map { s => s + "\n" }
        .mkString


  // Show the classification counts for a single field,
  // or '-' if there aren't any.
  def showCountsField 
    ( classifications : Array[Classifier]
    , counts          : Array[Int]) 
    : String = {

    // Squash the classification counts to eliminate classifiers that
    // subsume more specific ones.
    val clasMap_sq    : Map[Classifier, Int]
      = Squash.squash (Classifier.all .zip (counts). toMap)

    // Make a sequence of the remaining classification names and their counts.
    val clasCounts
      = clasMap_sq
          .toSeq
          .map { ci => ci match {
            case (c, i) => (c, c.name, i) }}

    // Pretty print classifications with their counts.
    val strs
      = clasCounts
          // Drop classifications that didn't match.
          .filter   { cni  => cni match {
            case (c, n, i)   => i > 0 }}

          // Sort classifications so we get a deterministic ordering
          // between runs.
          .sortWith { (x1, x2) => x1._1.name      < x2._1.name }
          .sortWith { (x1, x2) => x1._1.sortOrder < x2._1.sortOrder }

          // Pretty print with the count after the classifier name.
          .map      { nc  => nc match {
            case (c, n, i)   => n + ":" + i.toString }}

    // If there aren't any classifications for this field then
    // just print "-" as a placeholder.
    if (strs.length == 0) "-"
    else strs.mkString(", ")
  }

}

