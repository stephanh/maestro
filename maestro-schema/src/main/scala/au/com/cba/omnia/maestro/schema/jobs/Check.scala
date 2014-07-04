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

import scala.io.Source
import org.apache.hadoop.fs._
import org.apache.commons.lang.StringEscapeUtils._
import com.twitter.scalding._, Dsl._, TDsl._

import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.hive.Input

// TODO: limit the number of bad rows output by a flag.
// TODO: report the primary key for rows that do not match.
// We want to be able to find them again.

// ----------------------------------------------------------------------------
// Hadoop job to check a table against an existing schema.
class Check(args: Args) 
  extends Job(args) 
{
  val nameSchema  = args("schema")
  val nameInput   = args("input")
  val nameOutput  = args("output")

  val filesInput  = Input.list(nameInput)
  val pipeInput   = MultipleTextLineFiles(filesInput.map{_.toString} :_*)
  val pipeOutput  = TextLine(nameOutput)


  // Read the schema definition.
  val strSchema = 
    Source.fromFile(nameSchema)
      .getLines
      .map { _ + "\n" }
      .reduceLeft(_+_)

  // Throw an exception if the schema didn't parse.
  val schema = 
    parser.Schema(strSchema) match { 
      case Left(err) => throw new Exception("schema parse error: " + err.toString())
      case Right(s)  => s
    }

  // Check the input files.
  pipeInput.read

    // The TextLine reader adds a line number field when reading,
    // but we only want the line data.
    .project ('line)

     // Check each row against the schema, producing a sequence of fields
     // that don't match.
    .map     ('line -> 'errors)   { s : String => 
      Check.validRow(schema, '|', s).toSeq }

     // Only report on rows that have at least one error.
    .filter  ('errors)            { r : Seq[(ColumnSpec, String)] =>
      r.size > 0 }

     // Pretty print the errors for each row.
    .map     ('errors -> 'result) { r : Seq[(ColumnSpec, String)] =>
      Pretty.prettyErrors(r) }

    // Write out the bad rows to a file.
    .project ('result)
    .write   (pipeOutput)
}


// ----------------------------------------------------------------------------
object Check
{
  // Check if a row matches the associated table spec.
  // Returns the column specs and field values that don't match.
  def validRow(spec: TableSpec, separator: Char, s: String)
    : Seq[(ColumnSpec, String)]
    = s .split(separator)
        .zip     (spec.columnSpecs)
        .flatMap (ss => ss match {
          case (field, cspec) => checkField(cspec, field) })

  // Check if a field value matches its associated column spec,
  //   returning None if yes, or the original spec and string if no.
  def checkField(spec: ColumnSpec, s: String)
    : Option[(ColumnSpec, String)]
    = if (spec.matches(s))
            None
      else  Some((spec, s))
}


// ----------------------------------------------------------------------------
object Pretty
{
  // Pretty print an optional format, showing '-' for the None case.
  def prettyOptionFormat(op: Option[Format]): String
    = op match { 
        case None    => "-"
        case Some(f) => f.pretty 
    }

  // Pretty print a sequence of check errors.
  def prettyErrors(errors: Seq[(ColumnSpec, String)]): String
    = errors
        .map { xx => xx match {
          case (spec, s)  
            => spec.name ++ 
                "("       ++ 
                "\"" ++ escapeJava(s) ++ "\"" ++ 
                " : "     ++ 
                prettyOptionFormat(spec.format) ++ 
                ")" }}
        .mkString("; ")
}

