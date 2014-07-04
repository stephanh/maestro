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
package parser

import scala.collection.immutable._
import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{NoPosition, Position, Reader}

import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.syntax._
import au.com.cba.omnia.maestro.schema.hive._
import au.com.cba.omnia.maestro.schema.hive.HiveType._
import au.com.cba.omnia.maestro.schema.Schema._


/**
 * Parser for table tastes.
 */
object Taste {
  def apply(input: String): Either[Schema.NoSuccess, Seq[Histogram]]
    = Schema.parseTaste(input)
}


