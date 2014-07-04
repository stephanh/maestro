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

import au.com.cba.omnia.maestro.schema.syntax._


// ----------------------------------------------------------------------------
// A concrete syntax that we identify as something more specific than 
// a random collection of characters.
// 
// A syntax may describe a single Tope, though each Tope can have many syntaxes.
//
// A syntax may not describe a Tope at all. For example, if some field is 
// always a collection of digits then we identify that as a specific syntax 
// without knowing what external entity it refers to.
//
abstract class Syntax 
{
  // Printable name for a syntax.
  // This can be displayed to the user, as well as used for a key for maps.
  def name : String

  // Sort order to use when printing schemas.
  // Lower is earlier.
  def sortOrder : Int
    = 0

  // Yield how well the syntax matches this String.
  // In the range [0.0, 1.0].
  // If 0.0 it definately does not match, if 1.0 it definately does.
  def likeness (s : String) : Double

  // Check if this string completely matches the Tope, or not.
  def matches  (s : String) : Boolean
    = likeness(s) >= 1.0
}


// ----------------------------------------------------------------------------
// All the syntaxes we support.
//
// The definitions of the individual syntaxes are written out by hand,
// instead of using something more fancy involving higher order functions.
// We need the matching to be fast because we test all fields against
// all syntaxes.
//
object Syntaxes
{
  // All the generic syntaxes that we know about.
  //  These are standard formats and encodings, that we use when we can't
  //  identify what Tope a String refers to.
  val syntaxes  = Array(
    Any,
    Empty,
    Null,
    White,
    Upper,
    Lower,
    Alpha,
    AlphaNum,
    Nat, 
    Real,
    NegReal,
    PosReal,

    // Null disasters.
    Exact("NULL"),
    Exact("null"),

    // Stupid placeholders.
    Exact("999999999"),
    Exact("99999"),
    Exact("99999.99"),
    Exact("XXXXXXXX"),

    // Timestamp sentinals.
    Exact("0000-00-00"),
    Exact("0001-01-01"),
    Exact("1900-01-01"),
    Exact("9999-12-31"),

    // Bool encodings.
    Exact("0"),
    Exact("1"),
    Exact("Y"),
    Exact("N"),
    Exact("ACTIVE"),
    Exact("INACTIVE"),
    Exact("active"),
    Exact("inactive"))


  // The names of all the syntaxes.
  val names : Array[String]
    = syntaxes .map {_.name}
}

