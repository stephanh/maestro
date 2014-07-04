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

import  au.com.cba.omnia.maestro.schema.syntax._


// ----------------------------------------------------------------------------
// A wrapper for raw syntaxes, and topes with their syntax.
abstract class Classifier
{ 
  // Human readable name for the Classifier.
  def name : String

  // Sort order when pretty printing schemas.
  def sortOrder : Int

  // Yield how well the classifier matches this String.
  // In the range [0.0, 1.0].
  // If 0.0 it definately does not match, if 1.0 it definately does.
  def likeness (s : String) : Double

  // Check if this string completely matches the Tope, or not.
  def matches  (s : String) : Boolean
    = likeness(s) >= 1.0
}


// A raw syntax specification.
case class ClasSyntax  (syntax : Syntax)
  extends Classifier
{ 
  val name : String
    = syntax.name

  val sortOrder : Int
    = syntax.sortOrder

  def likeness (s : String) : Double
    = syntax.likeness(s)
}

// A syntax specification that belongs to a tope.
case class ClasTope    (tope   : Tope, syntax: Syntax)
  extends Classifier
{
  val name : String
    = tope.name + "." ++ syntax.name

  val sortOrder : Int
    = 100


  def likeness (s : String) : Double
    = syntax.likeness(s)
}


// ----------------------------------------------------------------------------
object Classifier
{
  // Yield an array of all classifiers we know about.
  val all : Array[Classifier] = {

    // All the raw syntaxes classifiers.
    def sx  : Array[Classifier]
      = Syntaxes.syntaxes     
          .map { s  : Syntax         => ClasSyntax(s) }.toArray

    // All the tope syntax classifiers.
    def st  : Array[Classifier]
      = Topes.topeSyntaxes
          .map { ts : (Tope, Syntax) => ts match {
            case (tope, syntax) => ClasTope (tope, syntax) }}

    sx ++ st
  }
}

