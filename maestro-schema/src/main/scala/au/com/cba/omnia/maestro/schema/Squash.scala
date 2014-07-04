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

import au.com.cba.omnia.maestro.schema._
import au.com.cba.omnia.maestro.schema.syntax._


object Squash
{
  // Map of field Classifier to how many fields in a table column
  // matched that classifier.
  type ClasMap      = Map[Classifier, Int]


  // For general syntaxes that completely subsume more specific ones, 
  //  remove the more general ones. 
  //
  // For example, if we have Real:100 and Digits:100 we only need to keep
  // Digits:100 because Real:100 because all digit strings are also real 
  // numbers.
  //
  // The syntax hierarchy is as follows, where syntaxes on the left 
  // subsume syntaxes on the right.
  //
  //   AlphaNum  + Alpha  + Upper + Exact(..)
  //                      - Lower + Exact(..)
  //             + Real   + Digits + Exact (..)
  //
  def squash  (ss : ClasMap) : ClasMap = {
    val ss2 = squash1(ss)
    if (ss.size == ss2.size) ss 
    else squash(ss2)
  }


  // Do a single round of squashing, removing just one classifier.
  def squash1 (ss : ClasMap) : ClasMap
   = (sub(ClasSyntax(Nat),      ClasSyntax(Exact("0")))_         compose
      sub(ClasSyntax(Nat),      ClasSyntax(Exact("1")))_         compose
      sub(ClasSyntax(Nat),      ClasSyntax(Exact("99999")))_     compose
      sub(ClasSyntax(Nat),      ClasSyntax(Exact("999999999")))_ compose

      sub(ClasSyntax(Real),     ClasSyntax(Nat))_                compose

      sub(ClasSyntax(Lower),    ClasSyntax(Exact("active")))_    compose
      sub(ClasSyntax(Lower),    ClasSyntax(Exact("inactive")))_  compose

      sub(ClasSyntax(Upper),    ClasSyntax(Exact("ACTIVE")))_    compose
      sub(ClasSyntax(Upper),    ClasSyntax(Exact("INACTIVE")))_  compose
      sub(ClasSyntax(Upper),    ClasSyntax(Exact("N")))_         compose
      sub(ClasSyntax(Upper),    ClasSyntax(Exact("N")))_         compose
      sub(ClasSyntax(Upper),    ClasSyntax(Exact("Y")))_         compose
      sub(ClasSyntax(Upper),    ClasSyntax(Exact("XXXXXXXX")))_  compose

      sub(ClasSyntax(Alpha),    ClasSyntax(Lower))_              compose
      sub(ClasSyntax(Alpha),    ClasSyntax(Upper))_              compose

      sub(ClasSyntax(AlphaNum), ClasSyntax(Real))_               compose
      sub(ClasSyntax(AlphaNum), ClasSyntax(Alpha))
      ) (ss)


  // The first classifier subsumes the second.
  // In the ClasMap, if the first classifier has the same count as the second
  //  then remove the first. 
  def sub 
    ( k1 : Classifier, k2 : Classifier)
    ( ss : ClasMap) : ClasMap
    = if (ss.isDefinedAt(k1) && ss.isDefinedAt(k2))
        if (ss(k1) == ss(k2)) (ss - k1) else ss
      else ss
}

