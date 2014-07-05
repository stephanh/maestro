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

import org.scalacheck.{Gen, Prop}

import au.com.cba.omnia.maestro.core.split

object SplitterSpec extends test.Spec { def is = s2"""

Splitter properties
====================

  number of delimited segments = numDelims + 1  $delimitedNum
  mkString(delim) reverses delim split          $delimitedRoundTrip

  fixed split returns bad length on bad input   $fixedBadLength
  fixed split length is correct on good inputs  $fixedNum
  concat reverses fixed split on good inputs    $fixedRoundTrip
"""

  val delimiterChr: Char = '$' // delimitedStr assumes this is not an alphaChar
  val delimiter: String = delimiterChr.toString

  val delimitedStr: Gen[String] = for {
    numSegments <- Gen.choose(0,20)
    segments    <- Gen.listOfN(numSegments, Gen.alphaStr)
  } yield segments.mkString(delimiter)


  val delimitedNum: Prop = Prop.forAll(delimitedStr) (str => {
    val numDelimiters = str.count(_ == delimiterChr)
    val segments      = Splitter.split(Splitter.delimited(delimiter), str)
    segments.length must_== (numDelimiters+1)
  })

  val delimitedRoundTrip: Prop = Prop.forAll(delimitedStr) (str => {
    val segments = Splitter.split(Splitter.delimited(delimiter), str)
    segments.mkString(delimiter) must_== str
  })


  val goodFixedLengthStr: Gen[(List[Int], String)] = for {
    numFields <- Gen.choose(0, 100)
    lengths   <- Gen.listOfN(numFields, Gen.choose(0, 20))
    row       <- Gen.listOfN(lengths.sum, Gen.alphaChar).map(_.mkString)
  } yield (lengths, row)

  val badFixedLengthStr: Gen[(List[Int], String)] = for {
    numFields <- Gen.choose(0, 100)
    lengths   <- Gen.listOfN(numFields, Gen.choose(0, 20))
    offset    <- Gen.choose(-10,10).suchThat(off => off != 0 && lengths.sum + off >= 0)
    row       <- Gen.listOfN(lengths.sum + offset, Gen.alphaChar).map(_.mkString)
  } yield (lengths, row)

  val fixedBadLength: Prop = Prop.forAll(badFixedLengthStr) { case (lengths, str) => {
    val segments = Splitter.split(Splitter.fixed(lengths), str)
    segments.length must_!= lengths.length
  }}

  val fixedNum: Prop = Prop.forAll(goodFixedLengthStr) { case (lengths, str) => {
    val segments = Splitter.split(Splitter.fixed(lengths), str)
    segments.length must_== lengths.length
  }}

  val fixedRoundTrip: Prop = Prop.forAll(goodFixedLengthStr) { case (lengths, str) => {
    val segments = Splitter.split(Splitter.fixed(lengths), str)
    segments.mkString must_== str
  }}
}
