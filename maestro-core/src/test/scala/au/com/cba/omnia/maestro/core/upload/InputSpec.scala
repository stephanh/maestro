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
package upload

import org.specs2.Specification

import java.io.File

import org.joda.time.DateTime

import com.cba.omnia.edge.hdfs.{Error, Ok, Result}

class InputSpec extends Specification { def is = s2"""

Input properties
================

timestamp parsing
-----------------

  can parse yyyMMdd         $parse_yyyyMMdd
  can parse yyyyMMddHHmm    $parse_yyyyMMddHHmm
  can parse yyyyddMM        $parse_yyyyddMM
  can parse yyMMdd          $parse_yyMMdd
  can parse yyMMddHH        $parse_yyMMddHH
  can parse yyyy-MM-dd-HH   $parse_yyyy_MM_dd_HH
  can parse yyMM            $parse_yyMM
  can parse yyyyMMdd_HHmmss $parse_yyyyMMdd_HHmmss

control files
-------------

  .CTL is control file ext $ctlIsControl
  .CTR is control file ext $ctrIsControl

 findFiles filter
 ----------------

  reject longer prefix    $rejectLong
  reject prefix in middle $rejectMiddle

 categorize input files
 ----------------------

  accept files with correct timestamp      $aceptGoodTime
  label control files                      $labelControlFiles
  reject files without timestamp           $rejectNoTimestamp
  reject files with multiple timestamps    $rejectMultupleTimestamp
  reject files with wrong timestamp format $rejectBadTimeFormat
  reject files with impossible timestamp   $rejectBadTime
"""

  def parse_yyyyMMdd =
    Input.parseTimeStamp("yyyyMMdd", "20140531") must beLike {
      case Ok(time) => {
        time.getYear mustEqual 2014
        time.getMonthOfYear mustEqual 5
        time.getDayOfMonth mustEqual 31
        time.getHourOfDay mustEqual 0
      }
    }

  def parse_yyyyMMddHHmm =
    Input.parseTimeStamp("yyyyMMddHHmm", "201405312300") must beLike {
      case Ok(time) => {
        time.getYear mustEqual 2014
        time.getMonthOfYear mustEqual 5
        time.getDayOfMonth mustEqual 31
        time.getHourOfDay mustEqual 23
      }
    }

  def parse_yyyyddMM =
    Input.parseTimeStamp("yyyyddMM", "20140512") must beLike {
      case Ok(time) => {
        time.getYear mustEqual 2014
        time.getMonthOfYear mustEqual 12
        time.getDayOfMonth mustEqual 5
        time.getHourOfDay mustEqual 0
      }
    }

  def parse_yyMMdd =
    Input.parseTimeStamp("yyMMdd", "140531") must beLike {
      case Ok(time) => {
        time.getYear mustEqual 2014
        time.getMonthOfYear mustEqual 5
        time.getDayOfMonth mustEqual 31
        time.getHourOfDay mustEqual 0
      }
    }

  def parse_yyMMddHH =
    Input.parseTimeStamp("yyMMddHH", "14053105") must beLike {
      case Ok(time) => {
        time.getYear mustEqual 2014
        time.getMonthOfYear mustEqual 5
        time.getDayOfMonth mustEqual 31
        time.getHourOfDay mustEqual 5
      }
    }

  def parse_yyyy_MM_dd_HH =
    Input.parseTimeStamp("yyyy-MM-dd-HH", "2014-05-31-05") must beLike {
      case Ok(time) => {
        time.getYear mustEqual 2014
        time.getMonthOfYear mustEqual 5
        time.getDayOfMonth mustEqual 31
        time.getHourOfDay mustEqual 5
      }
    }

  def parse_yyMM =
    Input.parseTimeStamp("yyMM", "1405") must beLike {
      case Ok(time) => {
        time.getYear mustEqual 2014
        time.getMonthOfYear mustEqual 5
        time.getDayOfMonth mustEqual 1
        time.getHourOfDay mustEqual 0
      }
    }

  def parse_yyyyMMdd_HHmmss =
    Input.parseTimeStamp("yyyyMMdd_HHmmss", "20140612_230441") must beLike {
      case Ok(time) => {
        time.getYear mustEqual 2014
        time.getMonthOfYear mustEqual 6
        time.getDayOfMonth mustEqual 12
        time.getHourOfDay mustEqual 23
        time.getMinuteOfHour mustEqual 4
        time.getSecondOfMinute mustEqual 41
      }
    }

  def ctlIsControl =
    Input.isControl(new File("local.CTL"))

  def ctrIsControl =
    Input.isControl(new File("local.CTR"))

  def rejectLong = isolatedTest((dirs: IsolatedDirs) => {
    val f1 = new File(dirs.testDir, "local20140506.txt")
    val f2 = new File(dirs.testDir, "localname20140506.txt")
    val data1 = Data(f1, new File(List("2014", "06", "05") mkString File.separator))
    f1.createNewFile
    f2.createNewFile

    val fileList = Input.findFiles(dirs.testDir, "local", "yyyyddMM")
    fileList mustEqual Ok(List(data1))
  })

  def rejectMiddle = isolatedTest((dirs: IsolatedDirs) => {
    val f1 = new File(dirs.testDir, "yahoolocal20140506.txt")
    val f2 = new File(dirs.testDir, "localname20140506.txt")
    f1.createNewFile
    f2.createNewFile

    val fileList = Input.findFiles(dirs.testDir, "local", "yyyyddMM")
    fileList mustEqual Ok(Nil)
  })

  def aceptGoodTime = {
    val goodFile = new File("local20140506")
    val src = Input.getInput(goodFile, "yyyyddMM")
    src must beLike { case Ok(Data(_,_)) => ok }
  }

  def labelControlFiles = {
    val ctrlFile = new File("local20140506.CTL")
    val src = Input.getInput(ctrlFile, "yyyyddMM")
    src must beLike { case Ok(Control(_)) => ok }
  }

  def rejectNoTimestamp = {
    val badFile = new File("local.txt")
    val src = Input.getInput(badFile, "yyyyddMM")
    src must beLike { case Error(_) => ok }
  }

  def rejectMultupleTimestamp = {
    val badFile = new File("local20140506and20140507")
    val src = Input.getInput(badFile, "yyyyddMM")
    src must beLike { case Error(_) => ok }
  }

  def rejectBadTimeFormat = {
    val badFile = new File("local201406")
    val src = Input.getInput(badFile, "yyyyddMM")
    src must beLike { case Error(_) => ok }
  }

  def rejectBadTime = {
    val badFile = new File("Tlocal20149999")
    val src = Input.getInput(badFile, "yyyyddMM")
    src must beLike { case Error(_) => ok }
  }
}
