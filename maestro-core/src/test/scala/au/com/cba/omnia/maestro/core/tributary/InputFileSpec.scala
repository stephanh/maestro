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
package tributary

import org.specs2.mutable.Specification

import java.io.File

import org.joda.time.DateTime

class InputFileSpec extends Specification {
  "1_Timestamp breakup" should {

    """T1.1: return the correct breakup of year month day hour yyyyMMdd""" in new IsolatedTest {
      val timebreak: DateTime = InputFile
        .parseTimeStamp("yyyyMMdd", "20140531")
        .getOrElse(new DateTime("2000"))
      timebreak.getYear mustEqual 2014
      timebreak.getMonthOfYear mustEqual 5
      timebreak.getDayOfMonth mustEqual 31
      timebreak.getHourOfDay mustEqual 0
    }

    """T1.2: return the correct breakup of year month day hour yyyyMMddHHmm""" in new IsolatedTest {
      val timebreak: DateTime = InputFile
        .parseTimeStamp("yyyyMMddHHmm", "201405312300")
        .getOrElse(new DateTime("2000"))
      timebreak.getYear mustEqual 2014
      timebreak.getMonthOfYear mustEqual 5
      timebreak.getDayOfMonth mustEqual 31
      timebreak.getHourOfDay mustEqual 23
    }

    """T1.3: return the correct breakup of year month day hour early morning yyyyMMddHHmm""" in new IsolatedTest {
      val timebreak: DateTime = InputFile
        .parseTimeStamp("yyyyMMddHHmm", "201405310005")
        .getOrElse(new DateTime("2000"))
      timebreak.getYear mustEqual 2014
      timebreak.getMonthOfYear mustEqual 5
      timebreak.getDayOfMonth mustEqual 31
      timebreak.getHourOfDay mustEqual 0
    }

    """T1.4: return the correct breakup of year month day hour yyyyddMM""" in new IsolatedTest {
      val timebreak: DateTime = InputFile
        .parseTimeStamp("yyyyddMM", "20140512")
        .getOrElse(new DateTime("2000"))
      timebreak.getYear mustEqual 2014
      timebreak.getMonthOfYear mustEqual 12
      timebreak.getDayOfMonth mustEqual 5
      timebreak.getHourOfDay mustEqual 0
    }

    """T1.5: return the correct breakup of year month day hour yyMMdd""" in new IsolatedTest {
      val timebreak = InputFile
        .parseTimeStamp("yyMMdd", "140531")
        .getOrElse(new DateTime("2000"))
      timebreak.getYear mustEqual 2014
      timebreak.getMonthOfYear mustEqual 5
      timebreak.getDayOfMonth mustEqual 31
      timebreak.getHourOfDay mustEqual 0
    }

    """T1.6: return the correct breakup of year month day hour yyMMddHH""" in new IsolatedTest {
      val timebreak = InputFile
        .parseTimeStamp("yyMMddHH", "14053105")
        .getOrElse(new DateTime("2000"))
      timebreak.getYear mustEqual 2014
      timebreak.getMonthOfYear mustEqual 5
      timebreak.getDayOfMonth mustEqual 31
      timebreak.getHourOfDay mustEqual 5
    }

    """T1.7: return the correct breakup of year month day hour yyyy-MM-dd-HH""" in new IsolatedTest {
      val timebreak = InputFile
        .parseTimeStamp("yyyy-MM-dd-HH", "2014-05-31-05")
        .getOrElse(new DateTime("2000"))
      timebreak.getYear mustEqual 2014
      timebreak.getMonthOfYear mustEqual 5
      timebreak.getDayOfMonth mustEqual 31
      timebreak.getHourOfDay mustEqual 5
    }

    """T1.8: return the correct breakup of year month day hour yyMM""" in new IsolatedTest {
      val timebreak = InputFile
        .parseTimeStamp("yyMM", "1405")
        .getOrElse(new DateTime("2000"))
      timebreak.getYear mustEqual 2014
      timebreak.getMonthOfYear mustEqual 5
      timebreak.getDayOfMonth mustEqual 1
      timebreak.getHourOfDay mustEqual 0
    }

    """T1.9: return the correct breakup of year month day hour yyyyDDMM_HHmmss""" in new IsolatedTest {
      val timebreak = InputFile
        .parseTimeStamp("yyyyMMdd_HHmmss", "20140612_230441")
        .getOrElse(new DateTime("2000"))
      timebreak.getYear mustEqual 2014
      timebreak.getMonthOfYear mustEqual 6
      timebreak.getDayOfMonth mustEqual 12
      timebreak.getHourOfDay mustEqual 23
      timebreak.getMinuteOfHour mustEqual 4
      timebreak.getSecondOfMinute mustEqual 41
    }
  }

  "2_ControlFile check" should {

    """T2.1: Check if the file is control file .CTL""" in new IsolatedTest {
      val local = File.createTempFile("local", ".CTL", testDir)
      InputFile.isControlFile(local) must beTrue
    }

    """T2.2: Check if the file is control file .CTR""" in new IsolatedTest {
      val local = File.createTempFile("local", ".CTR", testDir)
      InputFile.isControlFile(local) must beTrue
    }
  }

  "3_findFiles filter " should {

    """T3.1: filter with similar names in prefix""" in new IsolatedTest {
      val f1 = new File(testDir + File.separator + "local20140506.txt")
      val f2 = new File(testDir + File.separator + "localname20140506.txt")
      val local1 = f1.createNewFile
      val local2 = f2.createNewFile
      val fileList = InputFile.findFiles(testDir, "local", "yyyyddMM")
      fileList.length mustEqual 1
      fileList.contains(f1)
    }

    """T3.2: filter with similar names in middle""" in new IsolatedTest {
      val f1 = new File(testDir + File.separator + "yahoolocal20140506.txt")
      val f2 = new File(testDir + File.separator + "localname20140506.txt")
      val local1 = f1.createNewFile
      val local2 = f2.createNewFile
      val fileList = InputFile.findFiles(testDir, "local", "yyyyddMM")
      fileList.length mustEqual 0
    }
  }

  "4_getInputFile files " should {

    """T4.1: reject files with no timestamp""" in new IsolatedTest {
      val badFile = new File(testDir, "local.txt")
      val src = InputFile.getInputFile(badFile, "yyyyddMM")
      src must beLike { case UnexpectedFile(_,_) => ok }
    }

    """T4.2: label control files""" in new IsolatedTest {
      val ctrlFile = new File(testDir, "local20140506.CTL")
      val src = InputFile.getInputFile(ctrlFile, "yyyyddMM")
      src must beLike { case ControlFile(_) => ok }
    }

    """T4.3: reject files with wrong date format""" in new IsolatedTest {
      val badFile = new File(testDir, "local201406")
      val src = InputFile.getInputFile(badFile, "yyyyddMM")
      src must beLike { case UnexpectedFile(_,_) => ok }
    }

    """T4.4: reject files with impossible date""" in new IsolatedTest {
      val badFile = new File(testDir, "Tlocal20149999")
      val src = InputFile.getInputFile(badFile, "yyyyddMM")
      src must beLike { case UnexpectedFile(_,_) => ok }
    }

    """T4.5: reject files with multiple dates""" in new IsolatedTest {
      val badFile = new File(testDir, "local20140506and20140507")
      val src = InputFile.getInputFile(badFile, "yyyyddMM")
      src must beLike { case UnexpectedFile(_,_) => ok }
    }

    """T4.6: accept files with one correct date""" in new IsolatedTest {
      val goodFile = new File(testDir, "local20140506")
      val src = InputFile.getInputFile(goodFile, "yyyyddMM")
      src must beLike { case DataFile(_,_) => ok }
    }
  }
}
