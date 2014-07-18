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

import com.cba.omnia.edge.hdfs.{Error, Ok, Result}

class InputSpec extends Specification {
  "1_Timestamp breakup" should {

    """T1.1: return the correct breakup of year month day hour yyyyMMdd""" in new IsolatedTest {
      Input.parseTimeStamp("yyyyMMdd", "20140531") must beLike {
        case Ok(time) => {
          time.getYear mustEqual 2014
          time.getMonthOfYear mustEqual 5
          time.getDayOfMonth mustEqual 31
          time.getHourOfDay mustEqual 0
        }
      }
    }

    """T1.2: return the correct breakup of year month day hour yyyyMMddHHmm""" in new IsolatedTest {
      Input.parseTimeStamp("yyyyMMddHHmm", "201405312300") must beLike {
        case Ok(time) => {
          time.getYear mustEqual 2014
          time.getMonthOfYear mustEqual 5
          time.getDayOfMonth mustEqual 31
          time.getHourOfDay mustEqual 23
        }
      }
    }

    """T1.3: return the correct breakup of year month day hour early morning yyyyMMddHHmm""" in new IsolatedTest {
      Input.parseTimeStamp("yyyyMMddHHmm", "201405310005") must beLike {
        case Ok(time) => {
          time.getYear mustEqual 2014
          time.getMonthOfYear mustEqual 5
          time.getDayOfMonth mustEqual 31
          time.getHourOfDay mustEqual 0
        }
      }
    }

    """T1.4: return the correct breakup of year month day hour yyyyddMM""" in new IsolatedTest {
      Input.parseTimeStamp("yyyyddMM", "20140512") must beLike {
        case Ok(time) => {
          time.getYear mustEqual 2014
          time.getMonthOfYear mustEqual 12
          time.getDayOfMonth mustEqual 5
          time.getHourOfDay mustEqual 0
        }
      }
    }

    """T1.5: return the correct breakup of year month day hour yyMMdd""" in new IsolatedTest {
      Input.parseTimeStamp("yyMMdd", "140531") must beLike {
        case Ok(time) => {
          time.getYear mustEqual 2014
          time.getMonthOfYear mustEqual 5
          time.getDayOfMonth mustEqual 31
          time.getHourOfDay mustEqual 0
        }
      }
    }

    """T1.6: return the correct breakup of year month day hour yyMMddHH""" in new IsolatedTest {
      Input.parseTimeStamp("yyMMddHH", "14053105") must beLike {
        case Ok(time) => {
          time.getYear mustEqual 2014
          time.getMonthOfYear mustEqual 5
          time.getDayOfMonth mustEqual 31
          time.getHourOfDay mustEqual 5
        }
      }
    }

    """T1.7: return the correct breakup of year month day hour yyyy-MM-dd-HH""" in new IsolatedTest {
      Input.parseTimeStamp("yyyy-MM-dd-HH", "2014-05-31-05") must beLike {
        case Ok(time) => {
          time.getYear mustEqual 2014
          time.getMonthOfYear mustEqual 5
          time.getDayOfMonth mustEqual 31
          time.getHourOfDay mustEqual 5
        }
      }
    }

    """T1.8: return the correct breakup of year month day hour yyMM""" in new IsolatedTest {
      Input.parseTimeStamp("yyMM", "1405") must beLike {
        case Ok(time) => {
          time.getYear mustEqual 2014
          time.getMonthOfYear mustEqual 5
          time.getDayOfMonth mustEqual 1
          time.getHourOfDay mustEqual 0
        }
      }
    }

    """T1.9: return the correct breakup of year month day hour yyyyDDMM_HHmmss""" in new IsolatedTest {
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
    }
  }

  "2_ControlFile check" should {

    """T2.1: Check if the file is control file .CTL""" in new IsolatedTest {
      val local = File.createTempFile("local", ".CTL", testDir)
      Input.isControl(local) must beTrue
    }

    """T2.2: Check if the file is control file .CTR""" in new IsolatedTest {
      val local = File.createTempFile("local", ".CTR", testDir)
      Input.isControl(local) must beTrue
    }
  }

  "3_findFiles filter " should {

    """T3.1: filter with similar names in prefix""" in new IsolatedTest {
      val f1 = new File(testDir + File.separator + "local20140506.txt")
      val f2 = new File(testDir + File.separator + "localname20140506.txt")
      val data1 = Data(f1, new File(List("2014", "6", "5") mkString File.separator))
      f1.createNewFile
      f2.createNewFile

      val fileList = Input.findFiles(testDir, "local", "yyyyddMM")
      fileList mustEqual Ok(List(data1))
    }

    """T3.2: filter with similar names in middle""" in new IsolatedTest {
      val f1 = new File(testDir + File.separator + "yahoolocal20140506.txt")
      val f2 = new File(testDir + File.separator + "localname20140506.txt")
      f1.createNewFile
      f2.createNewFile

      val fileList = Input.findFiles(testDir, "local", "yyyyddMM")
      fileList mustEqual Ok(Nil)
    }
  }

  "4_getInput files " should {

    """T4.1: reject files with no timestamp""" in new IsolatedTest {
      val badFile = new File(testDir, "local.txt")
      val src = Input.getInput(badFile, "yyyyddMM")
      src must beLike { case Error(_) => ok }
    }

    """T4.2: label control files""" in new IsolatedTest {
      val ctrlFile = new File(testDir, "local20140506.CTL")
      val src = Input.getInput(ctrlFile, "yyyyddMM")
      src must beLike { case Ok(Control(_)) => ok }
    }

    """T4.3: reject files with wrong date format""" in new IsolatedTest {
      val badFile = new File(testDir, "local201406")
      val src = Input.getInput(badFile, "yyyyddMM")
      src must beLike { case Error(_) => ok }
    }

    """T4.4: reject files with impossible date""" in new IsolatedTest {
      val badFile = new File(testDir, "Tlocal20149999")
      val src = Input.getInput(badFile, "yyyyddMM")
      src must beLike { case Error(_) => ok }
    }

    """T4.5: reject files with multiple dates""" in new IsolatedTest {
      val badFile = new File(testDir, "local20140506and20140507")
      val src = Input.getInput(badFile, "yyyyddMM")
      src must beLike { case Error(_) => ok }
    }

    """T4.6: accept files with one correct date""" in new IsolatedTest {
      val goodFile = new File(testDir, "local20140506")
      val src = Input.getInput(goodFile, "yyyyddMM")
      src must beLike { case Ok(Data(_,_)) => ok }
    }
  }
}
