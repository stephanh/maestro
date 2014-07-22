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

import java.io.File
import java.text.SimpleDateFormat

import org.joda.time.DateTime

import scalaz._, Scalaz._

import au.com.cba.omnia.omnitool.time.TimeParser

import com.cba.omnia.edge.hdfs.{Hdfs, Result}

/** A file existing in the source directory */
sealed trait Input

/** A control file, not to be loaded into HDFS */
case class Control(file: File) extends Input

/** A file to be loaded into HDFS */
case class Data(file: File, fileSubDir: File) extends Input

/** Factory for `Input`s */
object Input {

  /** Pattern for fetching time from file name */
  val regexTimeStampString =
    "[0-9]{2,4}[-/]{0,1}[0-9]{2,4}[-/_]{0,1}[0-9]{0,2}[-_/]{0,1}[0-9]{0,2}[-_/]{0,1}[0-9]{0,2}"

  /** Patterns for control files */
  val controlFilePatterns = List("./S_*", "_CTR.*", "_CTR", "\\.CTR", "\\.CTL", "\\.ctl")

  /** Find files from a directory matching a given filename prefix with a timestamp suffix. */
  def findFiles(sourceDir: File, fileName: String, timeFormat: String): Result[List[Input]] = {
    val fileNameRegex = s"^$fileName[_:-]?$regexTimeStampString.*".r
    sourceDir
      .listFiles.toList
      .filter(f => fileNameRegex.findFirstIn(f.getName).isDefined)
      .traverse(getInput(_, timeFormat))
  }

  /** Gets the input file corresponding to a file */
  def getInput(file: File, timeFormat: String): Result[Input] =
    if (isControl(file))
      Result.ok(Control(file))
    else
      for {
        timeStamp <- getTimeStamp(file)
        date      <- parseTimeStamp(timeFormat, timeStamp)
      } yield Data(file, pathToFile(date, timeFormat))

  /** get the timestamp from the file name */
  def getTimeStamp(file: File) =
    regexTimeStampString.r.findAllIn(file.getName).toList match {
      case Nil              => Result.fail(s"could not find timestamp in ${file.getName}")
      case _ :: _ :: _      => Result.fail(s"found more than one timestamp in ${file.getName}")
      case timeStamp :: Nil => Result.ok(timeStamp)
    }

  /** parsing `Result` */
  def parseTimeStamp(timeFormat: String, timeStamp: String) =
    TimeParser.parse(timeStamp, timeFormat).fold(Result.fail(_), Result.ok(_))

  /** check if file matches any of the control file patterns */
  def isControl(file: File) =
    controlFilePatterns.exists(_.r.findFirstIn(file.getName).isDefined)

  /** get the relative path to this file in hdfs and the archive dir */
  def pathToFile(dateTime: DateTime, timeFormat: String): File = {
    // zero padding so that alphabetic order matches numeric order
    val mandatoryDirs = List(
      f"${dateTime.getYear}%04d",
      f"${dateTime.getMonthOfYear}%02d"
    )
    val optionalDirs = List(
      f"${dateTime.getDayOfMonth}%02d",
      f"${dateTime.getHourOfDay}%02d",
      f"${dateTime.getMinuteOfHour}%02d"
    )
    val numOptional = List("dd", "HH", "mm").takeWhile(timeFormat.contains(_)).length

    val actualDirs = mandatoryDirs ++ optionalDirs.take(numOptional)
    new File(actualDirs mkString File.separator)
  }
}
