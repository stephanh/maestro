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

import java.io.File
import java.text.SimpleDateFormat

import org.joda.time.DateTime

import scalaz.{-\/, \/-}

import au.com.cba.omnia.omnitool.time.TimeParser

/** ADT representing the different types of input file we can find when loading
  * into HDFS
  */
sealed trait InputFile

/** A file to be loaded into HDFS */
case class DataFile(file: File, fileSubDir: File) extends InputFile

/** A control file */
case class ControlFile(file: File) extends InputFile

/** A file that unexpectedly can't be processed */
case class UnexpectedFile(file: File, error: String) extends InputFile


/** Factory for `InputFile`s */
object InputFile {

  /** Pattern for fetching time from file name */
  val regexTimeStampString =
    "[0-9]{2,4}[-/]{0,1}[0-9]{2,4}[-/_]{0,1}[0-9]{0,2}[-_/]{0,1}[0-9]{0,2}[-_/]{0,1}[0-9]{0,2}"

  /** Patterns for control files */
  val controlFilePatterns = List("./S_*", "_CTR.*", "_CTR", "\\.CTR", "\\.CTL", "\\.ctl")

  /** Find files from a directory matching a given filename prefix with a timestamp suffix. */
  def findFiles(sourceDir: File, fileName: String, timeFormat: String): Seq[InputFile] = {
    // TODO is flat file listing correct? we don't do recursive search?
    val fileNameRegex = s"^$fileName[_:-]?$regexTimeStampString.*".r
    sourceDir
      .listFiles.toList
      .filter(f => fileNameRegex.findFirstIn(f.getName).isDefined)
      .map(getInputFile(_, timeFormat))
  }

  /** gets the input file corresponding to a file */
  def getInputFile(file: File, timeFormat: String): InputFile =
    getTimeStamps(file) match {
      case Nil              => UnexpectedFile(file, s"could not find timestamp in ${file.getName}")
      case _ :: _ :: _      => UnexpectedFile(file, s"found more than one timestamp in ${file.getName}")
      case timeStamp :: Nil =>

        parseTimeStamp(timeFormat, timeStamp) match {
          case -\/(exn) => UnexpectedFile(file, s"The file timestamp is not valid in ${file.getName} (${exn.toString})")
          case \/-(date)   =>

            if (isControlFile(file))
              ControlFile(file)
            else
              DataFile(file, pathToFile(date, timeFormat))
        }
    }

  /** get all timestamps from file name */
  def getTimeStamps(file: File) =
    regexTimeStampString.r.findAllIn(file.getName).toList

  /** extract time from time stamps */
  def parseTimeStamp(timeFormat: String, timeStamp: String) =
    TimeParser.parse(timeStamp, timeFormat)

  /** check if file matches any of the control file patterns */
  def isControlFile(file: File) =
    controlFilePatterns.exists(_.r.findFirstIn(file.getName).isDefined)

  /** get the relative path to this file in hdfs and the archive dir */
  def pathToFile(dateTime: DateTime, timeFormat: String): File = {
    val mandatoryDirs = List(
      f"${dateTime.getYear}%04d",
      dateTime.getMonthOfYear.toString)
    val optionalDirs = List(
      dateTime.getDayOfMonth.toString,
      dateTime.getHourOfDay.toString,
      dateTime.getMinuteOfHour.toString)
    val numOptional = List("dd", "HH", "mm").takeWhile(timeFormat.contains(_)).length

    val actualDirs = mandatoryDirs ++ optionalDirs.take(numOptional)

    new File(actualDirs.mkString(File.pathSeparator))
  }
}
