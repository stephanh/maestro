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
package task

import java.io.File

import org.apache.hadoop.conf.Configuration

import org.apache.log4j.Logger

import scalaz.\&/.{This, That, Both}

import com.cba.omnia.edge.hdfs.{Error, Ok}

import au.com.cba.omnia.maestro.core.tributary._

/** Send files to HDFS */
trait Tributary {

  /**
    *  Pushes source files onto HDFS and archives them locally.
    *
    *  @param srcName: Source System Name
    *  @param fileName: File Name
    *  @param timeFormat: Timestamp format
    *  @param locSourceDir: Local source landing directory
    *  @param archiveDir: Local archive directory
    *  @param hdfsLandingDir: HDFS landing directory, must be subdirectory of HDFS source directory
    */
  def flow(srcName: String, fileName: String, timeFormat: String,
    locSourceDir: String, archiveDir: String, hdfsLandingDir: String) {

    Tributary.splitHdfsDir(new File(hdfsLandingDir)) match {
      case Some((hdfsSourceDir, destSubDir)) =>
        Tributary.flowImpl(srcName, fileName, timeFormat, locSourceDir,
          archiveDir, hdfsSourceDir.toString, destSubDir)

      case None =>
        Tributary.logger.error(s"Could not start Tributary flow: could not find HDFS source dir in $hdfsLandingDir")
    }

  }

  /**
    *  Pushes source files onto HDFS and archives them locally.
    *
    *  @param srcName: Source System Name
    *  @param fileName: File Name
    *  @param timeFormat: Timestamp format
    *  @param locSourceDir: Local source landing directory
    *  @param archiveDir: Local archive directory
    *  @param hdfsSourceDir: HDFS source directory
    *  @param destSubDir: subdirectory of HDFS source and archive file
    *
    */
  def flow(srcName: String, fileName: String, timeFormat: String,
    locSourceDir: String, archiveDir: String, hdfsSourceDir: String, destSubDir: String) {

    Tributary.flowImpl(srcName, fileName, timeFormat, locSourceDir, archiveDir,
      hdfsSourceDir, destSubDir)
  }
}

/**
  * Contains implementation for `flow` methods in `Tributary` trait.
  *
  * WARNING: The methods on this object are not considered part of the public
  * maestro API, and may change without warning. Use the methods in the maestro
  * API instead, unless you know what you are doing.
  */
object Tributary {

  val logger = Logger.getLogger("Tributary")

  /** Split hdfsLandingDir into hdfs source dir and subdirectory within source dir */
  def splitHdfsDir(hdfsDir: File): Option[(File, String)] =
    if (hdfsDir.getName == "source")
      Some((hdfsDir, ""))
    else
      Option(hdfsDir.getParentFile)
        .flatMap(splitHdfsDir(_))
        .map(pair => (pair._1, pair._2 + File.separator + hdfsDir.getName))

  /** Implementation of `flow` methods in `Tributary` trait */
  def flowImpl(srcName: String, fileName: String, timeFormat: String,
    locSourceDir: String, archiveDir: String, hdfsSourceDir: String, destSubDir: String) {

    logger.info("Start of Tributary Flow")
    logger.info(s"srcName       = $srcName")     // srcName is only used for logging.
    logger.info(s"fileName      = $fileName")
    logger.info(s"timeFormat    = $timeFormat")
    logger.info(s"locSourceDir  = $locSourceDir")
    logger.info(s"archiveDir    = $archiveDir")
    logger.info(s"hdfsSourceDir = $hdfsSourceDir")
    logger.info(s"destSubDir    = $destSubDir")

    val inputFiles = InputFile.findFiles(new File(locSourceDir), fileName, timeFormat)
    val conf = new Configuration

    if (inputFiles.isEmpty) {
      logger.info("No input files have been found")
    } else {
      inputFiles.foreach(file => {
        file match {
          case ControlFile(file)         => logger.info(s"Skipping control file ${file.getName}")
          case UnexpectedFile(file, msg) => logger.error(s"error processing ${file.getName}: $msg")

          case src @ DataFile(_,_)       => {
            val result = GenericPush.processTheFile(src, hdfsSourceDir, archiveDir, destSubDir).safe.run(conf)
            result match {

              case Error(That(exn))      => logger.error(s"error processing $file", exn)
              case Error(This(msg))      => logger.error(msg)
              case Error(Both(msg, exn)) => logger.error(msg, exn)

              case Ok(Pushed(src, dest, Copied))        => logger.info(s"copied ${src.file} to $dest")
              case Ok(Pushed(src, dest, AlreadyExists)) => logger.info(s"skipping ${src.file} as it already exists at $dest")
            }
          }
        }
      })
    }

    logger.info(s"Tributary flow ended for $srcName $fileName $locSourceDir $archiveDir $hdfsSourceDir $destSubDir")
  }
}
