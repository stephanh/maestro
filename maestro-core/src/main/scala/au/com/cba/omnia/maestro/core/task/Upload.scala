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

import scalaz._, Scalaz._, scalaz.\&/.{This, That, Both}

import com.cba.omnia.edge.hdfs.{Error, Hdfs, Ok, Result}

import au.com.cba.omnia.maestro.core.upload._

/**
  * Push source files to HDFS using [[upload]] and archive them.
  *
  * See the example at `au.com.cba.omnia.maestro.example.CustomerUploadExample`.
  *
  * In order to run map-reduce jobs, we first need to get our data onto HDFS.
  * [[upload]] copies data files from the local machine onto HDFS and archives
  * the files.
  *
  * For a given `domain` and `tableName`, [[upload]] copies data files from
  * the standard location: `\$bigDataRoot/dataFeed/\$domain`, to the standard
  * HDFS location: `\$env/source/\$domain/\$tableName/<year>/<month>/<optional>/<datetime>/<directories>`.
  *
  * Only use [[customUpload]] if we are required to use non-standard locations.
  */
trait Upload {

  /**
    * Pushes source files onto HDFS and archives them locally.
    *
    * `upload` expects data files intended for HDFS to be placed in
    * the local folder `\$bigDataRoot/dataFeed/\$domain`. Different source systems
    * will use different values for `\$domain`. Data files will look like
    * `\$tableName<separator>\$timeFormat.<extension>`. `\$tableName` and
    * `\$timeFormat` will vary for each source system.
    *
    * Each data file will be copied onto HDFS as the following file:
    * `\$env/source/\$domain/\$tableName/<year>/<month>/<optional>/<datetime>/<directories>/<originalFileName>`.
    *
    * Data files are also gzipped and archived on the local machine. Each data
    * file is archived as:
    * `\$archiveRoot/dataFeed/\$domain/\$tableName/<year>/<month>/<optional>/<datetime>/<directories>/<originalFileName>.gz`.
    *
    * Some files placed on the local machine are control files. These files
    * are not intended for HDFS and are ignored by `upload`. `upload` will log
    * a message whenever it ignores a control file.
    *
    * When an error occurs, `upload` stops copying files immediately. Once the
    * cause of the error has been addressed, `upload` can be run again to copy
    * any remaining files to HDFS. `upload` will refuse to copy a file if that
    * file or it's control flags are already present in HDFS. If you
    * need to overwrite a file that already exists in HDFS, you will need to
    * delete the file and it's control flags before `upload` will replace it.
    *
    * @param domain: Domain (source system name)
    * @param tableName: Table name (file prefix)
    * @param timeFormat: Timestamp format
    * @param bigDataRoot: Root directory of incoming data files
    * @param archiveRoot: Root directory of the local archive
    * @param env: HDFS environment
    * @return Any error occuring when uploading files
    */
  def upload(domain: String, tableName: String, timeFormat: String,
    bigDataRoot: String, archiveRoot: String, env: String): Result[Unit] = {
    val logger = Logger.getLogger("Upload")

    logger.info("Start of upload")
    logger.info(s"domain      = $domain") // domain is only used for logging
    logger.info(s"tableName   = $tableName")
    logger.info(s"timeFormat  = $timeFormat")
    logger.info(s"bigDataRoot = $bigDataRoot")
    logger.info(s"archiveRoot = $archiveRoot")
    logger.info(s"env         = $env")

    val locSourceDir   = List(bigDataRoot, "dataFeed", domain)            mkString File.separator
    val archiveDir     = List(archiveRoot, "dataFeed", domain, tableName) mkString File.separator
    val hdfsLandingDir = List(env,         "source",   domain, tableName) mkString File.separator

    val conf = new Configuration
    val result: Result[Unit] = Upload.uploadImpl(tableName, timeFormat, locSourceDir, archiveDir, hdfsLandingDir).safe.run(conf)

    val args = s"$domain $tableName"
    result match {
      case Ok(())                => logger.info(s"Upload ended for $args")
      case Error(This(msg))      => logger.error(s"Upload failed for $args: $msg")
      case Error(That(exn))      => logger.error(s"Upload failed for $args", exn)
      case Error(Both(msg, exn)) => logger.error(s"Upload failed for $args: $msg", exn)
    }

    result
  }

  /**
    * Pushes source files onto HDFS and archives them locally, using non-standard file locations.
    *
    * As per [[upload]], except the user has more control where to find data
    * files, where to copy them, and where to archive them.
    *
    * Data files are found in the local folder `\$locSourceDir`. They are copied
    * to `\$hdfsLandingDir/<year>/<month>/<optional>/<datetime>/<directories>/<originalFileName>`,
    * and archived at `\$archiveDir/<year>/<month>/<optional>/<datetime>/<directories>/<originalFileName>.gz`.
    * In all other respects `customUpload` behaves the same as [[upload]].
    *
    * @param domain: Domain (source system name)
    * @param tableName: Table name (file prefix)
    * @param timeFormat: Timestamp format
    * @param locSourceDir: Local source landing directory
    * @param archiveDir: Local archive directory
    * @param hdfsLandingDir: HDFS landing directory
    * @return Any error occuring when uploading files
    */
  def customUpload(domain: String, tableName: String, timeFormat: String,
    locSourceDir: String, archiveDir: String, hdfsLandingDir: String): Result[Unit] = {
    val logger = Logger.getLogger("Upload")

    logger.info("Start of custom upload")
    logger.info(s"domain         = $domain") // domain is only used for logging
    logger.info(s"tableName      = $tableName")
    logger.info(s"timeFormat     = $timeFormat")
    logger.info(s"locSourceDir   = $locSourceDir")
    logger.info(s"archiveDir     = $archiveDir")
    logger.info(s"hdfsLandingDir = $hdfsLandingDir")

    val conf = new Configuration
    val result = Upload.uploadImpl(tableName, timeFormat, locSourceDir, archiveDir, hdfsLandingDir).safe.run(conf)

    val args = s"$domain $tableName"
    result match {
      case Ok(())                => logger.info(s"Custom upload ended for $args")
      case Error(This(msg))      => logger.error(s"Custom upload failed for $args: $msg")
      case Error(That(exn))      => logger.error(s"Custom upload failed for $args", exn)
      case Error(Both(msg, exn)) => logger.error(s"Custom upload failed for $args: $msg", exn)
    }

    result
  }
}

/**
  * Contains implementation for `upload` methods in [[Upload]] trait.
  *
  * WARNING: The methods on this object are not considered part of the public
  * maestro API, and may change without warning. Use the methods in the maestro
  * API instead, unless you know what you are doing.
  */
object Upload {
  val logger = Logger.getLogger("Upload")

  /** Implementation of `upload methods in [[Upload]] trait */
  def uploadImpl(tableName: String, timeFormat: String,
    locSourceDir: String, archiveDir: String, hdfsLandingDir: String): Hdfs[Unit] =
    for {
      inputFiles <- Hdfs.result(Input.findFiles(new File(locSourceDir), tableName, timeFormat))

      _ <- inputFiles traverse_ {
        case Control(file)   => Hdfs.value(logger.info(s"skipping control file ${file.getName}"))
        case src @ Data(_,_) => for {
          copied <- Push.push(src, archiveDir, hdfsLandingDir)
          _      =  logger.info(s"copied ${copied.source.getName} to ${copied.dest}")
        } yield ()
      }
    } yield ()
}
