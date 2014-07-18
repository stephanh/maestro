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

import scalaz._, Scalaz._

import com.cba.omnia.edge.hdfs.{Error, Hdfs, Ok, Result}

import au.com.cba.omnia.maestro.core.tributary._

/** Send files to HDFS */
trait Tributary {

  def flow(domain: String, tableName: String, timeFormat: String,
    bigDataRoot: String, archiveRoot: String, env: String): Result[Unit] = {
    val logger = Logger.getLogger("Tributary")

    logger.info("Start of Tributary flow")
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
    val result: Result[Unit] = Tributary.flowImpl(tableName, timeFormat, locSourceDir, archiveDir, hdfsLandingDir).safe.run(conf)

    // TODO log depends on what result is!
    logger.info(s"Tributary flow ended for $domain $tableName $timeFormat $bigDataRoot $archiveRoot $env")

    result
  }

  /**
    * Pushes source files onto HDFS and archives them locally.
    *
    * TODO more documentation!!
    *
    * @param domain: Domain (source system name)
    * @param tableName: Table name (file prefix)
    * @param timeFormat: Timestamp format
    * @param locSourceDir: Local source landing directory
    * @param archiveDir: Local archive directory
    * @param hdfsLandingDir: HDFS landing directory
    */
  def customFlow(domain: String, tableName: String, timeFormat: String,
    locSourceDir: String, archiveDir: String, hdfsLandingDir: String): Result[Unit] = {
    val logger = Logger.getLogger("Tributary")

    logger.info("Start of Tributary custom flow")
    logger.info(s"domain         = $domain") // domain is only used for logging
    logger.info(s"tableName      = $tableName")
    logger.info(s"timeFormat     = $timeFormat")
    logger.info(s"locSourceDir   = $locSourceDir")
    logger.info(s"archiveDir     = $archiveDir")
    logger.info(s"hdfsLandingDir = $hdfsLandingDir")

    val conf = new Configuration
    val result = Tributary.flowImpl(tableName, timeFormat, locSourceDir, archiveDir, hdfsLandingDir).safe.run(conf)

    // TODO log depends on what result is!
    logger.info(s"Tributary custom flow ended for $domain $tableName $timeFormat $locSourceDir $archiveDir $hdfsLandingDir")

    result
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

  /** Implementation of `flow` methods in `Tributary` trait */
  def flowImpl(tableName: String, timeFormat: String,
    locSourceDir: String, archiveDir: String, hdfsLandingDir: String): Hdfs[Unit] =
    for {
      inputFiles <- Hdfs.result(Input.findFiles(new File(locSourceDir), tableName, timeFormat))

      _ <- inputFiles.traverse_(_ match {
        case Control(file)   => Hdfs.value(logger.info(s"skipping control file ${file.getName}"))
        case src @ Data(_,_) => for {
          copied <- Push.push(src, archiveDir, hdfsLandingDir)
          _      =  logger.info(s"copied ${copied.source.getName} to ${copied.dest}")
        } yield ()
      })
    } yield ()
}
