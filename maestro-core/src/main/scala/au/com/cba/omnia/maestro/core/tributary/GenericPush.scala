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

import org.apache.hadoop.fs.{ Path, FileSystem }

import com.cba.omnia.edge.hdfs.{Hdfs, Result}

/** Represents the action required to push to HDFS */
sealed trait PushAction

/** The file was copied to HDFS */
case class Copied() extends PushAction

/** The file already existed in HDFS */
case class AlreadyExists() extends PushAction

/** Represents a successfull push to HDFS */
case class Pushed(soure: DataFile, dest: Path, action: PushAction)

/**
 * Description : Generic Push to HDFS implemented.
 * Hdfs operations are done using the `edge` project.
 *
 * Time Format can be any of the following:
 * yyyyMMdd, yyyyddMM, yyMMdd, yyMMddHH, yyyy-MM-dd-HH, yyMM etc
 */
object GenericPush {

  /**
    * Process the incoming file:
    *   - copy the file to Hdfs
    *   - create an "ingestion complete" flag on Hdfs
    *   - archive the file
    */
  def processTheFile(src: DataFile, hdfsLandingDir: String, archiveDir: String,
    destSubDir: String): Hdfs[Pushed] = {
    val hdfsDestDir = Hdfs.path(hdfsLandingDir + File.separator + destSubDir + File.separator + src.fileSubDir)
    val archiveDestDir = new File(archiveDir + File.separator + destSubDir + File.separator + src.fileSubDir)

    for {
      pushed <- copyToHdfs(src, hdfsDestDir)
      _      <- createFlag(hdfsDestDir, "_INGESTION_COMPLETE")
      _      <- Hdfs.result(archiveFile(src.file, archiveDestDir))
    } yield pushed
  }

  /**
    * Copy the file to HDFS.
    *
    * Succeeds if the file already exists in HDFS.
    */
  def copyToHdfs(src: DataFile, destDir: Path): Hdfs[Pushed] = {
    val destFile = new Path(destDir, src.file.getName)

    val action = for {
      dirOk   <- Hdfs.mkdirs(destDir) // returns true if directory already exists
      _       <- EdgeOp.failHdfsIf(!dirOk)("directory does not exist")

      pushAct <- EdgeOp.or[PushAction](
        // check if file already exists
        for {
          exists <- Hdfs.exists(destFile)
          _      <- EdgeOp.failHdfsIf(!exists)("The file does not already exist")
        } yield AlreadyExists(),

        // if file does not exist, try to copy file over
        for {
          _      <- Hdfs.copyFromLocalFile(src.file, destDir)
          exists <- Hdfs.exists(destFile)
          _      <- EdgeOp.failHdfsIf(!exists)("Hdfs ops succeeded, but the file was not created")
        } yield Copied())

    } yield Pushed(src, destFile, pushAct)

    Failure.wrapHdfs[Pushed](s"Copy to hdfs has failed for ${src.file}")(action)
  }

  /**
    * Create Ingestion complete flag in HDFS.
    *
    * Succeeds if the ingestion flag already exists in HDFS.
    */
  def createFlag(destDir: Path, flagName: String): Hdfs[Unit] = {
    val flagPath = new Path(destDir, flagName)

    val action = EdgeOp.or[Unit](
      // check if flag already exists
      for {
        exists <- Hdfs.exists(flagPath)
        _      <- EdgeOp.failHdfsIf(!exists)("")
      } yield (),

      // if flag does not exist, try to create it
      for {
        _      <- Hdfs.create(flagPath)
        exists <- Hdfs.exists(flagPath)
        _      <- EdgeOp.failHdfsIf(!exists)("")
      } yield ())

    Failure.wrapHdfs[Unit](s"Cannot create ingestion flag in $destDir")(action)
  }

  /**
    * Archive the file after moving to HDFS.
    *
    * Succeeds if the file already exists in archive dir.
    */
  def archiveFile(srcFile: File, destDir: File): Result[Unit] =
    Failure.wrap(s"The archive has failed for $srcFile") {
      val destFile   = new File(destDir, srcFile.getName)
      val fileExists = destFile.isFile
      val dirExists  = destDir.isDirectory

      for {
        _ <- EdgeOp.failIf(!dirExists && !destDir.mkdirs)("could not create destination directory")
        _ <- EdgeOp.failIf(!fileExists && !srcFile.renameTo(destFile))("failed to move file to destination")
      } yield ()
    }
}
