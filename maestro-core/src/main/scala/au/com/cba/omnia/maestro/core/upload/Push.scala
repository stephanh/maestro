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

import org.apache.hadoop.fs.Path

import com.cba.omnia.edge.hdfs.{Hdfs, Result}

/** File copied to HDFS */
case class Copied(source: File, dest: Path)

/**
 * Functions to push files to HDFS
 * Hdfs operations are done using the `edge` project.
 *
 * Time Format can be any of the following:
 * yyyyMMdd, yyyyddMM, yyMMdd, yyMMddHH, yyyy-MM-dd-HH, yyMM etc
 */
object Push {

  /**
    * Copy the file to HDFS and archive it.
    *
    * If nothing goes wrong, this methods will:
    *   - copy the file to Hdfs
    *   - create an "ingestion complete" flag on Hdfs
    *   - archive the file
    *
    * This method will fail if a duplicate of the file or it's
    * ingestion complete or processed flags already exist in HDFS.
    */
  def push(src: Data, archiveDir: String, hdfsLandingDir: String): Hdfs[Copied] = {
    val hdfsDestDir      = Hdfs.path(List(hdfsLandingDir, src.fileSubDir) mkString File.separator)
    val hdfsDestFile     = new Path(hdfsDestDir, src.file.getName)
    val hdfsFlagFile     = new Path(hdfsDestDir, "_INGESTION_COMPLETE")
    val hdfsProcFlagFile = new Path(hdfsDestDir, "_PROCESSED")
    val archiveDestDir   = new File(List(archiveDir, src.fileSubDir) mkString File.separator)

    for {
      // fail if any traces of the file already exist on HDFS
      dup     <- Hdfs.exists(hdfsDestFile)
      _       <- EdgeOp.failHdfsIf(dup)(s"${src.file.getName} already exists at $hdfsDestFile")

      dupFlag <- Hdfs.exists(hdfsFlagFile)
      _       <- EdgeOp.failHdfsIf(dupFlag)(s"${src.file.getName} already has an ingestion complete flag at $hdfsFlagFile")

      dupProc <- Hdfs.exists(hdfsProcFlagFile)
      _       <- EdgeOp.failHdfsIf(dupProc)(s"${src.file.getName} already has a processed flag at $hdfsProcFlagFile")

      // copy file over
      dirOk   <- Hdfs.mkdirs(hdfsDestDir) // returns true if directory already exists
      _       <- EdgeOp.failHdfsIf(!dirOk)("$hdfsDestDir could not be created")
      _       <- Hdfs.copyFromLocalFile(src.file, hdfsDestDir)

      // create ingestion complete flag
      _       <- Hdfs.create(hdfsFlagFile)

      // archive file
      _      <- Hdfs.result(archiveFile(src.file, archiveDestDir))

    } yield Copied(src.file, hdfsDestFile)
  }

  /**
    * Archive the file after moving to HDFS.
    *
    * Overwrites any existing file in the archive directory.
    */
  def archiveFile(srcFile: File, destDir: File): Result[Unit] = {
      val destFile   = new File(destDir, srcFile.getName + ".gz")
      val fileExists = destFile.isFile
      val dirExists  = destDir.isDirectory

      for {
        _ <- EdgeOp.failIf(!dirExists && !destDir.mkdirs)("could not create archive directory for ${srcFile.getName}")
        _ <- Result.safe(FileOp.overwriteCompressed(srcFile, destFile))
      } yield ()
    }
}
