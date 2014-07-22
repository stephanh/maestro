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

import scala.util.control.NonFatal

import java.io.{File, FileOutputStream}

import java.util.zip.GZIPOutputStream

import com.google.common.io.Files

import scalaz.\/

/**
  * Operations on files
  *
  * TODO move these operations to the appropriate place in omnitool
  */
object FileOp {
  /**
    * Moves file from source to destination.
    *
    * If another file already exists at destination, this method overwrites it.
    *
    * This method is not atomic. Other processes operating on dest or source at
    * the same time will cause problems.
    */
  def overwrite(source: File, dest: File): Unit =
    // if there is nothing to overwrite, we simply move the file
    if (!dest.exists)
      Files.move(source, dest)

    // if we have to overwrite destination file, we try not to be left in a state
    // where the destination file is deleted but we failed to move new file over
    else {
      val tempDir = Files.createTempDir
      val temp = new File(tempDir, "target")

      try {
        Files.move(dest, temp)
        Files.move(source, dest)
      }
      catch {
        case NonFatal(e) => {
          // assuming either dest is old file and temp does not exist,
          // or dest does not exist and temp is old file
          if (temp.exists) Files.move(temp, dest)
          throw e
        }
      }
      finally {
        if (temp.exists) temp.delete
        if (tempDir.exists) tempDir.delete
      }
    }

  /**
    * Moves file from source to destination. Destination is gzipped.
    *
    * If another file already exists at destination, this method overwrites it.
    *
    * This method is not atomic. Other processes operating on dest or source at
    * the same time will cause problems.
    */
  def overwriteCompressed(source: File, dest: File): Unit = {
    val temp = File.createTempFile("moved", "gz")

    // there must be a more convienient canonical way of doing this in scala ...
    try {
      val writer = new GZIPOutputStream(new FileOutputStream(temp))
      try {
        Files.copy(source, writer)
        writer.flush
      }
      finally {
        writer.close
      }
      overwrite(temp, dest)
    }
    finally {
      temp.delete // can't rely on deleteOnExit
    }
  }
}
