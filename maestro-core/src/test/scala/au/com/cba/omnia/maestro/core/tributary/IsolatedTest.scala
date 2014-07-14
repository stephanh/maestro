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

import scala.util.Random

import java.io.File
import java.io.FileNotFoundException

import org.specs2.mutable.After
import org.specs2.specification.Scope

/** Trait that provides clean test directories for Tributary tests */
trait IsolatedTest extends Scope with After {

  // machinery to find a suitable isolated root dir for temporary test folders
  val random = new Random()
  val tmpDir = System.getProperty("java.io.tmpdir")

  def findUniqueRoot(): File = {
    val possibleDir = new File(tmpDir, "isolated-test-" + random.nextInt(Int.MaxValue)).getAbsoluteFile
    val unique = possibleDir.mkdir
    if (unique) possibleDir else findUniqueRoot() // don't expect to recurse often
  }

  // isolated folders for tests to refer to
  val rootDir = findUniqueRoot()

  val testDir = new File(rootDir, "testInputDirectory")
  val testDirS = testDir.toString

  val archiveDir = new File(rootDir, "testArchiveDirectory")
  val archiveDirS = archiveDir.toString

  // TODO hdfsDir in isolated temp dir: should be in Hdfs instead
  val hdfsDir = new File(rootDir, "testHdfsDirectory")
  val hdfsDirS = hdfsDir.toString

  List(testDir, archiveDir, hdfsDir) foreach (dir => {
    val created = dir.mkdir
    if (!created) throw new FileNotFoundException(dir.toString)
  })

  // after the test has completed, remove the temporary dir
  // TODO use a library which implements the equivalent of deleteAll
  // TODO avoid following symlinks (hopefully the above todo gives us this for free)
  def after {
    def deleteAll(file: File) {
      if (file.isDirectory)
        file.listFiles.foreach(deleteAll)
      if (file.exists)
        file.delete
    }
    deleteAll(rootDir)
  }
}
